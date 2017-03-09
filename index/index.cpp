#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <memory>
#include "index/index.h"
#include "index/inner_node.h"

const uint8_t db::index::index::kMagic[8] = {
  'I',
  'N',
  'D',
  'E',
  'X',
  'I',
  'D',
  'X'
};

bool db::index::index::open(const char* filename)
{
  // If the file exists...
  struct stat sbuf;
  if (stat(filename, &sbuf) == 0) {
    // Open file for reading/writing.
    if ((fd_ = ::open(filename, O_RDWR)) != -1) {
      // Map file into memory.
      if ((data_ = mmap(NULL,
                        sbuf.st_size,
                        PROT_READ | PROT_WRITE,
                        MAP_SHARED,
                        fd_,
                        0)) != MAP_FAILED) {
        filesize_ = sbuf.st_size;

        header_ = reinterpret_cast<header*>(data_);

        // Check that header_->nnodes is not too big.
        return (((header_->nnodes + 1) * kNodeSize) <= filesize_);
      }
    }
  } else {
    // Create file.
    if ((fd_ = ::open(filename, O_CREAT | O_RDWR, 0644)) != -1) {
      // Grow file.
      if (ftruncate(fd_, kAllocate) == 0) {
        // Map file into memory.
        if ((data_ = mmap(NULL,
                          kAllocate,
                          PROT_READ | PROT_WRITE,
                          MAP_SHARED,
                          fd_,
                          0)) != MAP_FAILED) {
          filesize_ = kAllocate;

          header_ = reinterpret_cast<header*>(data_);

          // Fill header.
          memcpy(header_->magic, kMagic, sizeof(kMagic));

          header_->nnodes = 0;
          header_->nkeys = 0;

          header_->root = 0;

          return true;
        }
      }
    }
  }

  return false;
}

void db::index::index::close()
{
  if (data_ != MAP_FAILED) {
    munmap(data_, filesize_);
    data_ = MAP_FAILED;
  }

  if (fd_) {
    ::close(fd_);
    fd_ = -1;
  }
}

bool db::index::index::add(const void* key,
                           keylen_t keylen,
                           uint64_t dataoff,
                           comparator_t comp)
{
  struct level {
    uint64_t off;
    nodeoff_t pos;
  };

  // If the key is neither too short nor too long...
  if ((keylen >= kKeyMinLen) && (keylen <= kKeyMaxLen)) {
    // If there is root...
    if (header_->root != 0) {
      struct level levels[kMaxDepth];

      uint64_t off = header_->root;
      nodeoff_t pos;

      size_t depth = 0;

      struct node* n;

      do {
        // Read node.
        if ((n = read_node(off)) != NULL) {
          // Inner node?
          if (n->t == node::type::kInnerNode) {
            levels[depth].off = off;

            // Search key in the node.
            if (!static_cast<const struct inner_node*>(n)->search(key,
                                                                  keylen,
                                                                  comp,
                                                                  pos)) {
              off = (pos != 0) ?
                static_cast<const struct inner_node*>(n)->
                  entries[pos - 1].child :
                static_cast<const struct inner_node*>(n)->left;
            } else {
              off = static_cast<const struct inner_node*>(n)->
                      entries[pos].child;
            }

            levels[depth].pos = pos;

            if (++depth == kMaxDepth) {
              return false;
            }
          } else {
            // Leaf node.

            // Search key in the node.
            if (!static_cast<const struct leaf_node*>(n)->search(key,
                                                                 keylen,
                                                                 comp,
                                                                 pos)) {
              break;
            } else {
              // Key is already in the node.

              // If the key had been deleted...
              if (static_cast<const struct leaf_node*>(n)->erased(pos)) {
                static_cast<struct leaf_node*>(n)->entries[pos].deleted = 0;

                header_->nkeys++;
              }

              // Update data offset.
              static_cast<struct leaf_node*>(n)->entries[pos].dataoff = dataoff;

              return true;
            }
          }
        } else {
          return false;
        }
      } while (true);

      // Insert key in the node (if it fits).
      if (static_cast<struct leaf_node*>(n)->add(key, keylen, dataoff, pos)) {
        header_->nkeys++;

        return true;
      } else {
        // Node is full.

        // Create right node.
        uint64_t rightoff;
        if (create_node(depth, rightoff) != 0) {
          // Memory mapping might have been relocated.
          n = read_node(off);

          // If the node had already a right node...
          uint64_t oldrightoff;
          if ((oldrightoff = static_cast<const struct leaf_node*>(n)->next)
              != 0) {
            node* oldright;
            if ((oldright = read_node(oldrightoff)) != NULL) {
              static_cast<struct leaf_node*>(oldright)->prev = rightoff;
            } else {
              return false;
            }
          }

          void* mem = reinterpret_cast<void*>(
                        reinterpret_cast<uint8_t*>(data_) + rightoff
                      );

          struct leaf_node* right_leaf = new (mem) leaf_node();

          // Split leaf node.
          static_cast<struct leaf_node*>(n)->split(off,
                                                   rightoff,
                                                   right_leaf,
                                                   pos,
                                                   key,
                                                   keylen,
                                                   dataoff);

          header_->nkeys++;

          // The first key of the right node goes up to the parent.
          key = right_leaf->key(0);
          keylen = right_leaf->keylen(0);

          uint8_t upkey[kKeyMaxLen];

          struct node* r = right_leaf;

          while (depth > 0) {
            depth--;

            uint64_t child = rightoff;

            n = read_node(levels[depth].off);
            pos = levels[depth].pos;

            if (static_cast<struct inner_node*>(n)->add(key,
                                                        keylen,
                                                        child,
                                                        pos)) {
              return true;
            } else {
              // Create right node.
              if (create_node(depth, rightoff) != 0) {
                void* mem = reinterpret_cast<void*>(
                              reinterpret_cast<uint8_t*>(data_) + rightoff
                            );

                struct inner_node* right_inner = new (mem) inner_node();
                r = right_inner;

                // Split inner node.
                static_cast<struct inner_node*>(n)->split(right_inner,
                                                          pos,
                                                          key,
                                                          keylen,
                                                          child,
                                                          upkey,
                                                          keylen);

                key = upkey;
              } else {
                return false;
              }
            }
          }

          // Create new root node.
          if (create_node(0, off) != 0) {
            void* mem = reinterpret_cast<void*>(
                          reinterpret_cast<uint8_t*>(data_) + off
                        );

            struct inner_node* root = new (mem) inner_node();

            root->t = node::type::kInnerNode;
            root->parent = 0;

            root->add(key, keylen, rightoff, static_cast<nodeoff_t>(0));

            root->left = header_->root;

            n->parent = off;
            r->parent = off;

            header_->root = off;

            return true;
          }
        }
      }
    } else {
      // Create root node.
      uint64_t off;
      if (create_node(0, off)) {
        header_->nnodes = 1;
        header_->nkeys = 1;
        header_->root = off;

        void* mem = reinterpret_cast<void*>(
                      reinterpret_cast<uint8_t*>(data_) + header_->root
                    );

        struct leaf_node* root = new (mem) leaf_node();

        root->t = node::type::kLeafNode;
        root->parent = 0;

        root->prev = 0;
        root->next = 0;

        root->add(key, keylen, dataoff, static_cast<nodeoff_t>(0));

        return true;
      }
    }
  }

  return false;
}

bool db::index::index::erase(const void* key,
                             keylen_t keylen,
                             comparator_t comp)
{
  // If the key is neither too short nor too long...
  if ((keylen >= kKeyMinLen) && (keylen <= kKeyMaxLen)) {
    // If there is root...
    if (header_->root != 0) {
      uint64_t off = header_->root;

      do {
        // Read node.
        struct node* n;
        if ((n = read_node(off)) != NULL) {
          // Inner node?
          if (n->t == node::type::kInnerNode) {
            // Search key in the node.
            nodeoff_t pos;
            if (!static_cast<const struct inner_node*>(n)->search(key,
                                                                  keylen,
                                                                  comp,
                                                                  pos)) {
              off = (pos != 0) ?
                static_cast<const struct inner_node*>(n)->
                  entries[pos - 1].child :
                static_cast<const struct inner_node*>(n)->left;
            } else {
              off = static_cast<const struct inner_node*>(n)->
                      entries[pos].child;
            }
          } else {
            // Leaf node.

            // Erase key.
            if (static_cast<struct leaf_node*>(n)->erase(key, keylen, comp)) {
              header_->nkeys--;
            }

            return true;
          }
        } else {
          return false;
        }
      } while (true);
    } else {
      return true;
    }
  }

  return false;
}

bool db::index::index::begin(iterator& it) const
{
  // If there is root...
  if (header_->root != 0) {
    uint64_t off = header_->root;

    const struct node* n;

    do {
      // Read node.
      if ((n = read_node(off)) != NULL) {
        // Inner node?
        if (n->t == node::type::kInnerNode) {
          off = static_cast<const struct inner_node*>(n)->left;
        } else {
          // Leaf node.
          break;
        }
      } else {
        return false;
      }
    } while (true);

    do {
      const struct leaf_node* leaf =
                              reinterpret_cast<const struct leaf_node*>(n);

      for (nodeoff_t i = 0; i < leaf->nentries; i++) {
        if (!leaf->erased(i)) {
          it.off_ = off;
          it.node_ = leaf;
          it.pos_ = i;

          return true;
        }
      }

      off = leaf->next;

    } while ((n = read_node(off)) != NULL);
  }

  return false;
}

bool db::index::index::end(iterator& it) const
{
  // If there is root...
  if (header_->root != 0) {
    uint64_t off = header_->root;

    const struct node* n;

    do {
      // Read node.
      if ((n = read_node(off)) != NULL) {
        // Inner node?
        if (n->t == node::type::kInnerNode) {
          off = static_cast<const struct inner_node*>(n)->
                entries[n->nentries - 1].child;
        } else {
          // Leaf node.
          break;
        }
      } else {
        return false;
      }
    } while (true);

    do {
      const struct leaf_node* leaf =
                              reinterpret_cast<const struct leaf_node*>(n);

      for (nodeoff_t i = leaf->nentries; i > 0; i--) {
        if (!leaf->erased(i - 1)) {
          it.off_ = off;
          it.node_ = leaf;
          it.pos_ = i - 1;

          return true;
        }
      }

      off = leaf->prev;

    } while ((n = read_node(off)) != NULL);
  }

  return false;
}

bool db::index::index::previous(iterator& it) const
{
  const struct node* n = it.node_;
  uint64_t off = it.off_;
  nodeoff_t i = it.pos_;

  do {
    for (; i > 0; i--) {
      if (!reinterpret_cast<const struct leaf_node*>(n)->erased(i - 1)) {
        it.off_ = off;
        it.node_ = static_cast<const struct leaf_node*>(n);
        it.pos_ = i - 1;

        return true;
      }
    }

    off = static_cast<const struct leaf_node*>(n)->prev;

    if ((n = read_node(off)) != NULL) {
      i = n->nentries;
    } else {
      break;
    }
  } while (true);

  return false;
}

bool db::index::index::next(iterator& it) const
{
  const struct node* n = it.node_;
  uint64_t off = it.off_;
  nodeoff_t i = it.pos_ + 1;

  do {
    for (; i < n->nentries; i++) {
      if (!reinterpret_cast<const struct leaf_node*>(n)->erased(i)) {
        it.off_ = off;
        it.node_ = static_cast<const struct leaf_node*>(n);
        it.pos_ = i;

        return true;
      }
    }

    off = static_cast<const struct leaf_node*>(n)->next;
    i = 0;
  } while ((n = read_node(off)) != NULL);

  return false;
}

bool db::index::index::find(const void* key,
                            keylen_t keylen,
                            comparator_t comp,
                            iterator& it) const
{
  // If the key is neither too short nor too long...
  if ((keylen >= kKeyMinLen) && (keylen <= kKeyMaxLen)) {
    // If there is root...
    if (header_->root != 0) {
      uint64_t off = header_->root;

      do {
        // Read node.
        const struct node* n;
        if ((n = read_node(off)) != NULL) {
          // Inner node?
          if (n->t == node::type::kInnerNode) {
            // Search key in the node.
            nodeoff_t pos;
            if (!static_cast<const struct inner_node*>(n)->search(key,
                                                                  keylen,
                                                                  comp,
                                                                  pos)) {
              off = (pos != 0) ?
                static_cast<const struct inner_node*>(n)->
                  entries[pos - 1].child :
                static_cast<const struct inner_node*>(n)->left;
            } else {
              off = static_cast<const struct inner_node*>(n)->
                      entries[pos].child;
            }
          } else {
            // Leaf node.

            nodeoff_t pos;
            if ((static_cast<const struct leaf_node*>(n)->search(key,
                                                                 keylen,
                                                                 comp,
                                                                 pos)) &&
                (!static_cast<const struct leaf_node*>(n)->erased(pos))) {
              it.off_ = off;
              it.node_ = static_cast<const struct leaf_node*>(n);
              it.pos_ = pos;

              return true;
            } else {
              return false;
            }
          }
        } else {
          return false;
        }
      } while (true);
    }
  }

  return false;
}

bool db::index::index::print() const
{
  // If there is root...
  if (header_->root != 0) {
    uint64_t off = header_->root;

    const struct node* n;
    uint64_t nkeys = 0;
    size_t depth = 1;

    do {
      // Read node.
      if ((n = read_node(off)) != NULL) {
        // Inner node?
        if (n->t == node::type::kInnerNode) {
          off = static_cast<const struct inner_node*>(n)->left;

          depth++;
        } else {
          // Leaf node.
          break;
        }
      } else {
        printf("Error reading node at offset %lu.\n", off);
        return false;
      }
    } while (true);

    const struct leaf_node* leaf =
                            reinterpret_cast<const struct leaf_node*>(n);

    do {
      printf("Node:\n");
      leaf->print();
      printf("==========================================================\n");

      nkeys += leaf->nentries;

      // If there is next node...
      if (leaf->next != 0) {
        // Read node.
        if ((leaf = static_cast<const struct leaf_node*>(
                      read_node(leaf->next)
                    )) == NULL) {
          printf("Error reading node at offset %lu.\n", off);
          return false;
        }
      } else {
        break;
      }
    } while (true);

    if (nkeys != header_->nkeys) {
      printf("Unexpected number of keys %lu, expected %lu.\n",
             nkeys,
             header_->nkeys);
    } else {
      printf("# of keys: %lu.\n", nkeys);
    }

    printf("# of nodes: %lu.\n", header_->nnodes);
    printf("Depth: %zu.\n", depth);
  }

  return true;
}

bool db::index::index::create_node(size_t depth, uint64_t& off)
{
  // Allocate nodes (if needed).
  if (allocate(depth + 2)) {
    // Calculate the offset of the new node.
    off = (1 + header_->nnodes) * kNodeSize;

    header_->nnodes++;

    return true;
  }

  return false;
}

bool db::index::index::allocate(size_t count)
{
  // Calculate the needed size.
  uint64_t size = (1 + header_->nnodes + count) * kNodeSize;

  // If there is space enough...
  if (size <= filesize_) {
    return true;
  }

  // We have to allocate more nodes.

  size_t n = kAllocate;
  while (n < count) {
    n *= 2;
  }

  size = (1 + header_->nnodes + n) * kNodeSize;

  // Grow file.
  if (ftruncate(fd_, size) == 0) {
    // Remap file into memory.
    void* data;
    if ((data = mremap(data_,
                       filesize_,
                       size,
                       MREMAP_MAYMOVE)) != MAP_FAILED) {
      data_ = data;
      filesize_ = size;

      header_ = reinterpret_cast<header*>(data_);

      return true;
    }
  }

  return false;
}
