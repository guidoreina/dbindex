#ifndef DB_INDEX_INDEX_H
#define DB_INDEX_INDEX_H

#include <sys/mman.h>
#include "index/node.h"
#include "index/leaf_node.h"
#include "constants.h"

namespace db {
  namespace index {
    class index {
      public:
        // Constructor.
        index();

        // Destructor.
        ~index();

        // Open.
        bool open(const char* filename);

        // Close.
        void close();

        // Add key.
        bool add(const void* key,
                 keylen_t keylen,
                 uint64_t dataoff,
                 comparator_t comp);

        // Erase key (marks the key as deleted).
        bool erase(const void* key, keylen_t keylen, comparator_t comp);

        // Find key.
        bool find(const void* key,
                  keylen_t keylen,
                  comparator_t comp,
                  uint64_t& dataoff) const;

        // Get number of keys.
        uint64_t size() const;

        class iterator {
          friend class index;

          public:
            // Get key.
            const void* key() const;

            // Get key length.
            keylen_t keylen() const;

            // Get data offset.
            uint64_t data_offset() const;

          private:
            uint64_t off_;
            const struct leaf_node* node_;
            nodeoff_t pos_;
        };

        // Begin.
        bool begin(iterator& it) const;

        // End.
        bool end(iterator& it) const;

        // Previous.
        bool previous(iterator& it) const;

        // Next.
        bool next(iterator& it) const;

        // Find.
        bool find(const void* key,
                  keylen_t keylen,
                  comparator_t comp,
                  iterator& it) const;

        // Print.
        bool print() const;

      private:
        static const size_t kAllocate = 1024; // Number of nodes to allocate.
        static const uint8_t kMagic[8];

        struct header {
          uint8_t magic[8];

          uint64_t nnodes;
          uint64_t nkeys;

          uint64_t root;
        };

        int fd_;
        void* data_;

        uint64_t filesize_;

        header* header_;

        // Get node.
        node* read_node(uint64_t off);
        const node* read_node(uint64_t off) const;

        // Create node.
        bool create_node(size_t depth, uint64_t& off);

        // Allocate nodes.
        bool allocate(size_t count);
    };

    inline index::index()
      : fd_(-1),
        data_(MAP_FAILED)
    {
    }

    inline index::~index()
    {
      close();
    }

    inline bool index::find(const void* key,
                            keylen_t keylen,
                            comparator_t comp,
                            uint64_t& dataoff) const
    {
      iterator it;
      if (find(key, keylen, comp, it)) {
        dataoff = it.data_offset();

        return true;
      }

      return false;
    }

    inline uint64_t index::size() const
    {
      return header_->nkeys;
    }

    inline const void* index::iterator::key() const
    {
      return node_->key(pos_);
    }

    inline keylen_t index::iterator::keylen() const
    {
      return node_->keylen(pos_);
    }

    inline uint64_t index::iterator::data_offset() const
    {
      return node_->data_offset(pos_);
    }

    inline node* index::read_node(uint64_t off)
    {
      return ((off > 0) &&
              (off + kNodeSize <= filesize_) &&
              ((off & (kNodeSize - 1)) == 0)) ?
             reinterpret_cast<struct node*>(
               reinterpret_cast<uint8_t*>(data_) + off
             ) :
             NULL;
    }

    inline const node* index::read_node(uint64_t off) const
    {
      return ((off > 0) &&
              (off + kNodeSize <= filesize_) &&
              ((off & (kNodeSize - 1)) == 0)) ?
             reinterpret_cast<const struct node*>(
               reinterpret_cast<const uint8_t*>(data_) + off
             ) :
             NULL;
    }
  }
}

#endif // DB_INDEX_INDEX_H
