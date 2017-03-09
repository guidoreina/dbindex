#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "index/inner_node.h"

bool db::index::inner_node::add(const void* key,
                               keylen_t keylen,
                               uint64_t child,
                               comparator_t comp)
{
  // If the key is not too long...
  if (keylen <= kKeyMaxLen) {
    // If the key is not in the node...
    nodeoff_t pos;
    if (!search(key, keylen, comp, pos)) {
      return add(key, keylen, child, pos);
    } else {
      entries[pos].child = child;
      return true;
    }
  }

  return false;
}

bool db::index::inner_node::add(const void* key,
                               keylen_t keylen,
                               uint64_t child,
                               nodeoff_t pos)
{
  // If the entry + key fits in the node...
  if (sizeof(entry) + keylen <= available()) {
    // Copy key.
    memcpy(reinterpret_cast<uint8_t*>(this) + nextoff - keylen, key, keylen);

    nextoff -= keylen;

    // If not the last position...
    if (pos < nentries) {
      memmove(&entries[pos + 1],
              &entries[pos],
              (nentries - pos) * sizeof(entry));
    }

    // Fill entry.
    entries[pos].keyoff = nextoff;
    entries[pos].keylen = keylen;
    entries[pos].child = child;

    nentries++;

    return true;
  }

  return false;
}

void db::index::inner_node::split(inner_node* right,
                                  nodeoff_t pos,
                                  const void* key,
                                  keylen_t keylen,
                                  uint64_t child,
                                  void* upkey,
                                  keylen_t& upkeylen)
{
  // Case 1: Current number of entries is odd:
  //
  //      pos:       0     1     2     3     4
  //              +-----+-----+-----+-----+-----+
  //   values:    | 100 | 200 | 300 | 400 | 500 |
  //           +-----+-----+-----+-----+-----+-----+
  // children: | 050 | 150 | 250 | 350 | 450 | 550 |
  //           +-----+-----+-----+-----+-----+-----+
  //
  //   nentries = 5
  //   mid = nentries / 2 = 5 / 2 = 2
  //
  //
  //   Case 1.1: Insert: (key: 275, child: 280) => pos = 2
  //             Element to be added goes to the parent.
  //
  //     Left node:
  //
  //      pos:       0     1
  //              +-----+-----+
  //   values:    | 100 | 200 |
  //           +-----+-----+-----+
  // children: | 050 | 150 | 250 |
  //           +-----+-----+-----+
  //
  //     Right node:
  //
  //      pos:       0     1     2
  //              +-----+-----+-----+
  //   values:    | 300 | 400 | 500 |
  //           +-----+-----+-----+-----+
  // children: | 280 | 350 | 450 | 550 |
  //           +-----+-----+-----+-----+
  //
  //    Key: 275 goes up.
  //
  //
  //   Case 1.2: Insert: (key: 175, child: 180) => pos = 1
  //             Element 200 goes to the parent.
  //
  //     Left node:
  //
  //      pos:       0     1
  //              +-----+-----+
  //   values:    | 100 | 175 |
  //           +-----+-----+-----+
  // children: | 050 | 150 | 180 |
  //           +-----+-----+-----+
  //
  //     Right node:
  //
  //      pos:       0     1     2
  //              +-----+-----+-----+
  //   values:    | 300 | 400 | 500 |
  //           +-----+-----+-----+-----+
  // children: | 250 | 350 | 450 | 550 |
  //           +-----+-----+-----+-----+
  //
  //    Key: 200 goes up.
  //
  //
  //   Case 1.3: Insert: (key: 375, child: 380) => pos = 3
  //             Element 300 goes to the parent.
  //
  //     Left node:
  //
  //      pos:       0     1
  //              +-----+-----+
  //   values:    | 100 | 200 |
  //           +-----+-----+-----+
  // children: | 050 | 150 | 250 |
  //           +-----+-----+-----+
  //
  //     Right node:
  //
  //      pos:       0     1     2
  //              +-----+-----+-----+
  //   values:    | 375 | 400 | 500 |
  //           +-----+-----+-----+-----+
  // children: | 350 | 380 | 450 | 550 |
  //           +-----+-----+-----+-----+
  //
  //    Key: 300 goes up.
  //
  //
  //
  // Case 2: Current number of entries is even:
  //
  //      pos:       0     1     2     3
  //              +-----+-----+-----+-----+
  //   values:    | 100 | 200 | 300 | 400 |
  //           +-----+-----+-----+-----+-----+
  // children: | 050 | 150 | 250 | 350 | 450 |
  //           +-----+-----+-----+-----+-----+
  //
  //   nentries = 4
  //   mid = nentries / 2 = 4 / 2 = 2
  //
  //
  //   Case 2.1: Insert: (key: 275, child: 280) => pos = 2
  //             Element to be added goes to the parent.
  //
  //     Left node:
  //
  //      pos:       0     1
  //              +-----+-----+
  //   values:    | 100 | 200 |
  //           +-----+-----+-----+
  // children: | 050 | 150 | 250 |
  //           +-----+-----+-----+
  //
  //     Right node:
  //
  //      pos:       0     1
  //              +-----+-----+
  //   values:    | 300 | 400 |
  //           +-----+-----+-----+
  // children: | 280 | 350 | 450 |
  //           +-----+-----+-----+
  //
  //    Key: 275 goes up.
  //
  //
  //   Case 2.2: Insert: (key: 175, child: 180) => pos = 1
  //             Element 200 goes to the parent.
  //
  //     Left node:
  //
  //      pos:       0     1
  //              +-----+-----+
  //   values:    | 100 | 175 |
  //           +-----+-----+-----+
  // children: | 050 | 150 | 180 |
  //           +-----+-----+-----+
  //
  //     Right node:
  //
  //      pos:       0     1
  //              +-----+-----+
  //   values:    | 300 | 400 |
  //           +-----+-----+-----+
  // children: | 250 | 350 | 450 |
  //           +-----+-----+-----+
  //
  //    Key: 200 goes up.
  //
  //
  //   Case 2.3: Insert: (key: 375, child: 380) => pos = 3
  //             Element 300 goes to the parent.
  //
  //     Left node:
  //
  //      pos:       0     1
  //              +-----+-----+
  //   values:    | 100 | 200 |
  //           +-----+-----+-----+
  // children: | 050 | 150 | 250 |
  //           +-----+-----+-----+
  //
  //     Right node:
  //
  //      pos:       0     1
  //              +-----+-----+
  //   values:    | 375 | 400 |
  //           +-----+-----+-----+
  // children: | 350 | 380 | 450 |
  //           +-----+-----+-----+
  //
  //    Key: 300 goes up.
  //

  nodeoff_t mid = nentries / 2;

  if (pos > mid) {
    // Case 1.3: New key goes to the right node.

    fill_right(right, mid + 1, pos - (mid + 1), key, keylen, child);

    const struct entry* midentry = entries + mid;

    right->left = midentry->child;

    // Fill key which goes to the parent.
    memcpy(upkey,
           reinterpret_cast<const uint8_t*>(this) + midentry->keyoff,
           midentry->keylen);

    upkeylen = midentry->keylen;

    // Defragment current node.
    defrag();
  } else {
    fill_right(right, mid);

    if (pos < mid) {
      // Case 1.2: New key goes to the left node.

      const struct entry* midentry = entries + mid - 1;

      right->left = midentry->child;

      // Defragment current node and add key.
      defrag(midentry, pos, key, keylen, child, upkey, upkeylen);
    } else {
      // Case 1.1: New key goes to the parent node.

      right->left = child;

      // Fill key which goes to the parent.
      memcpy(upkey, key, keylen);
      upkeylen = keylen;

      // Defragment current node.
      defrag();
    }
  }
}

bool db::index::inner_node::search(const void* key,
                                   keylen_t keylen,
                                   comparator_t comp,
                                   nodeoff_t& pos) const
{
  int i = 0;
  int j = nentries - 1;

  while (i <= j) {
    int mid = (i + j) / 2;

    int ret = comp(key,
                   keylen,
                   reinterpret_cast<const uint8_t*>(this) + entries[mid].keyoff,
                   entries[mid].keylen);

    if (ret < 0) {
      j = mid - 1;
    } else if (ret == 0) {
      pos = static_cast<nodeoff_t>(mid);
      return true;
    } else {
      i = mid + 1;
    }
  }

  pos = static_cast<nodeoff_t>(i);

  return false;
}

void db::index::inner_node::print() const
{
  printf("Index:\n");

  printf("\tLeft: %lu.\n\n", left);

  for (nodeoff_t i = 0; i < nentries; i++) {
    printf("\t[%03u] Length: %u, key: '%.*s', child: %lu.\n",
           i + 1,
           entries[i].keylen,
           entries[i].keylen,
           reinterpret_cast<const uint8_t*>(this) + entries[i].keyoff,
           entries[i].child);
  }
}

void db::index::inner_node::fill_right(inner_node* right,
                                       nodeoff_t mid,
                                       nodeoff_t pos,
                                       const void* key,
                                       keylen_t keylen,
                                       uint64_t child)
{
  // Number of entries in the right node.
  nodeoff_t n = nentries - mid + 1;

  // Pointer to the middle entry.
  const struct entry* midentry = entries + mid;

  // Pointer to the entry where the key would be inserted in the current node.
  const struct entry* posentry = midentry + pos;

  // Pointer to the current entry in the current node.
  const struct entry* src = entries + (nentries - 1);

  // Pointer to the current entry in the right node.
  struct entry* dest = right->entries + (n - 1);

  // Copy keys from the current node to the right node in the range
  // [entries[mid + pos], entries[nentries]).
  nodeoff_t off;
  for (off = kNodeSize; src >= posentry; src--, dest--) {
    keylen_t len = src->keylen;
    off -= len;

    memcpy(reinterpret_cast<uint8_t*>(right) + off,
           reinterpret_cast<const uint8_t*>(this) + src->keyoff,
           len);

    dest->keyoff = off;
    dest->keylen = len;
    dest->child = src->child;
  }

  // Copy key.
  off -= keylen;
  memcpy(reinterpret_cast<uint8_t*>(right) + off, key, keylen);

  dest->keyoff = off;
  dest->keylen = keylen;
  dest->child = child;

  dest--;

  // Copy keys from the current node to the right node in the range
  // [entries[mid], entries[mid + pos]).
  for (; src >= midentry; src--, dest--) {
    keylen_t len = src->keylen;
    off -= len;

    memcpy(reinterpret_cast<uint8_t*>(right) + off,
           reinterpret_cast<const uint8_t*>(this) + src->keyoff,
           len);

    dest->keyoff = off;
    dest->keylen = len;
    dest->child = src->child;
  }

  right->t = t;

  right->parent = parent;

  right->nextoff = off;

  right->nentries = n;

  nentries -= n;
}

void db::index::inner_node::fill_right(inner_node* right, nodeoff_t mid)
{
  // Number of entries in the right node.
  nodeoff_t n = nentries - mid;

  // Pointer to the middle entry.
  const struct entry* midentry = entries + mid;

  // Pointer to the current entry in the current node.
  const struct entry* src = entries + (nentries - 1);

  // Pointer to the current entry in the right node.
  struct entry* dest = right->entries + (n - 1);

  // Copy keys from the current node to the right node in the range
  // [entries[mid], entries[nentries]).
  nodeoff_t off;
  for (off = kNodeSize; src >= midentry; src--, dest--) {
    keylen_t keylen = src->keylen;
    off -= keylen;

    memcpy(reinterpret_cast<uint8_t*>(right) + off,
           reinterpret_cast<const uint8_t*>(this) + src->keyoff,
           keylen);

    dest->keyoff = off;
    dest->keylen = keylen;
    dest->child = src->child;
  }

  right->t = t;

  right->parent = parent;

  right->nextoff = off;

  right->nentries = n;

  nentries -= n;
}

void db::index::inner_node::defrag()
{
  // Pointer to the current entry in the current node.
  struct entry* src = entries + (nentries - 1);

  uint8_t data[kNodeSize];
  for (nextoff = kNodeSize; src >= entries; src--) {
    keylen_t keylen = src->keylen;
    nextoff -= keylen;

    memcpy(data + nextoff,
           reinterpret_cast<const uint8_t*>(this) + src->keyoff,
           keylen);

    src->keyoff = nextoff;
  }

  memcpy(reinterpret_cast<uint8_t*>(this) + nextoff,
         data + nextoff,
         sizeof(data) - nextoff);
}

void db::index::inner_node::defrag(const struct entry* midentry,
                                   nodeoff_t pos,
                                   const void* key,
                                   keylen_t keylen,
                                   uint64_t child,
                                   void* upkey,
                                   keylen_t& upkeylen)
{
  // Pointer to the entry where the key will be inserted in the current node.
  const struct entry* posentry = entries + pos;

  // Pointer to the source entry in the current node.
  const struct entry* src = entries + (nentries - 2);

  // Pointer to the destination entry in the current node.
  struct entry* dest = entries + nentries - 1;

  // Save offset and length of the middle entry.
  nodeoff_t midentryoff = midentry->keyoff;
  keylen_t midentrylen = midentry->keylen;

  // Defragment keys in the range [entries[pos], entries[nentries]).
  uint8_t data[kNodeSize];
  for (nextoff = kNodeSize; src >= posentry; src--, dest--) {
    keylen_t len = src->keylen;
    nextoff -= len;

    memcpy(data + nextoff,
           reinterpret_cast<const uint8_t*>(this) + src->keyoff,
           len);

    dest->keyoff = nextoff;
    dest->keylen = len;
    dest->child = src->child;
  }

  // Copy key.
  nextoff -= keylen;
  memcpy(data + nextoff, key, keylen);

  dest->keyoff = nextoff;
  dest->keylen = keylen;
  dest->child = child;

  dest--;

  // Defragment keys in the range [entries[0], entries[pos]).
  for (; src >= entries; src--, dest--) {
    keylen_t len = src->keylen;
    nextoff -= len;

    memcpy(data + nextoff,
           reinterpret_cast<const uint8_t*>(this) + src->keyoff,
           len);

    dest->keyoff = nextoff;
    dest->keylen = len;
    dest->child = src->child;
  }

  // Fill key which goes to the parent.
  memcpy(upkey,
         reinterpret_cast<const uint8_t*>(this) + midentryoff,
         midentrylen);

  upkeylen = midentrylen;

  memcpy(reinterpret_cast<uint8_t*>(this) + nextoff,
         data + nextoff,
         sizeof(data) - nextoff);
}
