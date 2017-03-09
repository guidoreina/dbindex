#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "index/leaf_node.h"

bool db::index::leaf_node::add(const void* key,
                               keylen_t keylen,
                               uint64_t dataoff,
                               comparator_t comp)
{
  // If the key is not too long...
  if (keylen <= kKeyMaxLen) {
    // If the key is not in the node...
    nodeoff_t pos;
    if (!search(key, keylen, comp, pos)) {
      return add(key, keylen, dataoff, pos);
    } else {
      entries[pos].dataoff = dataoff;
      entries[pos].deleted = 0;

      return true;
    }
  }

  return false;
}

bool db::index::leaf_node::add(const void* key,
                               keylen_t keylen,
                               uint64_t dataoff,
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
    entries[pos].dataoff = dataoff;
    entries[pos].deleted = 0;

    nentries++;

    return true;
  }

  return false;
}

bool db::index::leaf_node::erase(const void* key,
                                 keylen_t keylen,
                                 comparator_t comp)
{
  // If the key is not too long...
  if (keylen <= kKeyMaxLen) {
    // If the key is in the node...
    nodeoff_t pos;
    if ((search(key, keylen, comp, pos)) && (!erased(pos))) {
      // Mark the key as deleted.
      entries[pos].deleted = 1;

      return true;
    }
  }

  return false;
}

void db::index::leaf_node::split(uint64_t leftoff,
                                 uint64_t rightoff,
                                 leaf_node* right,
                                 nodeoff_t pos,
                                 const void* key,
                                 keylen_t keylen,
                                 uint64_t dataoff)
{
  // Case 1: Current number of entries is odd:
  //
  //      pos:    0    1    2    3   4
  //           +----+----+----+----+----+
  //   values: | 10 | 20 | 30 | 40 | 50 |
  //           +----+----+----+----+----+
  //
  //   nentries = 5
  //   mid = (nentries + 1) / 2 = (5 + 1) / 2 = 6 / 2 = 3
  //
  //
  //   Case 1.1: Insert: 25 => pos = 2
  //
  //     Left node:
  //
  //        pos:    0    1    2    3   4
  //             +----+----+----+----+----+
  //     values: | 10 | 20 | 25 |    |    |
  //             +----+----+----+----+----+
  //
  //     Right node:
  //
  //        pos:    0    1    2    3   4
  //             +----+----+----+----+----+
  //     values: | 30 | 40 | 50 |    |    |
  //             +----+----+----+----+----+
  //
  //    pos (2) < mid (3)
  //    Copy to right node from position mid - 1 (position 2)
  //
  //
  //   Case 1.2: Insert: 35 => pos = 3
  //
  //     Left node:
  //
  //        pos:    0    1    2    3   4
  //             +----+----+----+----+----+
  //     values: | 10 | 20 | 30 |    |    |
  //             +----+----+----+----+----+
  //
  //     Right node:
  //
  //        pos:    0    1    2    3   4
  //             +----+----+----+----+----+
  //     values: | 35 | 40 | 50 |    |    |
  //             +----+----+----+----+----+
  //
  //    pos (3) >= mid (3)
  //    Copy to right node from position mid (position 3)
  //    New key is at position pos (3) - mid (3) = 0
  //
  //
  //
  // Case 2: Current number of entries is even:
  //
  //      pos:    0    1    2    3
  //           +----+----+----+----+
  //   values: | 10 | 20 | 30 | 40 |
  //           +----+----+----+----+
  //
  //   nentries = 4
  //   mid = (nentries + 1) / 2 = (4 + 1) / 2 = 5 / 2 = 2
  //
  //
  //   Case 2.1: Insert: 25 => pos = 2
  //
  //     Left node:
  //
  //        pos:    0    1    2    3   4
  //             +----+----+----+----+----+
  //     values: | 10 | 20 |    |    |    |
  //             +----+----+----+----+----+
  //
  //     Right node:
  //
  //        pos:    0    1    2    3   4
  //             +----+----+----+----+----+
  //     values: | 25 | 30 | 40 |    |    |
  //             +----+----+----+----+----+
  //
  //    pos (2) >= mid (2)
  //    Copy to right node from position mid (position 2)
  //    New key is at position pos (2) - mid (2) = 0
  //
  //
  //   Case 2.2: Insert: 15 => pos = 1
  //
  //     Left node:
  //
  //        pos:    0    1    2    3   4
  //             +----+----+----+----+----+
  //     values: | 10 | 15 |    |    |    |
  //             +----+----+----+----+----+
  //
  //     Right node:
  //
  //        pos:    0    1    2    3   4
  //             +----+----+----+----+----+
  //     values: | 20 | 30 | 40 |    |    |
  //             +----+----+----+----+----+
  //
  //    pos (1) < mid (2)
  //    Copy to right node from position mid - 1 (position 1)
  //

  nodeoff_t mid = (nentries + 1) / 2;

  if (pos >= mid) {
    fill_right(leftoff, rightoff, right, mid, pos - mid, key, keylen, dataoff);

    // Defragment current node.
    defrag();
  } else {
    fill_right(leftoff, rightoff, right, mid - 1);

    // Defragment current node and add key.
    defrag(pos, key, keylen, dataoff);
  }
}

bool db::index::leaf_node::search(const void* key,
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

void db::index::leaf_node::print() const
{
  printf("Index:\n");

  for (nodeoff_t i = 0; i < nentries; i++) {
    printf("\t[%03u] %sLength: %u, key: '%.*s'.\n",
           i + 1,
           erased(i) ? "[Deleted] " : "",
           keylen(i),
           keylen(i),
           reinterpret_cast<const char*>(key(i)));
  }
}

void db::index::leaf_node::fill_right(uint64_t leftoff,
                                      uint64_t rightoff,
                                      leaf_node* right,
                                      nodeoff_t mid,
                                      nodeoff_t pos,
                                      const void* key,
                                      keylen_t keylen,
                                      uint64_t dataoff)
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
    dest->dataoff = src->dataoff;
    dest->deleted = src->deleted;
  }

  // Copy key.
  off -= keylen;
  memcpy(reinterpret_cast<uint8_t*>(right) + off, key, keylen);

  dest->keyoff = off;
  dest->keylen = keylen;
  dest->dataoff = dataoff;
  dest->deleted = 0;

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
    dest->dataoff = src->dataoff;
    dest->deleted = src->deleted;
  }

  right->t = t;

  right->parent = parent;
  right->prev = leftoff;
  right->next = next;

  right->nextoff = off;

  right->nentries = n;

  nentries = mid;

  next = rightoff;
}

void db::index::leaf_node::fill_right(uint64_t leftoff, 
                                      uint64_t rightoff,
                                      leaf_node* right,
                                      nodeoff_t mid)
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
    dest->dataoff = src->dataoff;
    dest->deleted = src->deleted;
  }

  right->t = t;

  right->parent = parent;
  right->prev = leftoff;
  right->next = next;

  right->nextoff = off;

  right->nentries = n;

  nentries -= n;

  next = rightoff;
}

void db::index::leaf_node::defrag()
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

void db::index::leaf_node::defrag(nodeoff_t pos,
                                  const void* key,
                                  keylen_t keylen,
                                  uint64_t dataoff)
{
  // Pointer to the entry where the key will be inserted in the current node.
  const struct entry* posentry = entries + pos;

  // Pointer to the source entry in the current node.
  const struct entry* src = entries + (nentries - 1);

  // Pointer to the destination entry in the current node.
  struct entry* dest = entries + nentries;

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
    dest->dataoff = src->dataoff;
    dest->deleted = src->deleted;
  }

  // Copy key.
  nextoff -= keylen;
  memcpy(data + nextoff, key, keylen);

  dest->keyoff = nextoff;
  dest->keylen = keylen;
  dest->dataoff = dataoff;
  dest->deleted = 0;

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
    dest->dataoff = src->dataoff;
    dest->deleted = src->deleted;
  }

  memcpy(reinterpret_cast<uint8_t*>(this) + nextoff,
         data + nextoff,
         sizeof(data) - nextoff);

  nentries++;
}
