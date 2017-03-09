#ifndef DB_INDEX_LEAF_NODE_H
#define DB_INDEX_LEAF_NODE_H

#include <stddef.h>
#include "node.h"

namespace db {
  namespace index {
    struct leaf_node : public node {
      public:
        // Offset of the previous node.
        uint64_t prev;

        // Offset of the next node.
        uint64_t next;

        struct entry {
          // Offset of the key in the node.
          nodeoff_t keyoff;

          // Length of the key.
          keylen_t keylen:(sizeof(keylen_t) * 8) - 1;

          // Deleted?
          keylen_t deleted:1;

          // Offset of the data.
          uint64_t dataoff;
        } __attribute__((packed));

        // Dynamic array of entries.
        entry entries[1];

        // The keys are stored starting from the end of the node.

        // Add.
        bool add(const void* key,
                 keylen_t keylen,
                 uint64_t dataoff,
                 comparator_t comp);

        bool add(const void* key,
                 keylen_t keylen,
                 uint64_t dataoff,
                 nodeoff_t pos);

        // Erase (marks the key as deleted).
        bool erase(const void* key, keylen_t keylen, comparator_t comp);

        // Split.
        void split(uint64_t leftoff, // Offset of the node in disk.
                   uint64_t rightoff, // Offset of the right node in disk.
                   leaf_node* right,
                   nodeoff_t pos,
                   const void* key,
                   keylen_t keylen,
                   uint64_t dataoff);

        // Search.
        bool search(const void* key,
                    keylen_t keylen,
                    comparator_t comp,
                    nodeoff_t& pos) const;

        // Print.
        void print() const;

        // Get key at position.
        const void* key(nodeoff_t pos) const;

        // Get key length at position.
        keylen_t keylen(nodeoff_t pos) const;

        // Erased?
        bool erased(nodeoff_t pos) const;

        // Get data offset.
        uint64_t data_offset(nodeoff_t pos) const;

      private:
        // Available space.
        nodeoff_t available() const;

        // Fill right node.
        void fill_right(uint64_t leftoff, // Offset of the node in disk.
                        uint64_t rightoff, // Offset of the right node in disk.
                        leaf_node* right,
                        nodeoff_t mid,
                        nodeoff_t pos,
                        const void* key,
                        keylen_t keylen,
                        uint64_t dataoff);

        void fill_right(uint64_t leftoff, // Offset of the node in disk.
                        uint64_t rightoff, // Offset of the right node in disk.
                        leaf_node* right,
                        nodeoff_t mid);

        // Defragment node.
        void defrag();

        // Defragment node and add key.
        void defrag(nodeoff_t pos,
                    const void* key,
                    keylen_t keylen,
                    uint64_t dataoff);
    } __attribute__((packed));

    inline const void* leaf_node::key(nodeoff_t pos) const
    {
      return reinterpret_cast<const uint8_t*>(this) + entries[pos].keyoff;
    }

    inline keylen_t leaf_node::keylen(nodeoff_t pos) const
    {
      return entries[pos].keylen;
    }

    inline bool leaf_node::erased(nodeoff_t pos) const
    {
      return (entries[pos].deleted != 0);
    }

    inline uint64_t leaf_node::data_offset(nodeoff_t pos) const
    {
      return entries[pos].dataoff;
    }

    inline nodeoff_t leaf_node::available() const
    {
      return (nextoff -
              (offsetof(leaf_node, entries) + (nentries * sizeof(entry))));
    }
  }
}

#endif // DB_INDEX_LEAF_NODE_H
