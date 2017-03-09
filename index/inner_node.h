#ifndef DB_INDEX_INNER_NODE_H
#define DB_INDEX_INNER_NODE_H

#include <stddef.h>
#include "node.h"

namespace db {
  namespace index {
    struct inner_node : public node {
      public:
        // Offset of the first child with smaller keys.
        uint64_t left;

        struct entry {
          // Offset of the key in the node.
          nodeoff_t keyoff;

          // Length of the key.
          keylen_t keylen;

          // Offset of the first child with keys greater than the key.
          uint64_t child;
        } __attribute__((packed));

        // Dynamic array of entries.
        entry entries[1];

        // The keys are stored starting from the end of the node.

        // Add.
        bool add(const void* key,
                 keylen_t keylen,
                 uint64_t child,
                 comparator_t comp);

        bool add(const void* key,
                 keylen_t keylen,
                 uint64_t child,
                 nodeoff_t pos);

        // Split.
        void split(inner_node* right,
                   nodeoff_t pos,
                   const void* key,
                   keylen_t keylen,
                   uint64_t child,
                   void* upkey,
                   keylen_t& upkeylen);

        // Search.
        bool search(const void* key,
                    keylen_t keylen,
                    comparator_t comp,
                    nodeoff_t& pos) const;

        // Print.
        void print() const;

      private:
        // Available space.
        nodeoff_t available() const;

        // Fill right node.
        void fill_right(inner_node* right,
                        nodeoff_t mid,
                        nodeoff_t pos,
                        const void* key,
                        keylen_t keylen,
                        uint64_t child);

        void fill_right(inner_node* right, nodeoff_t mid);

        // Defragment node.
        void defrag();

        // Defragment node and add key.
        void defrag(const struct entry* midentry,
                    nodeoff_t pos,
                    const void* key,
                    keylen_t keylen,
                    uint64_t child,
                    void* upkey,
                    keylen_t& upkeylen);
    } __attribute__((packed));

    inline nodeoff_t inner_node::available() const
    {
      return (nextoff -
              (offsetof(inner_node, entries) + (nentries * sizeof(entry))));
    }
  }
}

#endif // DB_INDEX_INNER_NODE_H
