#ifndef DB_INDEX_NODE_H
#define DB_INDEX_NODE_H

#include "types.h"
#include "constants.h"

namespace db {
  namespace index {
    struct node {
      // Node type.
      enum class type : uint8_t {
        kInnerNode,
        kLeafNode
      };

      type t;

      // Offset of the parent node.
      uint64_t parent;

      // Number of entries.
      nodeoff_t nentries;

      // Offset of the next key.
      nodeoff_t nextoff;

      // Constructor.
      node();
    } __attribute__((packed));

    inline node::node()
      : nentries(0),
        nextoff(kNodeSize)
    {
    }
  }
}

#endif // DB_INDEX_NODE_H
