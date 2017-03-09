#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <new>
#include "index/leaf_node.h"
#include "index/inner_node.h"

static const keylen_t kKeyLength = 32;

static int comp(const void* key1,
                keylen_t keylen1,
                const void* key2,
                keylen_t keylen2);

static bool test1();
static bool test2();
static bool test3();
static bool test4();

int main()
{
  return test1() ? 0 : -1;
  //return test2() ? 0 : -1;
  //return test3() ? 0 : -1;
  //return test4() ? 0 : -1;
}

bool test1()
{
  uint8_t data[kNodeSize];

  // Create leaf node.
  db::index::leaf_node* node = new (data) db::index::leaf_node();

  // Add keys.
  nodeoff_t nentries;
  for (nentries = 0; ; nentries++) {
    char key[kKeyLength + 1];
    keylen_t keylen = snprintf(key, sizeof(key), "%0*u", kKeyLength, nentries);

    if (!node->add(key, keylen, 0, comp)) {
      break;
    }
  }

  // Search keys.
  for (nodeoff_t i = 0; i < nentries; i++) {
    char key[kKeyLength + 1];
    keylen_t keylen = snprintf(key, sizeof(key), "%0*u", kKeyLength, i);

    nodeoff_t pos;
    if (!node->search(key, keylen, comp, pos)) {
      fprintf(stderr, "Key '%s' not found.\n", key);
      return false;
    }

    if (pos != i) {
      fprintf(stderr,
              "Key '%s' found at the wrong position (position: %u, expected: "
              "%u).\n",
              key,
              pos,
              i);

      return false;
    }
  }

  // Print keys.
  node->print();

  // Delete keys.
  for (nodeoff_t i = 0; i < nentries; i++) {
    char key[kKeyLength + 1];
    keylen_t keylen = snprintf(key, sizeof(key), "%0*u", kKeyLength, i);

    if (!node->erase(key, keylen, comp)) {
      fprintf(stderr, "Error deleting key '%s' (not found).\n", key);
      return false;
    }
  }

  // Print keys.
  printf("After deleting keys...\n");
  node->print();

  return true;
}

bool test2()
{
  uint8_t data1[kNodeSize];

  // Create leaf node.
  db::index::leaf_node* node = new (data1) db::index::leaf_node();

  // Calculate the number of keys which fit in a node.
  nodeoff_t nentries;
  for (nentries = 0; ; nentries++) {
    char key[kKeyLength + 1];
    keylen_t keylen = snprintf(key, sizeof(key), "%0*u", kKeyLength, nentries);

    if (!node->add(key, keylen, 0, comp)) {
      break;
    }
  }

  printf("%u keys fit in a node.\n", nentries);
  printf("==========================================================\n");

  nodeoff_t count1 = 0;
  for (nodeoff_t i = 0; i <= nentries; i++) {
    // Create leaf node.
    node = new (data1) db::index::leaf_node();

    // Add keys.
    nodeoff_t count2 = 1;
    for (nodeoff_t j = 0; j < nentries; j++) {
      char key[kKeyLength + 1];
      keylen_t keylen = snprintf(key, sizeof(key), "%0*u", kKeyLength, count2);

      if (!node->add(key, keylen, 0, comp)) {
        fprintf(stderr, "Error adding key '%s'.\n", key);
        return false;
      }

      count2 += 2;
    }

    char key[kKeyLength + 1];
    keylen_t keylen = snprintf(key, sizeof(key), "%0*u", kKeyLength, count1);

    // Search position of key which produces the split.
    nodeoff_t pos;
    node->search(key, keylen, comp, pos);

    uint8_t data2[kNodeSize];

    // Create leaf node.
    db::index::leaf_node* right = new (data2) db::index::leaf_node();

    // Split node and add key.
    node->split(kNodeSize, 2 * kNodeSize, right, pos, key, keylen, 0);

    printf("Key which produces the split is '%s'.\n\n", key);

    node->print();
    printf("\n");
    right->print();

    printf("==========================================================\n");

    count1 += 2;
  }

  return true;
}

bool test3()
{
  uint8_t data[kNodeSize];

  // Create inner node.
  db::index::inner_node* node = new (data) db::index::inner_node();

  node->left = 0;

  // Add keys.
  nodeoff_t nentries;
  nodeoff_t count = 0;
  for (nentries = 0; ; nentries++) {
    char key[kKeyLength + 1];
    keylen_t keylen = snprintf(key, sizeof(key), "%0*u", kKeyLength, count);

    if (!node->add(key, keylen, count + 1, comp)) {
      break;
    }

    count += 2;
  }

  // Search keys.
  count = 0;
  for (nodeoff_t i = 0; i < nentries; i++) {
    char key[kKeyLength + 1];
    keylen_t keylen = snprintf(key, sizeof(key), "%0*u", kKeyLength, count);

    nodeoff_t pos;
    if (!node->search(key, keylen, comp, pos)) {
      fprintf(stderr, "Key '%s' not found.\n", key);
      return false;
    }

    if (pos != i) {
      fprintf(stderr,
              "Key '%s' found at the wrong position (position: %u, expected: "
              "%u).\n",
              key,
              pos,
              i);

      return false;
    }

    count += 2;
  }

  // Print keys.
  node->print();

  return true;
}

bool test4()
{
  uint8_t data1[kNodeSize];

  // Create inner node.
  db::index::inner_node* node = new (data1) db::index::inner_node();

  // Calculate the number of keys which fit in a node.
  nodeoff_t nentries;
  for (nentries = 0; ; nentries++) {
    char key[kKeyLength + 1];
    keylen_t keylen = snprintf(key, sizeof(key), "%0*u", kKeyLength, nentries);

    if (!node->add(key, keylen, 0, comp)) {
      break;
    }
  }

  printf("%u keys fit in a node.\n", nentries);
  printf("==========================================================\n");

  nodeoff_t count1 = 0;
  for (nodeoff_t i = 0; i <= nentries; i++) {
    // Create leaf node.
    node = new (data1) db::index::inner_node();

    node->left = 0;

    // Add keys.
    nodeoff_t count2 = 1;
    for (nodeoff_t j = 0; j < nentries; j++) {
      char key[kKeyLength + 1];
      keylen_t keylen = snprintf(key, sizeof(key), "%0*u", kKeyLength, count2);

      if (!node->add(key, keylen, count2 + 1, comp)) {
        fprintf(stderr, "Error adding key '%s'.\n", key);
        return false;
      }

      count2 += 2;
    }

    char key[kKeyLength + 1];
    keylen_t keylen = snprintf(key, sizeof(key), "%0*u", kKeyLength, count1);

    // Search position of key which produces the split.
    nodeoff_t pos;
    node->search(key, keylen, comp, pos);

    uint8_t data2[kNodeSize];

    uint8_t upkey[kKeyMaxLen];
    keylen_t upkeylen;

    // Create leaf node.
    db::index::inner_node* right = new (data2) db::index::inner_node();

    // Split node and add key.
    node->split(right, pos, key, keylen, count1 + 1, upkey, upkeylen);

    printf("Key which produces the split is '%s'.\n", key);
    printf("Key which goes up '%.*s'.\n\n",
           upkeylen,
           reinterpret_cast<const char*>(upkey));

    node->print();
    printf("\n");
    right->print();

    printf("==========================================================\n");

    count1 += 2;
  }

  return true;
}

int comp(const void* key1,
         keylen_t keylen1,
         const void* key2,
         keylen_t keylen2)
{
  keylen_t len = (keylen1 < keylen2) ? keylen1 : keylen2;
  return memcmp(key1, key2, len);
}
