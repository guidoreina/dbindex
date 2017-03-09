#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <limits.h>
#include "index/index.h"

static const keylen_t kKeyMinLength = 20;

static void usage(const char* program);

static int comp(const void* key1,
                keylen_t keylen1,
                const void* key2,
                keylen_t keylen2);

int main(int argc, const char** argv)
{
  if (argc != 4) {
    usage(argv[0]);
    return -1;
  }

  char* endptr;
  uint64_t nkeys = strtoull(argv[1], &endptr, 10);
  if ((*endptr) || (nkeys == 0)) {
    usage(argv[0]);
    return -1;
  }

  unsigned long n = strtoul(argv[2], &endptr, 10);
  if ((*endptr) || (n < kKeyMinLength) || (n > kKeyMaxLen)) {
    usage(argv[0]);
    return -1;
  }

  bool forward;
  if (strcasecmp(argv[3], "--add-forward") == 0) {
    forward = true;
  } else if (strcasecmp(argv[3], "--add-backward") == 0) {
    forward = false;
  } else {
    usage(argv[0]);
    return -1;
  }

  keylen_t keylen = static_cast<keylen_t>(n);

  db::index::index index;
  if (!index.open("index.idx")) {
    fprintf(stderr, "Error opening index.\n");
    return -1;
  }

  // Add keys.
  if (forward) {
    printf("Adding keys (forward)...\n");
    for (uint64_t i = 0; i < nkeys; i++) {
      char key[kKeyMaxLen + 1];
      keylen_t len = snprintf(key, sizeof(key), "%0*zu", keylen, i);

      if (!index.add(key, len, i, comp)) {
        fprintf(stderr, "Error adding key '%s'.\n", key);
        return -1;
      }
    }
  } else {
    printf("Adding keys (backward)...\n");
    for (uint64_t i = nkeys; i > 0; i--) {
      char key[kKeyMaxLen + 1];
      keylen_t len = snprintf(key, sizeof(key), "%0*zu", keylen, i - 1);

      if (!index.add(key, len, i - 1, comp)) {
        fprintf(stderr, "Error adding key '%s'.\n", key);
        return -1;
      }
    }
  }

  // Search keys.
  printf("Searching keys.\n");
  for (uint64_t i = 0; i < nkeys; i++) {
    char key[kKeyMaxLen + 1];
    keylen_t len = snprintf(key, sizeof(key), "%0*zu", keylen, i);

    uint64_t dataoff;
    if (!index.find(key, len, comp, dataoff)) {
      fprintf(stderr, "Error finding key '%s'.\n", key);
      return -1;
    }

    if (dataoff != i) {
      fprintf(stderr,
              "Unexpected data offset %lu, expected %lu.\n",
              dataoff,
              i);

      return -1;
    }
  }

  // Iterate keys (forward).
  printf("Iterating keys (forward)...\n");
  db::index::index::iterator it;
  if (index.begin(it)) {
    uint64_t i = 0;

    do {
      char key[kKeyMaxLen + 1];
      keylen_t len = snprintf(key, sizeof(key), "%0*zu", keylen, i);

      if (it.keylen() != len) {
        fprintf(stderr,
                "Unexpected length %u of key '%s'.\n", it.keylen(), key);

        return -1;
      }

      if (memcmp(it.key(), key, len) != 0) {
        fprintf(stderr,
                "Keys differ (key: '%.*s', expected: '%s').\n",
                it.keylen(),
                reinterpret_cast<const char*>(it.key()),
                key);

        return -1;
      }

      if (it.data_offset() != i) {
        fprintf(stderr,
                "Unexpected data offset %lu, expected %lu.\n",
                it.data_offset(),
                i);

        return -1;
      }

      i++;
    } while (index.next(it));

    if (i != nkeys) {
      fprintf(stderr,
              "Keys are missing (# keys: %lu, expected: %lu).\n",
              i,
              nkeys);

      return -1;
    }
  } else {
    fprintf(stderr, "Error getting first key.\n");
    return -1;
  }

  // Iterate keys (backward).
  printf("Iterating keys (backward)...\n");
  if (index.end(it)) {
    uint64_t i = nkeys;

    do {
      char key[kKeyMaxLen + 1];
      keylen_t len = snprintf(key, sizeof(key), "%0*zu", keylen, i - 1);

      if (it.keylen() != len) {
        fprintf(stderr,
                "Unexpected length %u of key '%s'.\n", it.keylen(), key);

        return -1;
      }

      if (memcmp(it.key(), key, len) != 0) {
        fprintf(stderr,
                "Keys differ (key: '%.*s', expected: '%s').\n",
                it.keylen(),
                reinterpret_cast<const char*>(it.key()),
                key);

        return -1;
      }

      if (it.data_offset() != i - 1) {
        fprintf(stderr,
                "Unexpected data offset %lu, expected %lu.\n",
                it.data_offset(),
                i);

        return -1;
      }

      i--;
    } while (index.previous(it));

    if (i != 0) {
      fprintf(stderr,
              "Keys are missing (# keys: %lu, expected: %lu).\n",
              nkeys - i,
              nkeys);

      return -1;
    }
  } else {
    fprintf(stderr, "Error getting last key.\n");
    return -1;
  }

  // Search keys.
  printf("Searching keys...\n");
  for (uint64_t i = 0; i < nkeys; i++) {
    char key[kKeyMaxLen + 1];
    keylen_t len = snprintf(key, sizeof(key), "%0*zu", keylen, i);

    if (!index.find(key, len, comp, it)) {
      fprintf(stderr, "Error finding key '%s'.\n", key);
      return -1;
    }

    if (it.data_offset() != i) {
      fprintf(stderr,
              "Unexpected data offset %lu, expected %lu.\n",
              it.data_offset(),
              i);

      return -1;
    }
  }

  uint64_t to_delete = nkeys / 4;

  // Erase keys at the beginning.
  printf("Erasing keys at the beginning.\n");
  for (uint64_t i = 0; i < to_delete; i++) {
    char key[kKeyMaxLen + 1];
    keylen_t len = snprintf(key, sizeof(key), "%0*zu", keylen, i);

    if (!index.erase(key, len, comp)) {
      fprintf(stderr, "Error erasing key '%s'.\n", key);
      return -1;
    }
  }

  // Erase keys at the end.
  printf("Erasing keys at the end.\n");
  for (uint64_t i = nkeys - to_delete; i < nkeys; i++) {
    char key[kKeyMaxLen + 1];
    keylen_t len = snprintf(key, sizeof(key), "%0*zu", keylen, i);

    if (!index.erase(key, len, comp)) {
      fprintf(stderr, "Error erasing key '%s'.\n", key);
      return -1;
    }
  }

  printf("Iterating keys (forward)...\n");
  if (index.begin(it)) {
    uint64_t i = to_delete;

    do {
      char key[kKeyMaxLen + 1];
      keylen_t len = snprintf(key, sizeof(key), "%0*zu", keylen, i);

      if (it.keylen() != len) {
        fprintf(stderr,
                "Unexpected length %u of key '%s'.\n", it.keylen(), key);

        return -1;
      }

      if (memcmp(it.key(), key, len) != 0) {
        fprintf(stderr,
                "Keys differ (key: '%.*s', expected: '%s').\n",
                it.keylen(),
                reinterpret_cast<const char*>(it.key()),
                key);

        return -1;
      }

      if (it.data_offset() != i) {
        fprintf(stderr,
                "Unexpected data offset %lu, expected %lu.\n",
                it.data_offset(),
                i);

        return -1;
      }

      i++;
    } while (index.next(it));

    if (i != nkeys - to_delete) {
      fprintf(stderr,
              "Keys are missing (stopped at position: %lu, expected "
              "position: %lu).\n",
              i,
              nkeys - to_delete);

      return -1;
    }
  } else {
    fprintf(stderr, "Error getting first key.\n");
    return -1;
  }

  // Search keys.
  printf("Searching keys...\n");
  for (uint64_t i = 0; i < nkeys; i++) {
    char key[kKeyMaxLen + 1];
    keylen_t len = snprintf(key, sizeof(key), "%0*zu", keylen, i);

    uint64_t dataoff;

    bool found = index.find(key, len, comp, dataoff);

    if (found != ((i >= to_delete) && (i < nkeys - to_delete))) {
      fprintf(stderr,
              "Key '%s' should%s have been found.\n",
              key,
              ((i < to_delete) || (i >= nkeys - to_delete)) ? "n't" : "");

      return -1;
    }
  }

  return 0;
}

void usage(const char* program)
{
  printf("Usage: %s <number-keys> <key-length> --add-forward | "
         "--add-backward\n",
         program);

  printf("<number-keys> ::= 1 .. %llu\n", ULLONG_MAX);
  printf("<key-length> ::= %u .. %u\n", kKeyMinLength, kKeyMaxLen);
}

int comp(const void* key1,
         keylen_t keylen1,
         const void* key2,
         keylen_t keylen2)
{
  keylen_t len = (keylen1 < keylen2) ? keylen1 : keylen2;

  int ret;
  if ((ret = memcmp(key1, key2, len)) < 0) {
    return -1;
  } else if (ret > 0) {
    return +1;
  } else {
    return (keylen1 - keylen2);
  }
}
