dbindex
=======
Database index class implementing a b+-tree on disk.

Constants:
* Node size: 4 KB.
* Maximum key length: 512 bytes.

Notes:
* The index doesn't accept duplicates (if the same key is added twice, the value is overwritten).
* It is not thread-safe.
* The key's value is a `uint64_t`, which can be the offset of the data in a data file.

Operations:
* Add.
* Delete (just marks the key as deleted).
* Find.
* Iterate (`begin()`, `end()`, `previous()`, `next()`).

The caller must provide a comparator for adding, deleting and finding keys.
The prototype of the comparator is:
```
int comp(const void* key1, keylen_t keylen1, const void* key2, keylen_t keylen2);
```

Example of comparator:
```
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
```

Example of use of the index class:
```
db::index::index index;

// Open index file.
index.open("index.idx");

// Add keys.
index.add("test0", 5, 0, comp);
index.add("test1", 5, 0, comp);
index.add("test2", 5, 0, comp);

// Iterate.
db::index::index::iterator it;
if (index.begin(it)) {
  do {
    // Print key.
    printf("Key: '%.*s'.\n", it.keylen(), reinterpret_cast<const char*>(it.key()));
  } while (index.next(it));
}

// Erase key.
index.erase("test1", 5, comp);
```
