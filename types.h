#ifndef DB_TYPES_H
#define DB_TYPES_H

#include <stdint.h>

typedef uint16_t nodeoff_t;
typedef nodeoff_t keylen_t;

typedef int (*comparator_t)(const void* key1,
                            keylen_t keylen1,
                            const void* key2,
                            keylen_t keylen2);

#endif // DB_TYPES_H
