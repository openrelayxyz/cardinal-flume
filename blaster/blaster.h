#include <stddef.h> 
#ifndef BLASTER_H
#define BLASTER_H

#ifdef __cplusplus
extern "C" {
#endif

void* new_sqlite_index_blaster(const char *fname);
void sqib_put_block(void* sqibv, size_t bloomLength, char* hash, char* coinbase, long long number, char* bloom, long long time, long long difficulty, long long gasLimit, long long gasUsed);
void sqib_close(void* sqibv);


#ifdef __cplusplus
}
#endif

#endif // BLASTER_H