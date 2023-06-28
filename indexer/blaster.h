#ifndef BLASTER_H
#define BLASTER_H

#ifdef __cplusplus
extern "C" {
#endif

void* new_sqlite_index_blaster(const char *fname);
void sqib_put_block(void* sqibv, char* hash, char* coinbase, long long number, long long time, char* bloom);
// void sqib_put_block(void* sqibv, long long number);
void sqib_close(void* sqibv);


#ifdef __cplusplus
}
#endif

#endif // BLASTER_H