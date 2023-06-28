#include "blaster.h"
#include <cstring>  
#include "../sqlite_blaster/src/util.h"
#include "../sqlite_blaster/src/sqlite_index_blaster.h"
#include <iostream>

void* new_sqlite_index_blaster(const char *fname) {
    sqlite_index_blaster* sqib = new sqlite_index_blaster(
        5, // Column count 
        1, // PK size
        "hash, coinbase, number, time, bloom", // Column names
        "blocks", // Table name
        4096, // Page size
        40000, //Cache size
        fname
    );
    return (void*)sqib;
}

const uint8_t col_types[] = {SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_INT64, SQLT_TYPE_INT64, SQLT_TYPE_TEXT};


void sqib_put_block(void* sqibv, char* hash, char* coinbase, long long number, long long time, char* bloom) {
    sqlite_index_blaster* sqib;
    int rec_len;
    uint8_t rec_buf[500];
    sqib = (sqlite_index_blaster*)sqibv;
    const void *rec_values[] = {hash, coinbase, &number, &time, bloom};
    rec_len = sqib->make_new_rec(rec_buf, 5, rec_values, NULL, col_types);
    std::cout << "Inside of cpp before put" << std::endl;
    sqib->put(rec_buf, -rec_len, NULL, 0);
    std::cout << "Inside of cpp after put" << std::endl;
    // sqib->close();
    // free(sqibv);
}

// void sqib_put_block(void* sqibv, long long number) {
//     sqlite_index_blaster* sqib;
//     int rec_len;
//     uint8_t rec_buf[500];
//     sqib = (sqlite_index_blaster*)sqibv;
//     const void *rec_values[] = {&number};
//     rec_len = sqib->make_new_rec(rec_buf, 5, rec_values, NULL, col_types);
//     sqib->put(rec_buf, -rec_len, NULL, 0);
// }

void sqib_close(void* sqibv) {
    sqlite_index_blaster* sqib;
    sqib = (sqlite_index_blaster*)(sqibv);
    sqib->close();
    std::cout << "sqib close function" << std::endl;
    free(sqibv);
}
