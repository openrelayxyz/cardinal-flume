#include "blaster.h"
#include <cstring>  
#include "../sqlite_blaster/src/util.h"
#include "../sqlite_blaster/src/sqlite_index_blaster.h"
#include <iostream>

void* new_sqlite_index_blaster(const char *fname) {
    sqlite_index_blaster* sqib = new sqlite_index_blaster(
        8, // Column count 
        1, // PK size
        "hash, coinbase, number, bloom, time, difficulty, gasLimit, gasUsed",  // Column names
        "blocks", // Table name
        4096, // Page size
        40000, //Cache size
        fname
    );
    return (void*)sqib;
}

const uint8_t col_types[] = {SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_INT64, SQLT_TYPE_TEXT, SQLT_TYPE_INT64, SQLT_TYPE_INT64, SQLT_TYPE_INT64, SQLT_TYPE_INT64};


void sqib_put_block(void* sqibv, size_t bloomLength, char* hash, char* coinbase, long long number, char* bloom, long long time, long long difficulty, long long gasLimit, long long gasUsed) {
    sqlite_index_blaster* sqib;
    int rec_len;
    uint8_t rec_buf[500];
    sqib = (sqlite_index_blaster*)sqibv;
    const void *rec_values[] = {hash, coinbase, &number, bloom, &time, &difficulty, &gasLimit, &gasUsed};
    const size_t value_lens[] = {32, 20, 8, bloomLength, 8, 8, 8, 8};
    rec_len = sqib->make_new_rec(rec_buf, 8, rec_values, value_lens, col_types);
    sqib->put(rec_buf, -rec_len, NULL, 0);
    // std::cout << "Inside of cpp after put" << std::endl;
}

void sqib_close(void* sqibv) {
    sqlite_index_blaster* sqib;
    sqib = (sqlite_index_blaster*)(sqibv);
    sqib->close();
    std::cout << "sqib close function" << std::endl;
    free(sqibv);
}
