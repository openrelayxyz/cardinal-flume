#include "blaster.h"
#include <cstring>  
#include "../sqlite_blaster/src/util.h"
#include "../sqlite_blaster/src/sqlite_index_blaster.h"
#include <iostream>

void* new_sqlite_index_blaster(const char *fname) {
    sqlite_index_blaster* sqib = new sqlite_index_blaster(
        20, // Column count 
        1, // PK size
        "number, hash, parentHash, uncleHash, coinbase, root, txRoot, receiptRoot, bloom, difficulty, gasLimit, gasUsed, time, extra, mixDigest, nonce, uncles, size, td, baseFee",  // Column names
        "blocks", // Table name
        4096, // Page size
        40000, //Cache size
        fname
    );
    return (void*)sqib;
}

const uint8_t col_types[] = {SQLT_TYPE_INT64, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_INT64, SQLT_TYPE_INT64, SQLT_TYPE_INT64, SQLT_TYPE_INT64, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_INT64, SQLT_TYPE_TEXT, SQLT_TYPE_INT64, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT};


void sqib_put_block(void* sqibv, long long number, char* hash, char* parentHash, char* uncleHash, char* coinbase, char* root, char* txRoot, char* receiptRoot, char* bloom, size_t bloomLength, long long difficulty, long long gasLimit, long long gasUsed, long long time, char* extra, size_t extraLength, char* mixDigest, long long nonce, char* uncles, size_t unclesLength, long long size, char* td, char* baseFee) {

    sqlite_index_blaster* sqib;
    int rec_len;
    uint8_t rec_buf[500];
    sqib = (sqlite_index_blaster*)sqibv;
    const void *rec_values[] = {&number, hash, parentHash, uncleHash, coinbase, root, txRoot, receiptRoot, bloom, &difficulty, &gasLimit, &gasUsed, &time, extra, mixDigest, &nonce, uncles, &size, td, baseFee};
    const size_t value_lens[] = {8, 32, 32, 32, 20, 32, 32, 32, bloomLength, 8, 8, 8, 8, extraLength, 32, 8, unclesLength, 8, 32, 32};
    rec_len = sqib->make_new_rec(rec_buf, 20, rec_values, value_lens, col_types);
    sqib->put(rec_buf, -rec_len, NULL, 0);
    std::cout << "Inside of cpp after put" << std::endl;
}

void sqib_close(void* sqibv) {
    sqlite_index_blaster* sqib;
    sqib = (sqlite_index_blaster*)(sqibv);
    sqib->close();
    std::cout << "sqib close function" << std::endl;
    free(sqibv);
}
