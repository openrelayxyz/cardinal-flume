#include "blaster.h"
#include <cstring>  
#include "../sqlite_blaster/src/util.h"
#include "../sqlite_blaster/src/sqlite_index_blaster.h"
#include <iostream>

void* new_sqlite_block_blaster(const char *fname) {
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

const uint8_t block_col_types[] = {SQLT_TYPE_INT64, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_INT64, SQLT_TYPE_INT64, SQLT_TYPE_INT64, SQLT_TYPE_INT64, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_INT64, SQLT_TYPE_TEXT, SQLT_TYPE_INT64, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT};


void sqib_put_block(void* sqibv, long long number, char* hash, char* parentHash, char* uncleHash, char* coinbase, char* root, char* txRoot, char* receiptRoot, char* bloom, size_t bloomLength, long long difficulty, long long gasLimit, long long gasUsed, long long time, char* extra, size_t extraLength, char* mixDigest, long long nonce, char* uncles, size_t unclesLength, long long size, char* td, char* baseFee) {

    sqlite_index_blaster* sqib;
    int rec_len;
    sqib = (sqlite_index_blaster*)sqibv;
    const void *rec_values[] = {&number, hash, parentHash, uncleHash, coinbase, root, txRoot, receiptRoot, bloom, &difficulty, &gasLimit, &gasUsed, &time, extra, mixDigest, &nonce, uncles, &size, td, baseFee};
    const size_t value_lens[] = {8, 32, 32, 32, 20, 32, 32, 32, bloomLength, 8, 8, 8, 8, extraLength, 32, 8, unclesLength, 8, 32, 32};
    size_t buf_size = 0;
    for(int i = 0; i < sizeof(value_lens) / sizeof(value_lens[0]); i++) {
        buf_size += value_lens[i];
    } 
    uint8_t rec_buf[buf_size];
    rec_len = sqib->make_new_rec(rec_buf, 20, rec_values, value_lens, block_col_types);
    sqib->put(rec_buf, -rec_len, NULL, 0);
    std::cout << "Inside of cpp after put block" << std::endl;
}

void* new_sqlite_tx_blaster(const char *fname) {
    sqlite_index_blaster* sqib = new sqlite_index_blaster(
        23, // Column count 
        1, // PK size
        "gas, gasPrice, hash, input, nonce, recipient, transactionIndex, value, v, r, s, sender, func, contractAddress, cumulativeGasUsed, gasUsed, loagsBloom, status, block, type access_list, gasFeeCap, gasTipCap",  // Column names
        "transactions", // Table name
        4096, // Page size
        40000, //Cache size
        fname
    );
    return (void*)sqib;
}

// type blastTx struct {
// 	// Id uint64
// 	Gas uint64
// 	GasPrice uint64
// 	Hash [32]byte
// 	Input []byte
// 	Nonce uint64
// 	Recipient [20]byte
// 	TransactionIndex uint64
// 	Value []byte
// 	V uint64
// 	R [32]byte
// 	S [32]byte
// 	Sender [20]byte
// 	Func [4]byte
// 	ContractAddress []byte
// 	CumulativeGasUsed uint64
// 	GasUsed uint64
// 	LogsBloom []byte
// 	Status uint64
// 	Block uint64
// 	Type uint64
// 	Access_list []byte
// 	GasFeeCap []byte
//     GasTipCap []byte
// }

const uint8_t tx_col_types[] = {SQLT_TYPE_INT64, SQLT_TYPE_INT64, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_INT64, SQLT_TYPE_TEXT, SQLT_TYPE_INT64, SQLT_TYPE_TEXT, SQLT_TYPE_INT64, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_INT64, SQLT_TYPE_INT64, SQLT_TYPE_TEXT, SQLT_TYPE_INT64, SQLT_TYPE_INT64, SQLT_TYPE_INT64, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT};

void sqib_put_tx(void* sqibv, long long gas, long long gasPrice, char* hash, char* input, size_t inputLength, long long nonce, char* recipient, long long transactionIndex, 
char* value, size_t valueLength, long long v, char* r, char* s, char* sender, char* func, char* contractAddress, size_t contractAddressLength, long long cumulativeGasUsed, 
long long gasUsed, char* logsBloom, size_t logsBloomLength, long long status, long long block, long long type, char* accessList, size_t accessListLength, 
char* gasFeeCap, size_t gasFeeCapLength, char* gasTipCap, size_t gasTipCapLength) {

    sqlite_index_blaster* sqib;
    int rec_len;
    sqib = (sqlite_index_blaster*)sqibv;
    const void *rec_values[] = {&gas, &gasPrice, hash, input, nonce, recipient, &transactionIndex, value, &v, r, s, sender, func, contractAddress, &cumulativeGasUsed, 
    &gasUsed, logsBloom, &status, &block, &type, accessList, gasFeeCap, gasTipCap};
    const size_t value_lens[] = {8, 8, 32, inputLength, 8, 20, 8, valueLength, 8, 32, 32, 20, 4, contractAddressLength, 8, logsBloomLength, 8, 8, 8, accessListLength, gasFeeCapLength, gasTipCapLength};
    size_t buf_size = 0;
    for(int i = 0; i < sizeof(value_lens) / sizeof(value_lens[0]); i++) {
        buf_size += value_lens[i];
    } 
    uint8_t rec_buf[buf_size];
    rec_len = sqib->make_new_rec(rec_buf, 23, rec_values, value_lens, tx_col_types);
    sqib->put(rec_buf, -rec_len, NULL, 0);
    std::cout << "Inside of cpp after put tx" << std::endl;

}

void* new_sqlite_log_blaster(const char *fname) {
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


void sqib_close(void* sqibv) {
    sqlite_index_blaster* sqib;
    sqib = (sqlite_index_blaster*)(sqibv);
    sqib->close();
    std::cout << "sqib close function" << std::endl;
    free(sqibv);
}
