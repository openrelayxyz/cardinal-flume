#include "blaster.h"
#include <cstring>  
#include "../sqlite_blaster/src/util.h"
#include "../sqlite_blaster/src/sqlite_index_blaster.h"
#include <iostream>

void* new_sqlite_block_blaster(const char *fname) {
    sqlite_index_blaster* sqbb = new sqlite_index_blaster(
        20, // Column count 
        1, // PK size
        "number, hash, parentHash, uncleHash, coinbase, root, txRoot, receiptRoot, bloom, difficulty, gasLimit, gasUsed, time, extra, mixDigest, nonce, uncles, size, td, baseFee",  // Column names
        "blocks", // Table name
        4096, // Page size
        40000, //Cache size
        fname
    );
    return (void*)sqbb;
}

const uint8_t block_col_types[] = {SQLT_TYPE_INT64, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_INT64, SQLT_TYPE_INT64, SQLT_TYPE_INT64, SQLT_TYPE_INT64, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_INT64, SQLT_TYPE_TEXT, SQLT_TYPE_INT64, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT};


void sqib_put_block(void* sqibv, long long number, char* hash, char* parentHash, char* uncleHash, char* coinbase, char* root, char* txRoot, char* receiptRoot, char* bloom, size_t bloomLength, long long difficulty, long long gasLimit, long long gasUsed, long long time, char* extra, size_t extraLength, char* mixDigest, long long nonce, char* uncles, size_t unclesLength, long long size, char* td, char* baseFee) {

    sqlite_index_blaster* sqbb;
    int rec_len;
    sqbb = (sqlite_index_blaster*)sqibv;
    const void *rec_values[] = {&number, hash, parentHash, uncleHash, coinbase, root, txRoot, receiptRoot, bloom, &difficulty, &gasLimit, &gasUsed, &time, extra, mixDigest, &nonce, uncles, &size, td, baseFee};
    const size_t value_lens[] = {8, 32, 32, 32, 20, 32, 32, 32, bloomLength, 8, 8, 8, 8, extraLength, 32, 8, unclesLength, 8, 32, 32};
    size_t buf_size = 0;
    for(int i = 0; i < sizeof(value_lens) / sizeof(value_lens[0]); i++) {
        buf_size += value_lens[i];
    } 
    uint8_t rec_buf[buf_size];
    rec_len = sqbb->make_new_rec(rec_buf, 20, rec_values, value_lens, block_col_types);
    sqbb->put(rec_buf, -rec_len, NULL, 0);
    std::cout << "Inside of cpp after put block" << std::endl;
}

void sqbb_close(void* sqibv) {
    sqlite_index_blaster* sqbb;
    sqbb = (sqlite_index_blaster*)(sqibv);
    sqbb->close();
    std::cout << "sqib close block function" << std::endl;
    free(sqibv);
}

//TODO: move hash to beginning of the following list

void* new_sqlite_tx_blaster(const char *fname) {
    sqlite_index_blaster* sqtb = new sqlite_index_blaster(
        // 23, // Column count 
        // 1, // PK size
        // "hash, block, gas, gasPrice, input, nonce, recipient, transactionIndex, value, v, r, s, sender, func, contractAddress, cumulativeGasUsed, gasUsed, logsBloom, status, type, access_list, gasFeeCap, gasTipCap",  // Column names
        // "transactions", // Table name
        2, // Column count 
        1, // PK size
        "hash, block",  // Column names
        "transactions", // Table name
        4096, // Page size
        40000, //Cache size
        fname
    );
    return (void*)sqtb;
}

void sqtb_close(void* sqibv) {
    sqlite_index_blaster* sqtb;
    sqtb = (sqlite_index_blaster*)(sqibv);
    sqtb->close();
    std::cout << "sqib close tx function" << std::endl;
    free(sqibv);
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

// const uint8_t tx_col_types[] = {SQLT_TYPE_TEXT, SQLT_TYPE_INT64, SQLT_TYPE_INT64, SQLT_TYPE_INT64, SQLT_TYPE_INT64, SQLT_TYPE_INT64, SQLT_TYPE_TEXT, SQLT_TYPE_INT64, SQLT_TYPE_TEXT, SQLT_TYPE_INT64, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_INT64, SQLT_TYPE_INT64, SQLT_TYPE_TEXT, SQLT_TYPE_INT64, SQLT_TYPE_INT64, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT};
const uint8_t tx_col_types[] = {SQLT_TYPE_TEXT, SQLT_TYPE_INT64, SQLT_TYPE_INT64, SQLT_TYPE_INT64, SQLT_TYPE_TEXT};



void sqib_put_tx(void* sqibv, char* hash, long long block, long long gas, long long gasPrice, char* input, size_t inputLength, long long nonce, char* recipient, long long transactionIndex, 
char* value, size_t valueLength, long long v, char* r, char* s, char* sender, char* func, char* contractAddress, size_t contractAddressLength, long long cumulativeGasUsed, 
long long gasUsed, char* logsBloom, size_t logsBloomLength, long long status, long long type, char* accessList, size_t accessListLength, 
char* gasFeeCap, size_t gasFeeCapLength, char* gasTipCap, size_t gasTipCapLength) {

    std::cout << "Inside of cpp before put tx" << std::endl;

    sqlite_index_blaster* sqtb;
    std::cout << "init blaster" << std::endl;
    int rec_len;
    std::cout << "rec len" << std::endl;
    sqtb = (sqlite_index_blaster*)sqibv;
    std::cout << "define sqtb" << std::endl;
    // const void *rec_values[] = {hash, &block, &gas, &gasPrice, input, &nonce, recipient, &transactionIndex, value, &v, r, s, sender, func, contractAddress, &cumulativeGasUsed, &gasUsed, logsBloom, &status, &type, accessList, gasFeeCap, gasTipCap};
    const void *rec_values[] = {hash, &block, &gas, &gasPrice};
    std::cout << "rec vals" << std::endl;
    // const size_t value_lens[] = {32, 8, 8, 8, inputLength, 8, 20, 8, valueLength, 8, 32, 32, 20, 4, contractAddressLength, 8, 8, logsBloomLength, 8, 8, accessListLength, gasFeeCapLength, gasTipCapLength};
    const size_t value_lens[] = {32, 8, 8, 8};
    std::cout << "val len" << std::endl;
    size_t buf_size = 0;
    std::cout << "buf size" << std::endl;
    for(int i = 0; i < sizeof(value_lens) / sizeof(value_lens[0]); i++) {
        buf_size += value_lens[i];
    } 
    std::cout << "post loop" << std::endl;
    uint8_t rec_buf[buf_size];
    std::cout << "pass buff size" << std::endl;
    rec_len = sqtb->make_new_rec(rec_buf, 4, rec_values, value_lens, tx_col_types);
    std::cout << "set rec len" << std::endl;
    sqtb->put(rec_buf, -rec_len, NULL, 0);
    std::cout << "Inside of cpp after put tx" << std::endl;

}

//TODO move block and log index to the top and make the pk size 2

void* new_sqlite_log_blaster(const char *fname) {
    sqlite_index_blaster* sqlb = new sqlite_index_blaster(
        11, // Column count 
        2, // PK size
        "block, logIndex, address, topic0, topic1, topic2, topic3, data, transactionHash, transactionIndex, blockHash",  // Column names
        "event_logs", // Table name
        4096, // Page size
        40000, //Cache size
        fname
    );
    return (void*)sqlb;
}

const uint8_t log_col_types[] = {SQLT_TYPE_INT64, SQLT_TYPE_INT64, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT};

void sqib_put_log(void* sqibv, long long block, long long logIndex, char* address, char* topic0, char* topic1, char* topic2, char* topic3, char* data, size_t dataLength, char* transactionHash, char* transactionIndex, char* blockHash) {

    std::cout << "Inside of cpp before put log" << std::endl;

    sqlite_index_blaster* sqlb;
    int rec_len;
    sqlb = (sqlite_index_blaster*)sqibv;
    const void *rec_values[] = {&block, &logIndex, address, topic0, topic1, topic2, topic3, data, transactionHash, transactionIndex, blockHash};
    const size_t value_lens[] = {8, 8, 20, 32, 32, 32, 32, dataLength, 32, 32, 32};
    size_t buf_size = 0;
    for(int i = 0; i < sizeof(value_lens) / sizeof(value_lens[0]); i++) {
        buf_size += value_lens[i];
    } 
    uint8_t rec_buf[buf_size];
    rec_len = sqlb->make_new_rec(rec_buf, 11, rec_values, value_lens, log_col_types);
    sqlb->put(rec_buf, -rec_len, NULL, 0);
    std::cout << "Inside of cpp after put log" << std::endl;
}

void sqlb_close(void* sqibv) {
    sqlite_index_blaster* sqib;
    sqib = (sqlite_index_blaster*)(sqibv);
    sqib->close();
    std::cout << "sqib close log function" << std::endl;
    free(sqibv);
}
