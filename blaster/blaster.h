#include <stddef.h> 
#ifndef BLASTER_H
#define BLASTER_H

#ifdef __cplusplus
extern "C" {
#endif

void* new_sqlite_block_blaster(const char *fname);

void* new_sqlite_withdrawal_blaster(const char *fname);

void* new_sqlite_tx_blaster(const char *fname);

void* new_sqlite_log_blaster(const char *fname);

void sqib_put_block(void* sqibv, long long number, char* hash, char* parentHash, char* uncleHash, char* coinbase, char* root, char* txRoot, char* receiptRoot, char* bloom, size_t bloomLength, long long difficulty, long long gasLimit, long long gasUsed, long long time, char* extra, size_t extraLength, char* mixDigest, long long nonce, char* uncles, size_t unclesLength, long long size, char* td, char* baseFee, char* withdrawalHash);


void sqib_put_withdrawal(void* sqibv, long long block, long long wthdrlIndex, long long vldtrIndex, char* address, long long amount, char* blockHash);

void sqib_put_tx(void* sqibv, char* hash, long long block, long long gas, long long gasPrice, char* input, size_t inputLength, long long nonce, char* recipient, size_t recipientLength, long long transactionIndex, 
char* value, size_t valueLength, long long v, char* r, char* s, char* sender, char* func, char* contractAddress, size_t contractAddressLength, long long cumulativeGasUsed, 
long long gasUsed, char* logsBloom, size_t logsBloomLength, long long status, long long type, char* accessList, size_t accessListLength, 
char* gasFeeCap, size_t gasFeeCapLength, char* gasTipCap, size_t gasTipCapLength);

void sqib_put_log(void* sqibv, long long block, long long logIndex, char* address, char* topic0, char* topic1, char* topic2, char* topic3, char* data, size_t dataLength, char* transactionHash, char* transacitonIndex, char* blockHash);

void sqbb_close(void* sqibv);
void sqwb_close(void* sqibv);
void sqtb_close(void* sqibv);
void sqlb_close(void* sqibv);


#ifdef __cplusplus
}
#endif

#endif // BLASTER_H