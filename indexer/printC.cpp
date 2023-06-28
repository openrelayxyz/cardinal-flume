#include "printC.h"
#include <stdio.h>

void myFuncC(char* hash, char* coinbase, long long number, long long time, char* bloom) {
    printf("hash: %s\n", hash);
    printf("coinbase: %s\n", coinbase);
    printf("number: %lld\n", number);
    printf("time: %lld\n", time);
    printf("bloom: %s\n", bloom);
}