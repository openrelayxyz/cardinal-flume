package blaster
// #include "blaster.h"
import "C"

import (
	log "github.com/inconshreveable/log15"
)


// address varchar(20),
// topic0 varchar(32),
// topic1 varchar(32),
// topic2 varchar(32),
// topic3 varchar(32),
// data blob,
// block BIGINT,
// logIndex MEDIUMINT,
// transactionHash varchar(32),
// transactionIndex varchar(32),
// blockHash varchar(32),
// PRIMARY KEY (block, logIndex)

type BlastLog struct {
	Address [20]byte
	Topic0 [32]byte
	Topic1 [32]byte
	Topic2 [32]byte
	Topic3 [32]byte
	Data []byte
	Block uint64
	LogIndex uint64
	TransactionHash [32]byte
	TransactionIndex [32]byte
	BlockHash [32]byte
 }

func (b *LogBlaster) PutLog(lg BlastLog) {
	var addressPtr *C.char
	var topic0Ptr *C.char
	var topic1Ptr *C.char
	var topic2Ptr *C.char
	var topic3Ptr *C.char
	var dataPtr *C.char
	var transHashPtr *C.char
	var transDexPtr *C.char
	var blockHashPtr *C.char

	addressPtr = (*C.char)(C.CBytes(lg.Address[:20]))
	topic0Ptr = (*C.char)(C.CBytes(lg.Topic0[:32]))
	topic1Ptr = (*C.char)(C.CBytes(lg.Topic1[:32]))
	topic2Ptr = (*C.char)(C.CBytes(lg.Topic2[:32]))
	topic3Ptr = (*C.char)(C.CBytes(lg.Topic3[:32]))
	dataLen := (C.size_t)(len(lg.Data))
	if dataLen > 0 {
		dataPtr = (*C.char)(C.CBytes(lg.Data[:dataLen]))
	}
	blockInt := C.longlong(lg.Block)
	logDexInt := C.longlong(lg.LogIndex)
	transHashPtr = (*C.char)(C.CBytes(lg.TransactionHash[:32]))
	transDexPtr = (*C.char)(C.CBytes(lg.TransactionIndex[:32]))
	blockHashPtr = (*C.char)(C.CBytes(lg.BlockHash[:32]))



	log.Error("inside of put log", "number", blockInt)

	C.sqib_put_log(
		b.DB, 
		addressPtr,
		topic0Ptr,
		topic1Ptr,
		topic2Ptr,
		topic3Ptr,
		dataPtr,
		dataLen,
		blockInt, 
		logDexInt,
		transHashPtr,
		transDexPtr,
		blockHashPtr,
	)
	log.Error("just past the squib put log function")
	

}