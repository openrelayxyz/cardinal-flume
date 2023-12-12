package blaster
// #include <stdlib.h>
// #include "blaster.h"
import "C"

import (
	"fmt"
	"unsafe"
)

type BlastLog struct {
	Address [20]byte
	Topic0 []byte
	Topic1 []byte
	Topic2 []byte
	Topic3 []byte
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

	blockInt := C.longlong(lg.Block)
	logDexInt := C.longlong(lg.LogIndex)
	addressPtr = (*C.char)(C.CBytes(lg.Address[:20]))
	topic0len := (C.size_t)(len(lg.Topic0))
	if topic0len > 0 {
		topic0Ptr = (*C.char)(C.CBytes(lg.Topic0[:topic0len]))
	}
	topic1len := (C.size_t)(len(lg.Topic1))
	if topic1len > 0 {
		topic1Ptr = (*C.char)(C.CBytes(lg.Topic1[:topic1len]))
	}
	topic2len := (C.size_t)(len(lg.Topic2))
	if topic2len > 0 {
		topic2Ptr = (*C.char)(C.CBytes(lg.Topic2[:topic2len]))
	}
	topic3len := (C.size_t)(len(lg.Topic3))
	if topic3len > 0 {
		topic3Ptr = (*C.char)(C.CBytes(lg.Topic3[:topic3len]))
	}
	dataLen := (C.size_t)(len(lg.Data))
	if dataLen > 20000 { // This value may need to be adjusted
		dataPtr = (*C.char)(C.CBytes([]byte{}))
		dataLen = 0
		b.appendToFile(lg.Block, lg.Data, lg.LogIndex)
	} else if dataLen > 0 {
		dataPtr = (*C.char)(C.CBytes(lg.Data[:dataLen]))
	}  
	transHashPtr = (*C.char)(C.CBytes(lg.TransactionHash[:32]))
	transDexPtr = (*C.char)(C.CBytes(lg.TransactionIndex[:32]))
	blockHashPtr = (*C.char)(C.CBytes(lg.BlockHash[:32]))

	b.Lock.Lock()

	C.sqib_put_log(
		b.DB, 
		blockInt, 
		logDexInt,
		addressPtr,
		topic0Ptr,
		topic0len,
		topic1Ptr,
		topic1len,
		topic2Ptr,
		topic2len,
		topic3Ptr,
		topic3len,
		dataPtr,
		dataLen,
		transHashPtr,
		transDexPtr,
		blockHashPtr,
	)

	defer C.free(unsafe.Pointer(addressPtr))
	defer C.free(unsafe.Pointer(topic0Ptr))
	defer C.free(unsafe.Pointer(topic1Ptr))
	defer C.free(unsafe.Pointer(topic2Ptr))
	defer C.free(unsafe.Pointer(topic3Ptr))
	defer C.free(unsafe.Pointer(dataPtr))
	defer C.free(unsafe.Pointer(transHashPtr))
	defer C.free(unsafe.Pointer(transDexPtr))
	defer C.free(unsafe.Pointer(blockHashPtr))

	b.Lock.Unlock()
}

func (b *LogBlaster) appendToFile(number uint64, data []byte, logIndex uint64) {

	statement := fmt.Sprintf("UPDATE event_logs SET data = X'%x' WHERE block = %v AND logIndex = %v;", data, number, logIndex)

	_, err := b.MIFile.Write([]byte(statement + "\n"))
	if err != nil {
		fmt.Println("Error writing to file:", err)
	}
}