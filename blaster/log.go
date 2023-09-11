package blaster
// #include <stdlib.h>
// #include "blaster.h"
import "C"

import (
	"unsafe"
)

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

	blockInt := C.longlong(lg.Block)
	logDexInt := C.longlong(lg.LogIndex)
	addressPtr = (*C.char)(C.CBytes(lg.Address[:20]))
	topic0Ptr = (*C.char)(C.CBytes(lg.Topic0[:32]))
	topic1Ptr = (*C.char)(C.CBytes(lg.Topic1[:32]))
	topic2Ptr = (*C.char)(C.CBytes(lg.Topic2[:32]))
	topic3Ptr = (*C.char)(C.CBytes(lg.Topic3[:32]))
	dataLen := (C.size_t)(len(lg.Data))
	if dataLen > 25000 { // This value may need to be adjusted
		dataPtr = (*C.char)(C.CBytes([]byte{}))
		dataLen = 0
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
		topic1Ptr,
		topic2Ptr,
		topic3Ptr,
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