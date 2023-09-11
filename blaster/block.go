package blaster
// #cgo CXXFLAGS: -std=c++11
// #cgo LDFLAGS: -lstdc++
// #include <stdlib.h>
// #include "blaster.h"
import "C"

import (
	"unsafe"
)

type BlastBlock struct {
	Number uint64
	Hash [32]byte
	ParentHash [32]byte
	UncleHash [32]byte
	Coinbase [20]byte
	Root [32]byte
	TxRoot [32]byte
	ReceiptRoot [32]byte
	Bloom []byte
	Difficulty uint64
	GasLimit uint64
	GasUsed  uint64
	Time uint64
	Extra []byte
	MixDigest [32]byte
	Nonce uint64
	Uncles []byte
	Size uint64
	Td [32]byte
	BaseFee [32]byte
	WithdrawalHash [32]byte
}

func (b *BlockBlaster) PutBlock(bck BlastBlock) {

	var bPtr *C.char
	var exPtr *C.char
	var unPtr *C.char
	var wthdHashPtr *C.char

	nInt := C.longlong(bck.Number)
	hPtr := (*C.char)(C.CBytes(bck.Hash[:32]))
	phPtr := (*C.char)(C.CBytes(bck.ParentHash[:32]))
	uhPtr := (*C.char)(C.CBytes(bck.UncleHash[:32]))
	cbPtr := (*C.char)(C.CBytes(bck.Coinbase[:20]))
	rPtr := (*C.char)(C.CBytes(bck.Root[:32]))
	trPtr := (*C.char)(C.CBytes(bck.TxRoot[:32]))
	rrPtr := (*C.char)(C.CBytes(bck.ReceiptRoot[:32]))
	blLen := (C.size_t)(len(bck.Bloom))
	if blLen > 0 {
		bPtr = (*C.char)(C.CBytes(bck.Bloom[:blLen]))
	}	
	dInt := C.longlong(bck.Difficulty)
	glInt := C.longlong(bck.GasLimit)
	guInt := C.longlong(bck.GasUsed)
	tInt := C.longlong(bck.Time)
	exLen := (C.size_t)(len(bck.Extra))
	if exLen > 0 { 
		exPtr = (*C.char)(C.CBytes(bck.Extra[:exLen]))
	} 
	mxPtr := (*C.char)(C.CBytes(bck.MixDigest[:32]))
	ncInt := C.longlong(bck.Nonce)
	unLen := (C.size_t)(len(bck.Uncles))
	if unLen > 0 {
		unPtr = (*C.char)(C.CBytes(bck.Uncles[:unLen]))
	}
	sInt := C.longlong(bck.Size)
	tdPtr := (*C.char)(C.CBytes(bck.Td[:32]))
	bfPtr := (*C.char)(C.CBytes(bck.BaseFee[:32]))
	wthdHashPtr = (*C.char)(C.CBytes(bck.WithdrawalHash[:32]))

	b.Lock.Lock()

	C.sqib_put_block(
		b.DB, 
		nInt,
		hPtr,
		phPtr,
		uhPtr,
		cbPtr,
		rPtr,
		trPtr,
		rrPtr,
		bPtr,
		blLen,
		dInt,
		glInt, 
		guInt,
		tInt, 
		exPtr,
		exLen,
		mxPtr,
		ncInt,
		unPtr,
		unLen,
		sInt, 
		tdPtr,
		bfPtr,
		wthdHashPtr,
	)

	defer C.free(unsafe.Pointer(hPtr))
	defer C.free(unsafe.Pointer(hPtr))
	defer C.free(unsafe.Pointer(phPtr))
	defer C.free(unsafe.Pointer(uhPtr))
	defer C.free(unsafe.Pointer(cbPtr))
	defer C.free(unsafe.Pointer(rPtr))
	defer C.free(unsafe.Pointer(trPtr))
	defer C.free(unsafe.Pointer(rrPtr))
	defer C.free(unsafe.Pointer(bPtr))
	defer C.free(unsafe.Pointer(exPtr))
	defer C.free(unsafe.Pointer(mxPtr))
	defer C.free(unsafe.Pointer(unPtr))
	defer C.free(unsafe.Pointer(tdPtr))
	defer C.free(unsafe.Pointer(bfPtr))
	defer C.free(unsafe.Pointer(wthdHashPtr))

	b.Lock.Unlock()
}



type BlastWithdrawal struct {
	Block uint64
	WithdrawalIndex uint64
	ValidatorIndex uint64
	Address [20]byte
	Amount uint64
	BlockHash [32]byte
}

func (b *WithdrawalBlaster) PutWithdrawal(wd BlastWithdrawal) {
	
	blockInt := C.longlong(wd.Block)
	wDexInt := C.longlong(wd.WithdrawalIndex)
	vDexInt := C.longlong(wd.ValidatorIndex)
	addPtr := (*C.char)(C.CBytes(wd.Address[:20]))
	amountInt := C.longlong(wd.Amount)
	bHashPtr := (*C.char)(C.CBytes(wd.BlockHash[:32]))

	b.Lock.Lock()

	C.sqib_put_withdrawal(
		b.DB, 
		blockInt,
		wDexInt,
		vDexInt,
		addPtr,
		amountInt,
		bHashPtr,
	)

	defer C.free(unsafe.Pointer(addPtr))
	defer C.free(unsafe.Pointer(bHashPtr))
	
	b.Lock.Unlock()
}