package blaster

// #include "blaster.h"
import "C"

import (
	"unsafe"
	// "math/big"

	// "github.com/openrelayxyz/cardinal-types"

	log "github.com/inconshreveable/log15"
)

type Blaster struct {
	DB unsafe.Pointer
}

func NewBlasterIndexer(dataBase string) *Blaster {
	db := C.new_sqlite_index_blaster(C.CString(dataBase))

	b := &Blaster{
		DB: db,
	}
	return b
}

// need to implement a put method for blocks, logs, and transactions. 

type sliceHeader struct {
	p   unsafe.Pointer
	len int
	cap int
}

func (b *Blaster) PutBlock(bck BlastBlock) {

	var bPtr *C.char
	var exPtr *C.char
	var unPtr *C.char
	
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

	log.Error("inside of put", "number", nInt)

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
	)
	log.Error("just past the squib put block function")
}
 
func (b *Blaster) Close() {
	defer log.Error("close called on blaster")
	C.sqib_close(b.DB)
}