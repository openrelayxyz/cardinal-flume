package blaster

// #include "blaster.h"
import "C"

import (
	"unsafe"
	"math/big"

	log "github.com/inconshreveable/log15"
)

type BlastBlock struct {
	Hash [32]byte
	ParentHash [32]byte
	Coinbase [20]byte
	Number uint64
	Time *big.Int
	Bloom []byte
}

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

// number      BIGINT PRIMARY KEY,
// 		    parentHash  varchar(32),
// 		    uncleHash   varchar(32),
// 		    coinbase    varchar(20),
// 		    root        varchar(32),
// 		    txRoot      varchar(32),
// 		    receiptRoot varchar(32),
// 		    bloom       blob,
// 		    difficulty  varchar(32),
// 		    gasLimit    BIGINT,
// 		    gasUsed     BIGINT,
// 		    time        BIGINT,
// 		    extra       blob,
// 		    mixDigest   varchar(32),
// 		    nonce       BIGINT,
// 		    uncles      blob,
// 		    size        BIGINT,
// 		    td          varchar(32),
// 		    baseFee varchar(32))

func (b *Blaster) Put(bck BlastBlock) {
	hPtr := (*C.char)(unsafe.Pointer(&bck.Hash[0]))
	phPtr := (*C.char)(unsafe.Pointer(&bck.ParentHash[0]))
	cPtr := (*C.char)(unsafe.Pointer(&bck.Coinbase[0]))
	bPtr := (*C.char)(unsafe.Pointer(&bck.Bloom[0]))
	nInt := C.longlong(bck.Number)
	tInt := C.longlong(bck.Time.Int64())

	log.Error("inside of put", "number", nInt)

	C.sqib_put_block(
		b.DB, 
		hPtr, 
		phPtr,
		cPtr, 
		nInt, 
		tInt, 
		bPtr,
	)
	log.Error("just past the squib put block function")
}
 
func (b *Blaster) Close() {
	defer log.Error("close called on blaster")
	C.sqib_close(b.DB)
}