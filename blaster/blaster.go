package blaster

// #include "blaster.h"
import "C"

import (
	"unsafe"
	// "math/big"

	// "github.com/openrelayxyz/cardinal-types"

	log "github.com/inconshreveable/log15"
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
	Difficulty int64
	GasLimit uint64
	GasUsed  uint64
	Time uint64
	Extra
	MixDigest
	Nonce
	Uncles
	Size
	Td
	BaseFee
	// 		    uncleHash   varchar(32),
	// 		    coinbase    varchar(20),
	// 		    root        varchar(32),
	// 		    txRoot      varchar(32),
	// 		    receiptRoot varchar(32),
	// 		    bloom       blob, 0
	// 		    difficulty  varchar(32), 0
	// 		    gasLimit    BIGINT, 0
	// 		    gasUsed     BIGINT, 0
	// 		    time        BIGINT, 0
	// 		    extra       blob,
	// 		    mixDigest   varchar(32),
	// 		    nonce       BIGINT,
	// 		    uncles      blob,
	// 		    size        BIGINT,
	// 		    td          varchar(32), 0
	// 		    baseFee varchar(32))
}

// pb.Number,
// pb.Hash,
// pb.ParentHash,
// header.UncleHash,
// header.Coinbase,
// header.Root,
// header.TxHash,
// header.ReceiptHash,
// compress(header.Bloom[:]),
// header.Difficulty.Int64(),
// header.GasLimit,
// header.GasUsed,
// header.Time,
// header.Extra,
// header.MixDigest,
// int64(binary.BigEndian.Uint64(header.Nonce[:])),
// uncleRLP,
// size,
// td.Bytes(),
// header.BaseFee,
// header.WithdrawalsHash,

// difficulty=int64 gaslimit=uint64 gasused=uint64 time=uint64

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
// 		    bloom       blob, 0
// 		    difficulty  varchar(32), 0
// 		    gasLimit    BIGINT, 0
// 		    gasUsed     BIGINT, 0
// 		    time        BIGINT, 0
// 		    extra       blob,
// 		    mixDigest   varchar(32),
// 		    nonce       BIGINT,
// 		    uncles      blob,
// 		    size        BIGINT,
// 		    td          varchar(32), 0
// 		    baseFee varchar(32))

type sliceHeader struct {
	p   unsafe.Pointer
	len int
	cap int
}

func (b *Blaster) Put(bck BlastBlock) {
	// hPtr := (*C.char)(unsafe.Pointer(&bck.Hash[0]))
	// cval := (C.CBytes(bck.Hash[:32]))
	// log.Error("hash", "pre c hash", bck.Hash, "len", len(bck.Hash), "cval", cval)
	hPtr := (*C.char)(C.CBytes(bck.Hash[:32]))
	blInt := (C.size_t)(len(bck.Bloom))
	// phPtr := (*C.char)(unsafe.Pointer(&bck.ParentHash[0]))
	cPtr := (*C.char)(unsafe.Pointer(&bck.Coinbase[0]))
	nInt := C.longlong(bck.Number)
	bPtr := (*C.char)(unsafe.Pointer(&bck.Bloom[0]))
	tInt := C.longlong(bck.Time)
	dInt := C.longlong(bck.Difficulty)
	glInt := C.longlong(bck.GasLimit)
	guInt := C.longlong(bck.GasUsed)
	

	log.Error("inside of put", "number", nInt)

	C.sqib_put_block(
		b.DB, 
		blInt,
		hPtr, 
		// phPtr,
		cPtr, 
		nInt, 
		bPtr,
		tInt, 
		dInt, 
		glInt,
		guInt,
	)
	log.Error("just past the squib put block function")
}
 
func (b *Blaster) Close() {
	defer log.Error("close called on blaster")
	C.sqib_close(b.DB)
}