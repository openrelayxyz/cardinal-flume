package blaster

// #include "blaster.h"
import "C"

import (
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
	WithdrawalsHash [32]byte
}

// number=int64 hash=types.Hash parent hash=types.Hash unclehash=types.Hash
// coinbase=common.Address root=types.Hash tx root=types.Hash receiptRoot=types.Hash
// bloom=[]uint8 difficulty=int64 gaslimit=uint64 gasused=uint64
// time=uint64 extra=[]uint8 mixed=types.Hash nonce=int64
// uncleRLP=[]uint8 size=int td="func() []uint8" basefee=*big.Int whash=*types.Hash

// number      BIGINT PRIMARY KEY,
// hash        varchar(32) UNIQUE,
// parentHash  varchar(32),
// uncleHash   varchar(32),
// coinbase    varchar(20),
// root        varchar(32),
// txRoot      varchar(32),
// receiptRoot varchar(32),
// bloom       blob,
// difficulty  varchar(32),
// gasLimit    BIGINT,
// gasUsed     BIGINT,
// time        BIGINT,
// extra       blob,
// mixDigest   varchar(32),
// nonce       BIGINT,
// uncles      blob,
// size        BIGINT,
// td          varchar(32),
// baseFee varchar(32))

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

	log.Error("inside of put block", "number", nInt)

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