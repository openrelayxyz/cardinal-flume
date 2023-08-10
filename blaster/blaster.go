package blaster

// #include "blaster.h"
import "C"

import (
	"unsafe"
	"sync"

	log "github.com/inconshreveable/log15"
)

type BlockBlaster struct {
	BlockDB unsafe.Pointer
	WithdrawalDB unsafe.Pointer
	BlockLock *sync.Mutex
	WithdrawalLock *sync.Mutex
}

// type WithdrawalBlaster struct {
// 	DB unsafe.Pointer
// 	Lock *sync.Mutex
// }

type TxBlaster struct {
	DB unsafe.Pointer
	Lock *sync.Mutex
}

type LogBlaster struct {
	DB unsafe.Pointer
	Lock *sync.Mutex
}

func NewBlasterBlockIndexer(dataBase string) *BlockBlaster {
	bdb, wdb := C.new_sqlite_block_blaster(C.CString(dataBase))
	// wdb := C.new_sqlite_withdrawal_blaster(C.CString(dataBase))

	b := &BlockBlaster{
		BlockDB: bdb,
		BlockLock: new(sync.Mutex),
		WithdrawalLock: new(sync.Mutex),
	}
	return b
}

// func NewBlasterWithdrawalIndexer(dataBase string) *WithdrawalBlaster {
// 	db := C.new_sqlite_withdrawal_blaster(C.CString(dataBase))

// 	b := &WithdrawalBlaster{
// 		DB: db,
// 		Lock: new(sync.Mutex),
// 	}
// 	return b
// }

func NewBlasterTxIndexer(dataBase string) *TxBlaster {
	db := C.new_sqlite_tx_blaster(C.CString(dataBase))

	b := &TxBlaster{
		DB: db,
		Lock: new(sync.Mutex),
	}
	return b
}

func NewBlasterLogIndexer(dataBase string) *LogBlaster {
	db := C.new_sqlite_log_blaster(C.CString(dataBase))

	b := &LogBlaster{
		DB: db,
		Lock: new(sync.Mutex),
	}
	return b
}

// need to implement a put method for blocks, logs, and transactions. 

type sliceHeader struct {
	p   unsafe.Pointer
	len int
	cap int
}
 
func (b *BlockBlaster) CloseBlocks() {
	defer log.Error("close called on blocks blaster")
	b.BlockLock.Lock()
	C.sqbb_close(b.BlockDB)
	C.sqwb_close(b.WithdrawalDB)
}

// func (b *BlockBlaster) CloseWithdrawals() {
// 	defer log.Error("close called on blocks blaster")
// 	b.WithdrawalLock.Lock()
// 	C.sqwb_close(b.DB)
// }

// func (b *BlockBlaster) Close() {
// 	defer log.Error("close called on withdrawals blaster")
// 	b.Lock.Lock()
// 	C.sqwb_close(b.DB)
// }

func (b *TxBlaster) Close() {
	defer log.Error("close called on tx blaster")
	b.Lock.Lock()
	C.sqtb_close(b.DB)
}

func (b *LogBlaster) Close() {
	defer log.Error("close called on log blaster")
	b.Lock.Lock()
	C.sqlb_close(b.DB)
}