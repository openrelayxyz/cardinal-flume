package blaster

// #include "blaster.h"
import "C"

import (
	"unsafe"
	"sync"

	log "github.com/inconshreveable/log15"
)

type BlockBlaster struct {
	DB unsafe.Pointer
	Lock *sync.Mutex
}

type WithdrawalBlaster struct {
	DB unsafe.Pointer
	Lock *sync.Mutex
}

type TxBlaster struct {
	DB unsafe.Pointer
	Lock *sync.Mutex
}

type LogBlaster struct {
	DB unsafe.Pointer
	Lock *sync.Mutex
}

func NewBlasterBlockIndexer(dataBase string) *BlockBlaster {
	db := C.new_sqlite_block_blaster(C.CString(dataBase))

	b := &BlockBlaster{
		DB: db,
		Lock: new(sync.Mutex),
	}
	return b
}

func NewBlasterWithdrawalIndexer(dataBase string) *WithdrawalBlaster {
	db := C.new_sqlite_withdrawal_blaster(C.CString(dataBase))

	b := &WithdrawalBlaster{
		DB: db,
		Lock: new(sync.Mutex),
	}
	return b
}

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
 
func (b *BlockBlaster) Close() {
	defer log.Error("close called on blocks blaster")
	b.Lock.Lock()
	C.sqbb_close(b.DB)
}

func (b *WithdrawalBlaster) Close() {
	defer log.Error("close called on withdrawals blaster")
	b.Lock.Lock()
	C.sqwb_close(b.DB)
}

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