package blaster

// #include "blaster.h"
import "C"

import (
	"unsafe"

	log "github.com/inconshreveable/log15"
)

type BlockBlaster struct {
	DB unsafe.Pointer
}

type TxBlaster struct {
	DB unsafe.Pointer
}

type LogBlaster struct {
	DB unsafe.Pointer
}

func NewBlasterBlockIndexer(dataBase string) *BlockBlaster {
	db := C.new_sqlite_block_blaster(C.CString(dataBase))

	b := &BlockBlaster{
		DB: db,
	}
	return b
}

func NewBlasterTxIndexer(dataBase string) *TxBlaster {
	db := C.new_sqlite_tx_blaster(C.CString(dataBase))

	b := &TxBlaster{
		DB: db,
	}
	return b
}

func NewBlasterLogIndexer(dataBase string) *LogBlaster {
	db := C.new_sqlite_log_blaster(C.CString(dataBase))

	b := &LogBlaster{
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
 
func (b *BlockBlaster) Close() {
	defer log.Error("close called on blocks blaster")
	C.sqbb_close(b.DB)
}

func (b *TxBlaster) Close() {
	defer log.Error("close called on tx blaster")
	C.sqtb_close(b.DB)
}

func (b *LogBlaster) Close() {
	defer log.Error("close called on log blaster")
	C.sqlb_close(b.DB)
}