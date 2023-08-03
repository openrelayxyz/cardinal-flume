package blaster

// #include "blaster.h"
import "C"

import (
	"unsafe"

	log "github.com/inconshreveable/log15"
)

type Blaster struct {
	DB unsafe.Pointer
}

func NewBlasterBlockIndexer(dataBase string) *Blaster {
	db := C.new_sqlite_block_blaster(C.CString(dataBase))

	b := &Blaster{
		DB: db,
	}
	return b
}

func NewBlasterTxIndexer(dataBase string) *Blaster {
	db := C.new_sqlite_tx_blaster(C.CString(dataBase))

	b := &Blaster{
		DB: db,
	}
	return b
}

func NewBlasterLogIndexer(dataBase string) *Blaster {
	db := C.new_sqlite_log_blaster(C.CString(dataBase))

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
 
func (b *Blaster) Close() {
	defer log.Error("close called on blaster")
	C.sqib_close(b.DB)
}