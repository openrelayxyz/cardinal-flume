package blaster

// #include "blaster.h"
import "C"

import (
	"unsafe"
	"sync"
	// "encoding/json"
	"reflect"
	"os"

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
	MIFile *os.File
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
	log.Info("block blaster initialized")
	return b
}

func NewBlasterWithdrawalIndexer(dataBase string) *WithdrawalBlaster {
	db := C.new_sqlite_withdrawal_blaster(C.CString(dataBase))

	b := &WithdrawalBlaster{
		DB: db,
		Lock: new(sync.Mutex),
	}
	log.Info("withdrawal blaster initialized")
	return b
}

func NewBlasterTxIndexer(dataBase string) *TxBlaster {
	db := C.new_sqlite_tx_blaster(C.CString(dataBase))
	file, err := os.OpenFile("missing_inputs.json", os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		log.Error("Error opening missing input file, TxBlaster is nil", "err", err)
		return nil
	}

	log.Error("file", "type", reflect.TypeOf(file))

	b := &TxBlaster{
		DB: db,
		Lock: new(sync.Mutex),
		MIFile: file,
	}
	log.Info("transaction blaster initialized")
	return b
}

func NewBlasterLogIndexer(dataBase string) *LogBlaster {
	db := C.new_sqlite_log_blaster(C.CString(dataBase))

	b := &LogBlaster{
		DB: db,
		Lock: new(sync.Mutex),
	}
	log.Info("log blaster initialized")
	return b
}

type sliceHeader struct {
	p   unsafe.Pointer
	len int
	cap int
}
 
func (b *BlockBlaster) Close() {
	defer log.Info("close called on blocks blaster")
	b.Lock.Lock()
	C.sqbb_close(b.DB)
}

func (b *WithdrawalBlaster) Close() {
	defer log.Info("close called on withdrawals blaster")
	b.Lock.Lock()
	C.sqwb_close(b.DB)
}

func (b *TxBlaster) Close() {
	defer log.Info("close called on tx blaster")
	b.Lock.Lock()
	b.MIFile.Close()
	C.sqtb_close(b.DB)
}

func (b *LogBlaster) Close() {
	defer log.Info("close called on log blaster")
	b.Lock.Lock()
	C.sqlb_close(b.DB)
}