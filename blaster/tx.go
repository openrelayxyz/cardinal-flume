package blaster
// #include "blaster.h"
import "C"

import (
	log "github.com/inconshreveable/log15"
)


// id INTEGER PRIMARY KEY AUTOINCREMENT,
// 		    gas BIGINT,
// 		    gasPrice BIGINT,
// 		    hash varchar(32),
// 		    input blob,
// 		    nonce BIGINT,
// 		    recipient varchar(20),
// 		    transactionIndex MEDIUMINT,
// 		    value varchar(32),
// 		    v SMALLINT,
// 		    r varchar(32),
// 		    s varchar(32),
// 		    sender varchar(20),
// 		    func varchar(4),
// 		    contractAddress varchar(20),
// 		    cumulativeGasUsed BIGINT,
// 		    gasUsed BIGINT,
// 		    logsBloom blob,
// 		    status TINYINT,
// 		    block BIGINT,
// 		    type TINYINT,
// 		    access_list blob,
// 		    gasFeeCap varchar(32),
// 		    gasTipCap varchar(32))

type BlastTx struct {
	// Id uint64
	Gas uint64
	GasPrice uint64
	Hash [32]byte
	Input []byte
	Nonce uint64
	Recipient [20]byte
	TransactionIndex uint64
	Value []byte
	V uint64
	R [32]byte
	S [32]byte
	Sender [20]byte
	Func [4]byte
	ContractAddress []byte
	CumulativeGasUsed uint64
	GasUsed uint64
	LogsBloom []byte
	Status uint64
	Block uint64
	Type uint64
	Accesslist []byte
	GasFeeCap []byte
    GasTipCap []byte
}

func (b *TxBlaster) PutTx(tx BlastTx) {
	var inputPtr *C.char
	var valuePtr *C.char
	var conAddPtr *C.char
	var bloomPtr *C.char
	var accListPtr *C.char
	var gFeePtr *C.char
	var gTipPtr *C.char
	
	gasInt := C.longlong(tx.Gas)
	gasPriceInt := C.longlong(tx.GasPrice)
	hashPtr := (*C.char)(C.CBytes(tx.Hash[:32]))
	inputLen := (C.size_t)(len(tx.Input))
	if inputLen > 0 {
		inputPtr = (*C.char)(C.CBytes(tx.Input[:inputLen]))
	}
	nonceInt := C.longlong(tx.Nonce)
	reciPtr := (*C.char)(C.CBytes(tx.Recipient[:20]))
	transDexInt := C.longlong(tx.TransactionIndex)
	valueLen := (C.size_t)(len(tx.Value))
	if valueLen > 0 {
		valuePtr = (*C.char)(C.CBytes(tx.Value[:valueLen]))
	}
	vInt := C.longlong(tx.V)
	rPtr := (*C.char)(C.CBytes(tx.R[:32]))
	sPtr := (*C.char)(C.CBytes(tx.S[:32]))
	sendPtr := (*C.char)(C.CBytes(tx.Sender[:20]))
	funcPtr := (*C.char)(C.CBytes(tx.Func[:4]))
	conAddLen := (C.size_t)(len(tx.ContractAddress))
	if conAddLen > 0 {
		conAddPtr = (*C.char)(C.CBytes(tx.ContractAddress[:conAddLen]))
	}
	cumulativeGasUsedInt := C.longlong(tx.CumulativeGasUsed)
	gasUsedInt := C.longlong(tx.GasUsed)
	bloomLen := (C.size_t)(len(tx.LogsBloom))
	if bloomLen > 0 {
		bloomPtr = (*C.char)(C.CBytes(tx.LogsBloom[:bloomLen]))
	}
	statInt := C.longlong(tx.Status)
	blockInt := C.longlong(tx.Block)
	log.Error("this is the c type block", "block", blockInt)
	typeInt := C.longlong(tx.Type)
	accListLen := (C.size_t)(len(tx.Accesslist))
	if accListLen > 0 {
		accListPtr = (*C.char)(C.CBytes(tx.Accesslist[:accListLen]))
	}
	gFeeLen := (C.size_t)(len(tx.GasFeeCap))
	if gFeeLen > 0 {
		gFeePtr = (*C.char)(C.CBytes(tx.GasFeeCap[:gFeeLen]))
	}
	gTipLen := (C.size_t)(len(tx.GasTipCap))
	if gTipLen > 0 {
		gTipPtr = (*C.char)(C.CBytes(tx.GasTipCap[:gTipLen]))
	}

	log.Error("inside of put tx", "number", blockInt)

	C.sqib_put_tx(
		b.DB, 
		gasInt,
		gasPriceInt,
		hashPtr,
		inputPtr,
		inputLen,
		nonceInt,
		reciPtr,
		transDexInt,
		valuePtr,
		valueLen,
		vInt,
		rPtr,
		sPtr,
		sendPtr,
		funcPtr,
		conAddPtr,
		conAddLen,
		cumulativeGasUsedInt,
		gasUsedInt,
		bloomPtr,
		bloomLen,
		statInt,
		blockInt,
		typeInt,
		accListPtr,
		accListLen,
		gFeePtr,
		gFeeLen,
		gTipPtr,
		gTipLen,
	)
	log.Error("just past the squib put tx function")
	

}