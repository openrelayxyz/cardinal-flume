package blaster
// #include "blaster.h"
import "C"

import (
	"fmt"
	// "encoding/json"
	// log "github.com/inconshreveable/log15"

	// "github.com/openrelayxyz/cardinal-types/hexutil"
	// "github.com/openrelayxyz/cardinal-types"
)

type BlastTx struct {
	Hash [32]byte
	Block uint64
	Gas uint64
	GasPrice uint64
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
	Type uint64
	Accesslist []byte
	GasFeeCap []byte
    GasTipCap []byte
}

var after C.longlong

func (b *TxBlaster) PutTx(tx BlastTx) {
	var inputPtr *C.char
	var valuePtr *C.char
	var conAddPtr *C.char
	var bloomPtr *C.char
	var accListPtr *C.char
	var gFeePtr *C.char
	var gTipPtr *C.char
	
	blockInt := C.longlong(tx.Block)
	hashPtr := (*C.char)(C.CBytes(tx.Hash[:32]))
	gasInt := C.longlong(tx.Gas)
	gasPriceInt := C.longlong(tx.GasPrice)
	inputLen := (C.size_t)(len(tx.Input))
	if inputLen > 20000 { // This value may need to be adjusted
		inputPtr = (*C.char)(C.CBytes([]byte{}))
		inputLen = 0
		b.appendToFile(tx.Block, tx.Hash, tx.Input) // we need to change this so that the entire transaction is skipped. 
	} else if inputLen > 0 {
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

	b.Lock.Lock()

	C.sqib_put_tx(
		b.DB,
		hashPtr,
		blockInt, 
		gasInt,
		gasPriceInt,
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
		typeInt,
		accListPtr,
		accListLen,
		gFeePtr,
		gFeeLen,
		gTipPtr,
		gTipLen,
	)
	b.Lock.Unlock()
}

type missingInput struct {
	number uint64
	hash string
	input []byte
}

func (b *TxBlaster) appendToFile(number uint64, hash [32]byte, input []byte) {

	hashSlice := hash[:]

	statement := fmt.Sprintf("UPDATE transactions SET input = X'%x' WHERE hash = X'%x';", input, hashSlice)
	// log.Error("this is the statement", "statement", statement)

	_, err := b.MIFile.Write([]byte(statement + "\n"))
	if err != nil {
		fmt.Println("Error writing to file:", err)
	}


	// log.Error("got a record", "hash", hexutil.Encode(hashSlice))
	
	// jsonData, err := json.Marshal(record)
	// if err != nil {
	// 	log.Error("Error marshaling missingInput JSON", "err", err, "number", number, "hash", hexutil.Encode(hashSlice))
	// }

	// if _, err = b.MIFile.Write(jsonData); err != nil {
	// 	log.Error("Error writing to missingInput file in put tx", "err", err, "number", number, "hash", hash)
	// }

}