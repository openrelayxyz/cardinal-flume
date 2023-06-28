package indexer

// #include "blaster.h"
import "C"

import (
	// "fmt"
	"unsafe"
	"math/big"

	log "github.com/inconshreveable/log15"
)

type BlastBlock struct {
	Hash [32]byte
	Coinbase [20]byte
	Number uint64
	Time *big.Int
	Bloom []byte
}

// type BlastBlock struct {
// 	Hash string
// 	Coinbase string
// 	Number string
// 	Time string
// 	Bloom string
// }


// func PrintC(bck BlastBlock) {
// 	hPtr := (*C.char)(unsafe.Pointer(&bck.Hash[0]))
// 	cPtr := (*C.char)(unsafe.Pointer(&bck.Coinbase[0]))
// 	bPtr := (*C.char)(unsafe.Pointer(&bck.Bloom[0]))
// 	nInt := C.longlong(bck.Number)
// 	tInt := C.longlong(bck.Time.Int64())
// 	fmt.Println(C.sqib_put_block(hPtr, cPtr, nInt, tInt, bPtr))
// }

// var BlastBlockZero = BlastBlock{
// 		Hash: [32]byte{112, 212, 25, 199, 230, 106, 81, 52, 26, 215, 250, 5, 66, 242, 40, 15, 38, 237, 155, 92, 190, 139, 62, 73, 225, 80, 161, 163, 167, 67, 180, 239},
// 		Coinbase: [20]byte{103, 11, 36, 97, 13, 249, 155, 22, 133, 174, 172, 13, 253, 83, 7, 185, 46, 12, 244, 215},
// 		Number: 1234567,
// 		Time: new(big.Int).SetInt64(32),
// 		Bloom: []byte(`132342352350ijdsaflkdsajlvkajsd02j30irjalkdfjli2jq0ijrofdsvkdgoi`),
// }

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

func (b *Blaster) Put(bck BlastBlock) {
	hPtr := (*C.char)(unsafe.Pointer(&bck.Hash[0]))
	cPtr := (*C.char)(unsafe.Pointer(&bck.Coinbase[0]))
	bPtr := (*C.char)(unsafe.Pointer(&bck.Bloom[0]))
	nInt := C.longlong(bck.Number)
	tInt := C.longlong(bck.Time.Int64())

	log.Error("inside of put", "number", nInt)

	C.sqib_put_block(
		b.DB, 
		hPtr, 
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