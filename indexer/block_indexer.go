package indexer
// #cgo CXXFLAGS: -std=c++11
// #cgo LDFLAGS: -lstdc++
import "C"

import (
	"encoding/binary"
	"fmt"
	"time"
	"io"
	"math/big"
	"regexp"
	"strconv"
	"sync"
	"reflect"
	"strings"

	"golang.org/x/crypto/sha3"

	log "github.com/inconshreveable/log15"
	
	"github.com/openrelayxyz/cardinal-evm/crypto"
	"github.com/openrelayxyz/cardinal-evm/rlp"
	evm "github.com/openrelayxyz/cardinal-evm/types"
	"github.com/openrelayxyz/cardinal-streams/delivery"
	"github.com/openrelayxyz/cardinal-types"

	"github.com/openrelayxyz/cardinal-flume/blaster"
)

var (
	uncleRegexp = regexp.MustCompile("c/[0-9a-z]+/b/([0-9a-z]+)/u/([0-9a-z]+)")
	txRegexp    = regexp.MustCompile("c/[0-9a-z]+/b/([0-9a-z]+)/t/([0-9a-z]+)")
)

type rlpData []byte

func (d rlpData) EncodeRLP(w io.Writer) error {
	_, err := w.Write(d)
	return err
}

var hasherPool = sync.Pool{
	New: func() interface{} { return sha3.NewLegacyKeccak256() },
}

type BlockIndexer struct {
	chainid uint64
	blastIdx *blaster.Blaster
}

type extblock struct {
	Header *evm.Header
	Txs    []evm.Transaction
	Uncles []rlpData
}

type extblockWithdrawals struct {
	Header *evm.Header
	Txs    []evm.Transaction
	Uncles []rlpData
	Withdrawals  evm.Withdrawals 
}

func NewBlockIndexer(chainid uint64, blasterIndexer *blaster.Blaster) Indexer {
	return &BlockIndexer{
		chainid: chainid,
		blastIdx: blasterIndexer,
	}
}

func (indexer *BlockIndexer) Index(pb *delivery.PendingBatch) ([]string, error) {
	var withdrawals evm.Withdrawals 
    if withdrawalBytes, ok := pb.Values[fmt.Sprintf("c/%x/b/%x/w", indexer.chainid, pb.Hash.Bytes())]; ok {
		if err := rlp.DecodeBytes(withdrawalBytes, &withdrawals); err != nil {
			log.Error("Rlp decoding error on withdrawls", "block", pb.Number)
			return nil, err
		}
	}
	headerBytes := pb.Values[fmt.Sprintf("c/%x/b/%x/h", indexer.chainid, pb.Hash.Bytes())]
	tdBytes := pb.Values[fmt.Sprintf("c/%x/b/%x/d", indexer.chainid, pb.Hash.Bytes())]
	td := new(big.Int).SetBytes(tdBytes)
	header := &evm.Header{}
	if err := rlp.DecodeBytes(headerBytes, &header); err != nil {
		return nil, err
	}
	bt := time.Unix(int64(header.Time), 0)
	blockTime = &bt

	eblock := &extblock{
		Header: header,
		Txs:    []evm.Transaction{},
		Uncles: []rlpData{},
	}

	uncleHashes := make(map[int64]types.Hash)
	txData := make(map[int64]evm.Transaction)
	uncleData := make(map[int64]rlpData)

	for k, v := range pb.Values {
		switch {
		case uncleRegexp.MatchString(k):
			parts := uncleRegexp.FindSubmatch([]byte(k))
			uncleIndex, _ := strconv.ParseInt(string(parts[2]), 16, 64)
			uncleHashes[int64(uncleIndex)] = crypto.Keccak256Hash(v)
			uncleData[int64(uncleIndex)] = rlpData(v)
		case txRegexp.MatchString(k):
			parts := txRegexp.FindSubmatch([]byte(k))
			txIndex, _ := strconv.ParseInt(string(parts[2]), 16, 64)
			var tx evm.Transaction
			tx.UnmarshalBinary(v)
			txData[int64(txIndex)] = tx
		default:
		}
	}
	eblock.Txs = make([]evm.Transaction, len(txData))
	for i, v := range txData {
		eblock.Txs[int(i)] = v
	}
	eblock.Uncles = make([]rlpData, len(uncleData))
	for i, v := range uncleData {
		eblock.Uncles[int(i)] = v
	}
	var size int
	if header.WithdrawalsHash != nil {
		eblockWithWithdrawals := &extblockWithdrawals{
			Header: eblock.Header,
			Txs:    eblock.Txs,
			Uncles: eblock.Uncles,
			Withdrawals: withdrawals,
		}
		ebwd, _ := rlp.EncodeToBytes(eblockWithWithdrawals)
		size = len(ebwd)
	} else {
		ebd, _ := rlp.EncodeToBytes(eblock)
		size = len(ebd)
	}
	uncles := make([]types.Hash, len(uncleHashes))
	for i, v := range uncleHashes {
		uncles[int(i)] = v
	}
	uncleRLP, _ := rlp.EncodeToBytes(uncles)

	if indexer.blastIdx != nil && pb.Number != 0 {

		// log.Error("hash", "hash", ApplyParameters("", pb.Hash))

		// hsh := ApplyBlasterParameters(pb.Hash)
		// var hash [32]byte
		// copy(hash[:], trimPrefix(pb.Hash.Bytes()))
		// h := ApplyParameters("", pb.Hash)
		// copy(hash[:], []byte(h[16:len(h)-2]))
		// var prntHsh [32]byte
		// ph := ApplyParameters("", pb.ParentHash)
		// copy(prntHsh[:], []byte(h[16:len(h)-2]))

		// hash := ApplyBlasterParameters(pb.Hash.Bytes())

		log.Error("experiment", "reuslt", strings.ToUpper(("X'" + pb.Hash.String()[2:] + "'")), "string?", reflect.TypeOf(ApplyBlasterParameters(pb.Hash).(string)), "interface?", reflect.TypeOf(ApplyBlasterParameters(pb.Hash)))

		// var hash [32]byte
		// b := []byte(fmt.Sprintf("X'%v", pb.Hash.String()[2:]))
		// copy(hash[:], b)
		// strings.ToUpper(("X'" + pb.Hash.String()[2:] + "'"))

	


		var cnbs [20]byte
		cb := ApplyParameters("", header.Coinbase)
		copy(cnbs[:], []byte(cb[16:len(cb)-2]))
		// tm := new(big.Int)
    	// tm.SetUint64(header.Time) 
		bloom := ApplyParameters("", compress(header.Bloom[:]))

		var BlstBlck = blaster.BlastBlock{
			Hash: [32]byte(pb.Hash),
			// ParentHash: prntHsh,
			Coinbase: cnbs,
			Number: uint64(pb.Number),
			Bloom: []byte(bloom[16:len(bloom)-2]),
			Time: header.Time,
			Difficulty: header.Difficulty.Int64(),
			GasLimit: header.GasLimit,
			GasUsed: header.GasUsed,
		}
		log.Error("calling put from within the block indexer")
		indexer.blastIdx.Put(BlstBlck)
		return nil, nil
	}

	statements := []string{
		ApplyParameters("DELETE FROM blocks WHERE number >= %v", pb.Number), 
		ApplyParameters("DELETE FROM withdrawals WHERE block >= %v", pb.Number),
	}
	
	if withdrawals.Len() > 0 {
		for _, wtdrl := range withdrawals {
			statements = append(statements, ApplyParameters(
			"INSERT INTO withdrawals(wtdrlIndex, vldtrIndex, address, amount, block, blockHash) VALUES (%v, %v, %v, %v, %v, %v)",
			wtdrl.Index,
			wtdrl.Validator,
			trimPrefix(wtdrl.Address[:]),
			wtdrl.Amount,
			pb.Number,
			pb.Hash,))
		}
	}

	log.Error("types", "number", reflect.TypeOf(pb.Number), "hash", reflect.TypeOf(pb.Hash), "parent hash", reflect.TypeOf(pb.ParentHash), "unclehash", reflect.TypeOf(header.UncleHash))
	log.Error("types2", "coinbase", reflect.TypeOf(header.Coinbase), "root", reflect.TypeOf(header.Root), "tx root", reflect.TypeOf(header.TxHash), "receiptRoot", reflect.TypeOf(header.Root))
	log.Error("types3", "bloom", reflect.TypeOf(compress(header.Bloom[:])), "difficulty", reflect.TypeOf(header.Difficulty.Int64()), "gaslimit", reflect.TypeOf(header.GasLimit), "gasused", reflect.TypeOf(header.GasUsed))
	log.Error("types4", "time", reflect.TypeOf(header.Time), "extra", reflect.TypeOf(header.Extra), "mixed", reflect.TypeOf(header.MixDigest), "nonce", reflect.TypeOf(int64(binary.BigEndian.Uint64(header.Nonce[:]))))
	log.Error("types5", "uncleRLP", reflect.TypeOf(uncleRLP), "size", reflect.TypeOf(size), "td", reflect.TypeOf(td.Bytes), "basefee", reflect.TypeOf(header.BaseFee), "whash", reflect.TypeOf(header.WithdrawalsHash))
	

	statements = append(statements, ApplyParameters(
		"INSERT INTO blocks(number, hash, parentHash, uncleHash, coinbase, root, txRoot, receiptRoot, bloom, difficulty, gasLimit, gasUsed, `time`, extra, mixDigest, nonce, uncles, size, td, baseFee, withdrawalHash) VALUES (%v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v)",
		pb.Number,
		pb.Hash,
		pb.ParentHash,
		header.UncleHash,
		header.Coinbase,
		header.Root,
		header.TxHash,
		header.ReceiptHash,
		compress(header.Bloom[:]),
		header.Difficulty.Int64(),
		header.GasLimit,
		header.GasUsed,
		header.Time,
		header.Extra,
		header.MixDigest,
		int64(binary.BigEndian.Uint64(header.Nonce[:])),
		uncleRLP,
		size,
		td.Bytes(),
		header.BaseFee,
		header.WithdrawalsHash,
	))
	return statements, nil
}
