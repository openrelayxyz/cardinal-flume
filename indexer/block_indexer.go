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
	blastBlockIdx *blaster.BlockBlaster
	blastWithdrawalIdx *blaster.WithdrawalBlaster
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

func NewBlockIndexer(chainid uint64, blasterBlockIndexer *blaster.BlockBlaster, blasterWithdrawalIndexer *blaster.WithdrawalBlaster) Indexer {
	return &BlockIndexer{
		chainid: chainid,
		blastBlockIdx: blasterBlockIndexer,
		blastWithdrawalIdx: blasterWithdrawalIndexer,
	}
}

func (indexer *BlockIndexer) Index(pb *delivery.PendingBatch) ([]string, error) {
	log.Error("inside of Block indexer")
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

	if indexer.blastBlockIdx != nil && pb.Number != 0 {
		return indexer.blockBatchIndex(header, pb, td, size, uncleRLP, withdrawals)
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

	// log.Error("types", "number", reflect.TypeOf(pb.Number), "hash", reflect.TypeOf(pb.Hash), "parent hash", reflect.TypeOf(pb.ParentHash), "unclehash", reflect.TypeOf(header.UncleHash))
	// log.Error("types2", "coinbase", reflect.TypeOf(header.Coinbase), "root", reflect.TypeOf(header.Root), "tx root", reflect.TypeOf(header.TxHash), "receiptRoot", reflect.TypeOf(header.Root))
	// log.Error("types3", "bloom", reflect.TypeOf(compress(header.Bloom[:])), "difficulty", reflect.TypeOf(header.Difficulty.Int64()), "gaslimit", reflect.TypeOf(header.GasLimit), "gasused", reflect.TypeOf(header.GasUsed))
	// log.Error("types4", "time", reflect.TypeOf(header.Time), "extra", reflect.TypeOf(header.Extra), "mixed", reflect.TypeOf(header.MixDigest), "nonce", reflect.TypeOf(int64(binary.BigEndian.Uint64(header.Nonce[:]))))
	// log.Error("types5", "uncleRLP", reflect.TypeOf(uncleRLP), "size", reflect.TypeOf(size), "td", reflect.TypeOf(td.Bytes), "basefee", reflect.TypeOf(header.BaseFee), "whash", reflect.TypeOf(header.WithdrawalsHash))
	

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

func (indexer *BlockIndexer) blockBatchIndex(header *evm.Header, pb *delivery.PendingBatch, td *big.Int, size int, uncleRLP []byte, withdrawals evm.Withdrawals) ([]string, error) {

	//TODO: withdrawals

	var totalD [32]byte
	copy(totalD[:], td.Bytes())

	var baseFee [32]byte
	copy(baseFee[:], header.BaseFee.Bytes())

	if header.WithdrawalsHash != nil {
		var wHash [32]byte
		copy(wHash[:], header.WithdrawalsHash.Bytes())
	}

	var BlstBlck = blaster.BlastBlock{
		Number: uint64(pb.Number),
		Hash: [32]byte(pb.Hash),
		ParentHash: [32]byte(pb.ParentHash),
		UncleHash: [32]byte(header.UncleHash),
		Coinbase: [20]byte(header.Coinbase),
		Root: [32]byte(header.Root),
		TxRoot: [32]byte(header.TxHash),
		ReceiptRoot: [32]byte(header.ReceiptHash),
		Bloom: []byte(compress(header.Bloom[:])),
		Difficulty: header.Difficulty.Uint64(),
		GasLimit: header.GasLimit,
		GasUsed: header.GasUsed,
		Time: header.Time,
		Extra: []byte(header.Extra),
		MixDigest: [32]byte(header.MixDigest),
		Nonce: uint64(binary.BigEndian.Uint64(header.Nonce[:])),
		Uncles: []byte(uncleRLP),
		Size: uint64(size),
		Td: totalD,
		BaseFee: baseFee,
	}

	// WithdrawalHash: wHash,

	log.Debug("calling put from within the block indexer")

	indexer.blastBlockIdx.PutBlock(BlstBlck)

	if indexer.blastWithdrawalIdx != nil && withdrawals.Len() > 0 {
		log.Error("NOTTTTT EEEEEEEMMMMMPPPPPPPPPTTTTTTTYYYYYYYYY")

		for _, wtdrl := range withdrawals {

			var address [20]byte
			copy(address[:], trimPrefix(wtdrl.Address[:]))

			var BlstWthdrl = blaster.BlastWithdrawal{
			Block: uint64(pb.Number),
			WithdrawalIndex: uint64(wtdrl.Index),
			ValidatorIndex: uint64(wtdrl.Validator),
			Address: address,
			Amount: wtdrl.Amount,
			BlockHash: pb.Hash,
			}
		indexer.blastWithdrawalIdx.PutWithdrawal(BlstWthdrl)
		log.Debug("calling put from within the withdrawals indexer")
		}
	}

	

	// for _, wd := range withdrawls {
	// 	indexer.blastIdx.PutWitdrawal(withdrawalType)
	// }
	return nil, nil
}
