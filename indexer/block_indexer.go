package indexer

import (
	"encoding/binary"
	"fmt"
	"time"

	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-evm/crypto"
	"github.com/openrelayxyz/cardinal-evm/rlp"
	evm "github.com/openrelayxyz/cardinal-evm/types"
	"github.com/openrelayxyz/cardinal-streams/delivery"
	"github.com/openrelayxyz/cardinal-types"
	"golang.org/x/crypto/sha3"
	"io"
	"math/big"
	"regexp"
	"strconv"
	"sync"
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

func NewBlockIndexer(chainid uint64) Indexer {
	return &BlockIndexer{chainid: chainid}
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
	*blockTime = time.Unix(int64(header.Time), 0)

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
