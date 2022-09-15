package main

import (
	"encoding/hex"
	"database/sql"
	"context"
	"fmt"
	"math"
	"math/big"
	// "reflect"

	"github.com/xsleonard/go-merkle"
	log "github.com/inconshreveable/log15"
	evm "github.com/openrelayxyz/cardinal-evm/types"
	"github.com/openrelayxyz/cardinal-evm/common"
	"github.com/openrelayxyz/cardinal-types/hexutil"
	"github.com/openrelayxyz/cardinal-evm/crypto"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/flume/plugins"
	"github.com/openrelayxyz/flume/config"
	"golang.org/x/crypto/sha3"

)

type PolygonBorService struct {
	db *sql.DB
	cfg *config.Config
}


func (service *PolygonBorService) GetAuthor(ctx context.Context, blockNumber plugins.BlockNumber) (*common.Address, error) {
	
	var signerBytes []byte
	err := service.db.QueryRowContext(ctx, "SELECT coinbase FROM blocks.blocks where number = ?;", hexutil.Uint64(blockNumber)).Scan(&signerBytes)
	if err != nil {
		log.Info("GetAuthor error", "err", err.Error())
		return nil, err
	}
	
	signer := plugins.BytesToAddress(signerBytes)
	
	return &signer, nil
}

var (
	// MaxCheckpointLength is the maximum number of blocks that can be requested for constructing a checkpoint root hash
	MaxCheckpointLength = uint64(math.Pow(2, 15))
)

type MaxCheckpointLengthExceededError struct {
	Start uint64
	End   uint64
}

func (e *MaxCheckpointLengthExceededError) Error() string {
	return fmt.Sprintf(
		"Start: %d and end block: %d exceed max allowed checkpoint length: %d",
		e.Start,
		e.End,
		MaxCheckpointLength,
	)
}

type InvalidStartEndBlockError struct {
	Start         uint64
	End           uint64
	CurrentHeader uint64
}

func (e *InvalidStartEndBlockError) Error() string {
	return fmt.Sprintf(
		"Invalid parameters start: %d and end block: %d params",
		e.Start,
		e.End,
	)
}

func convert(input [][32]byte) [][]byte {
	output := make([][]byte, 0, len(input))

	for _, in := range input {
		newInput := make([]byte, len(in[:]))
		copy(newInput, in[:])
		output = append(output, newInput)
	}

	return output
}

func nextPowerOfTwo(n uint64) uint64 {
	if n == 0 {
		return 1
	}
	// http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++

	return n
}

func convertTo32(input []byte) (output [32]byte) {
	l := len(input)
	if l > 32 || l == 0 {
		return
	}

	copy(output[32-l:], input[:])

	return
}

func appendBytes32(data ...[]byte) []byte {
	var result []byte

	for _, v := range data {
		paddedV := convertTo32(v)
		result = append(result, paddedV[:]...)
	}

	return result
}

func (service *PolygonBorService) GetRootHash(ctx context.Context, start uint64, end uint64) (string, error) {
	
	length := end - start + 1
	
	if length > MaxCheckpointLength {
		return "", &MaxCheckpointLengthExceededError{start, end}
	}
	
	var currentNumber uint64
	err := service.db.QueryRowContext(ctx, "SELECT max(number) FROM blocks.blocks;").Scan(&currentNumber)
	if err != nil {
		log.Error("Error fetching latest blockin GetRootHash", "err", err.Error())
		return "", err
	}

	if start > end || end > currentNumber {
		return "", &InvalidStartEndBlockError{Start: start, End: end, CurrentHeader: currentNumber}
	}

	// var blockHeaders []*evm.Header
	blockHeaders := make([]*evm.Header, 0, end-start+1)

	rows, _ := service.db.QueryContext(context.Background(), "SELECT number, txRoot, receiptRoot, `time` FROM blocks.blocks WHERE number >= ? AND number <= ? ;", start, end)
	defer rows.Close()

	for rows.Next() {
		var txRoot, receiptRoot []byte
		var number, time uint64
		err := rows.Scan(&number, &txRoot, &receiptRoot, &time)
		if err != nil {
			log.Info("scan error GetRootHash", "err", err.Error())
			return "", err
		}
		bigNumber := new(big.Int).SetUint64(number)
		header := &evm.Header{
			Number: bigNumber,
			TxHash: plugins.BytesToHash(txRoot),
			Time: time,
			ReceiptHash: plugins.BytesToHash(receiptRoot),
		}

		blockHeaders = append(blockHeaders, header)

	}

	headers := make([][32]byte, nextPowerOfTwo(length))

	for i := 0; i < len(blockHeaders); i++ {
		blockHeader := blockHeaders[i]
		header := crypto.Keccak256(appendBytes32(
			blockHeader.Number.Bytes(),
			new(big.Int).SetUint64(blockHeader.Time).Bytes(),
			blockHeader.TxHash.Bytes(),
			blockHeader.ReceiptHash.Bytes(),
		))

		var arr [32]byte

		copy(arr[:], header)
		headers[i] = arr
	}

	tree := merkle.NewTreeWithOpts(merkle.TreeOptions{EnableHashSorting: false, DisableHashLeaves: true})
	if err := tree.Generate(convert(headers), sha3.NewLegacyKeccak256()); err != nil {
		return "", err
	}

	root := hex.EncodeToString(tree.Root().Hash)
	// api.rootHashCache.Add(key, root)

	return root, nil


}

func (service *PolygonBorService) GetSignersAtHash(ctx context.Context, hash types.Hash) ([]common.Address, error) {

	var result []common.Address

	snap, err := service.GetSnapshot(context.Background(), hash)
	if err != nil {
		log.Error("Error fetching snapshot GetSignersAtHash", "err", err.Error())
		return nil, err
	}

	for _, validator := range snap.ValidatorSet.Validators {
		result = append(result, validator.Address)
	}

	return result, nil 

}

func (service *PolygonBorService) GetCurrentValidators(ctx context.Context) ([]*Validator, error) {

	var blockNumber int64
	var hash []byte
	err := service.db.QueryRowContext(ctx, "SELECT max(number), hash FROM blocks.blocks;").Scan(&blockNumber, &hash)
	if err != nil {
		log.Info("GetCurentValidators error", "err", err.Error())
		return nil, err
	}

	var result []*Validator

	snap, err := service.GetSnapshot(context.Background(), plugins.BytesToHash(hash))
	if err != nil {
		log.Error("Error fetching snapshot GetCurrentValidators", "err", err.Error())
		return nil, err
	}

	for _, validator := range snap.ValidatorSet.Validators {
		result = append(result, validator)
	}

	return result, nil 

}

func (service *PolygonBorService) GetCurrentProposer(ctx context.Context) (*common.Address, error) {

	var result *common.Address

	var blockNumber int64
	var hash []byte
	err := service.db.QueryRowContext(ctx, "SELECT max(number), hash FROM blocks.blocks;").Scan(&blockNumber, &hash)

	snap, err := service.GetSnapshot(context.Background(), plugins.BytesToHash(hash))
	if err != nil {
		log.Error("Error fetching snapshot GetCurrentProposer", "err", err.Error())
		return nil, err
	}

	result = &snap.ValidatorSet.Proposer.Address

	return result, nil 

}