package api

import (
	"math/big"

	evm "github.com/openrelayxyz/cardinal-evm/types"
	"github.com/openrelayxyz/cardinal-evm/common"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-types/hexutil"
)

type DecimalOrHex uint64

type bigList []*big.Int

func (ms bigList) Len() int {
	return len(ms)
}

func (ms bigList) Less(i, j int) bool {
	return ms[i].Cmp(ms[j]) < 0
}

func (ms bigList) Swap(i, j int) {
	ms[i], ms[j] = ms[j], ms[i]
}

type sortLogs []*evm.Log

func (ms sortLogs) Len() int {
	return len(ms)
}

func (ms sortLogs) Less(i, j int) bool {
	if ms[i].BlockNumber != ms[j].BlockNumber {
		return ms[i].BlockNumber < ms[j].BlockNumber
	}
	return ms[i].Index < ms[j].Index
}

func (ms sortLogs) Swap(i, j int) {
	ms[i], ms[j] = ms[j], ms[i]
}

type feeHistoryResult struct {
	OldestBlock  *hexutil.Big     `json:"oldestBlock"`
	Reward       [][]*hexutil.Big `json:"reward,omitempty"`
	BaseFee      []*hexutil.Big   `json:"baseFeePerGas,omitempty"`
	GasUsedRatio []float64        `json:"gasUsedRatio"`
}

// txGasAndReward is sorted in ascending order based on reward
type (
	txGasAndReward struct {
		gasUsed uint64
		reward  *big.Int
	}
	sortGasAndReward []txGasAndReward
)

func (s sortGasAndReward) Len() int { return len(s) }
func (s sortGasAndReward) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s sortGasAndReward) Less(i, j int) bool {
	return s[i].reward.Cmp(s[j].reward) < 0
}

type paginator[T any] struct {
	Items []T         `json:"items"`
	Token interface{} `json:"next,omitempty"`
}

// type Block struct {
// 	BaseFeePerGas    *hexutil.Big     `json:"baseFeePerGas,omitempty"`
// 	Difficulty       hexutil.Uint64   `json:"difficulty"`
// 	ExtraData        hexutil.Bytes    `json:"extraData"`
// 	GasLimit         hexutil.Uint64   `json:"gasLimit"`
// 	GasUsed          hexutil.Uint64   `json:"gasUsed"`
// 	Hash             types.Hash      `json:"hash"`
// 	LogsBloom        hexutil.Bytes    `json:"logsBloom"`
// 	Miner            common.Address   `json:"miner"`
// 	MixHash          types.Hash      `json:"mixHash"`
// 	Nonce            evm.BlockNonce `json:"nonce"`
// 	Number           hexutil.Uint64   `json:"number"`
// 	ParentHash       types.Hash      `json:"parentHash"`
// 	ReceiptsRoot     types.Hash      `json:"receiptsRoot"`
// 	Sha3Uncles       types.Hash      `json:"sha3Uncles"`
// 	Size             hexutil.Uint64   `json:"size"`
// 	StateRoot        types.Hash      `json:"stateRoot"`
// 	Timestamp        hexutil.Uint64   `json:"timeStamp"`
// 	TotalDifficulty  *hexutil.Big     `json:"totalDifficulty"`
// 	Transactions     interface{}      `json:"transactiions"`
// 	TransactionsRoot types.Hash      `json:"transactionsRoot"`
// 	Uncles           []types.Hash    `json:"uncles"`
// }

type TransactionTypeOne struct {
	Transactions []*rpcTransaction `json:"transactions"`
}

type TransactionTypeTwo struct {
	Transactions []types.Hash `json:"transactions"`
}

type rpcTransaction struct {
	BlockHash        *types.Hash      `json:"blockHash"`
	BlockNumber      *hexutil.Big      `json:"blockNumber"`
	From             common.Address    `json:"from"`
	Gas              hexutil.Uint64    `json:"gas"`
	GasPrice         *hexutil.Big      `json:"gasPrice"`
	GasFeeCap        *hexutil.Big      `json:"maxFeePerGas,omitempty"`
	GasTipCap        *hexutil.Big      `json:"maxPriorityFeePerGas,omitempty"`
	Hash             types.Hash       `json:"hash"`
	Input            hexutil.Bytes     `json:"input"`
	Nonce            hexutil.Uint64    `json:"nonce"`
	To               *common.Address   `json:"to"`
	TransactionIndex *hexutil.Uint64   `json:"transactionIndex"`
	Value            *hexutil.Big      `json:"value"`
	Type             hexutil.Uint64    `json:"type"`
	Accesses         *evm.AccessList `json:"accessList,omitempty"`
	ChainID          *hexutil.Big      `json:"chainId,omitempty"`
	V                *hexutil.Big      `json:"v"`
	R                *hexutil.Big      `json:"r"`
	S                *hexutil.Big      `json:"s"`
}

type FilterQuery struct {
	BlockHash *types.Hash     // used by eth_getLogs, return logs only from block with this hash
	FromBlock *big.Int         // beginning of the queried range, nil means genesis block
	ToBlock   *big.Int         // end of the range, nil means latest block
	Addresses []common.Address // restricts matches to events created by specific contracts

	// The Topic list restricts matches to particular event topics. Each event has a list
	// of topics. Topics matches a prefix of that list. An empty element slice matches any
	// topic. Non-empty elements represent an alternative that matches any of the
	// contained topics.
	//
	// Examples:
	// {} or nil          matches any topic list
	// {{A}}              matches topic A in first position
	// {{}, {B}}          matches any topic in first position AND B in second position
	// {{A}, {B}}         matches topic A in first position AND B in second position
	// {{A, B}, {C, D}}   matches topic (A OR B) in first position AND (C OR D) in second position
	Topics [][]types.Hash
}
