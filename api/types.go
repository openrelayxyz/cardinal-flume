package api

import (
	"fmt"
	"errors"
	"math"
	"math/big"
	"encoding/json"
	"strings"
	"strconv"
	// "reflect"

	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-evm/common"
	evm "github.com/openrelayxyz/cardinal-evm/types"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-rpc"
	"github.com/openrelayxyz/cardinal-types/hexutil"
)

type DecimalOrHex uint64

func (dh *DecimalOrHex) UnmarshalJSON(data []byte) error {
	input := strings.TrimSpace(string(data))
	if len(input) >= 2 && input[0] == '"' && input[len(input)-1] == '"' {
		input = input[1 : len(input)-1]
	}

	value, err := strconv.ParseUint(input, 10, 64)
	if err != nil {
		value, err = hexutil.DecodeUint64(input)
	}
	if err != nil {
		return err
	}
	*dh = DecimalOrHex(value)
	return nil
}

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

type logType struct {
	// Consensus fields:
	// address of the contract that generated the event
	Address common.Address `json:"address" gencodec:"required"`
	// list of topics provided by the contract.
	Topics []types.Hash `json:"topics" gencodec:"required"`
	// supplied by the contract, usually ABI-encoded
	Data hexutil.Bytes `json:"data" gencodec: "required"`

	// Derived fields. These fields are filled in by the node
	// but not secured by consensus.
	// block in which the transaction was included
	BlockNumber string `json:"blockNumber"`
	// hash of the transaction
	TxHash types.Hash `json:"transactionHash" gencodec:"required"`
	// index of the transaction in the block
	TxIndex hexutil.Uint `json:"transactionIndex"`
	// hash of the block in which the transaction was included
	BlockHash types.Hash `json:"blockHash"`
	// index of the log in the block
	Index hexutil.Uint `json:"logIndex"`

	// The Removed field is true if this log was reverted due to a chain reorganisation.
	// You must pay attention to this field if you receive logs through a filter query.
	Removed bool `json:"removed"`
}

type sortLogs []*logType

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

type sortTxMap []map[string]interface{}

func (ms sortTxMap) Len() int {
	return len(ms)
}

func (ms sortTxMap) Less(i, j int) bool {
	if ibni, ok := ms[i]["blockNumber"]; ok {
		jbni, ok := ms[j]["blockNumber"]
		if !ok {
			log.Warn("blockNumber not found")
			return false
		}
		switch ibn := ibni.(type) {
		case *hexutil.Big:
			jbn, ok := jbni.(*hexutil.Big)
			if !ok {
				log.Warn("blockNumber type mismatch")
				return false
			}
			if v := ibn.ToInt().Cmp(jbn.ToInt()); v != 0 {
				return v < 0
			}
		case hexutil.Uint64:
			jbn, ok := jbni.(hexutil.Uint64)
			if !ok {
				log.Warn("blockNumber type mismatch")
				return false
			}
			if jbn != ibn {
				return ibn < jbn
			}
		default:
			log.Warn("unknown BLOCK NUMBER type")
		}
	} else {
		log.Warn("Block number not found")
		return false
	}
	if itxi, ok := ms[i]["transactionIndex"]; ok {
		jtxi, ok := ms[j]["transactionIndex"]
		if !ok {
			log.Warn("transactionIndex not found")
			return false
		}
		switch itx := itxi.(type) {
		case *hexutil.Uint64:
			jtx, ok := jtxi.(*hexutil.Uint64)
			if !ok {
				log.Warn("transactionIndex type mismatch")
				return false
			}
			return *itx < *jtx
		case hexutil.Uint64:
			jtx, ok := jtxi.(hexutil.Uint64)
			if !ok {
				log.Warn("transactionIndex type mismatch")
			}
			return itx < jtx
		default:
			log.Warn("unknown tx index type")
			return false
		}
	} else {
		log.Warn("No tx index")
		return false
	}
	log.Warn("Sort mismatch")
	return false
}

func (ms sortTxMap) Swap(i, j int) {
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

type TransactionTypeOne struct {
	Transactions []*rpcTransaction `json:"transactions"`
}

type TransactionTypeTwo struct {
	Transactions []types.Hash `json:"transactions"`
}

type rpcTransaction struct {
	BlockHash        *types.Hash     `json:"blockHash"`
	BlockNumber      *hexutil.Big    `json:"blockNumber"`
	From             common.Address  `json:"from"`
	Gas              hexutil.Uint64  `json:"gas"`
	GasPrice         *hexutil.Big    `json:"gasPrice"`
	GasFeeCap        *hexutil.Big    `json:"maxFeePerGas,omitempty"`
	GasTipCap        *hexutil.Big    `json:"maxPriorityFeePerGas,omitempty"`
	Hash             types.Hash      `json:"hash"`
	Input            hexutil.Bytes   `json:"input"`
	Nonce            hexutil.Uint64  `json:"nonce"`
	To               *common.Address `json:"to"`
	TransactionIndex *hexutil.Uint64 `json:"transactionIndex"`
	Value            *hexutil.Big    `json:"value"`
	Type             hexutil.Uint64  `json:"type"`
	Accesses         *evm.AccessList `json:"accessList,omitempty"`
	ChainID          *hexutil.Big    `json:"chainId,omitempty"`
	V                *hexutil.Big    `json:"v"`
	R                *hexutil.Big    `json:"r"`
	S                *hexutil.Big    `json:"s"`
}

type FilterQuery struct {
	BlockHash *types.Hash      // used by eth_getLogs, return logs only from block with this hash
	FromBlock *rpc.BlockNumber       // beginning of the queried range, nil means genesis block
	ToBlock   *rpc.BlockNumber       // end of the range, nil means latest block
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

func (args FilterQuery) MarshalJSON() ([]byte, error) {
	type output struct {
		BlockHash *types.Hash     `json:"blockHash,omitempty"`
		FromBlock *rpc.BlockNumber `json:"fromBlock,omitempty"`
		ToBlock   *rpc.BlockNumber `json:"toBlock,omitempty"`
		Addresses []common.Address `json:"address,omitempty"`
		Topics    [][]types.Hash   `json:"topics,omitempty"`
	}
	out := output{
		BlockHash: args.BlockHash,
		Addresses: args.Addresses,
		Topics: args.Topics,
	}
	if args.FromBlock != nil {
		fromBlock := args.FromBlock
		out.FromBlock = fromBlock
	}
	if args.ToBlock != nil {
		toBlock := args.ToBlock
		out.ToBlock = toBlock
	}
	return json.Marshal(out)
}

// UnmarshalJSON sets *args fields with given data.
func (args *FilterQuery) UnmarshalJSON(data []byte) error {
	type input struct {
		BlockHash *types.Hash     `json:"blockHash"`
		FromBlock *rpc.BlockNumber `json:"fromBlock"`
		ToBlock   *rpc.BlockNumber `json:"toBlock"`
		Addresses interface{}      `json:"address"`
		Topics    []interface{}    `json:"topics"`
	}

	var raw input
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	if raw.BlockHash != nil {
		if raw.FromBlock != nil || raw.ToBlock != nil {
			// BlockHash is mutually exclusive with FromBlock/ToBlock criteria
			return errors.New("cannot specify both BlockHash and FromBlock/ToBlock, choose one or the other")
		}
		args.BlockHash = raw.BlockHash
	} else {
		if raw.FromBlock != nil {
			fromBlock := raw.FromBlock
			args.FromBlock = fromBlock
		}

		if raw.ToBlock != nil {
			toBlock := raw.ToBlock
			args.ToBlock = toBlock
		}
	}

	args.Addresses = []common.Address{}

	if raw.Addresses != nil {
		// raw.Address can contain a single address or an array of addresses
		switch rawAddr := raw.Addresses.(type) {
		case []interface{}:
			for i, addr := range rawAddr {
				if strAddr, ok := addr.(string); ok {
					addr, err := decodeAddress(strAddr)
					if err != nil {
						log.Error("invalid address at index %d: %v", i, err)
						return errors.New("invalid address")
					}
					args.Addresses = append(args.Addresses, addr)
				} else {
					log.Error("non-string address at index %d", i)
					return errors.New("non-string address")
				}
			}
		case string:
			addr, err := decodeAddress(rawAddr)
			if err != nil {
				log.Error("invalid address: %v", err)
				return errors.New("invalid address")
			}
			args.Addresses = []common.Address{addr}
		default:
			return errors.New("invalid addresses in query")
		}
	}

	// topics is an array consisting of strings and/or arrays of strings.
	// JSON null values are converted to common.Hash{} and ignored by the filter manager.
	if len(raw.Topics) > 0 {
		args.Topics = make([][]types.Hash, len(raw.Topics))
		for i, t := range raw.Topics {
			switch topic := t.(type) {
			case nil:
				// ignore topic when matching logs

			case string:
				// match specific topic
				top, err := decodeTopic(topic)
				if err != nil {
					return err
				}
				args.Topics[i] = []types.Hash{top}

			case []interface{}:
				// or case e.g. [null, "topic0", "topic1"]
				for _, rawTopic := range topic {
					if rawTopic == nil {
						// null component, match all
						args.Topics[i] = nil
						break
					}
					if topic, ok := rawTopic.(string); ok {
						parsed, err := decodeTopic(topic)
						if err != nil {
							return err
						}
						args.Topics[i] = append(args.Topics[i], parsed)
					} else {
						return errors.New("invalid topic(s)")
					}
				}
			default:
				return errors.New("invalid topic(s)")
			}
		}
	}

	return nil
}

func decodeAddress(s string) (common.Address, error) {
	b, err := hexutil.Decode(s)
	if err == nil && len(b) != common.AddressLength {
		log.Error("hex has invalid length %d after decoding; expected %d for address", len(b), common.AddressLength)
		err = errors.New("hex has invalid length, for address")
	}
	return bytesToAddress(b), err
}

func decodeTopic(s string) (types.Hash, error) {
	b, err := hexutil.Decode(s)
	if err == nil && len(b) != types.HashLength {
		log.Error("hex has invalid length %d after decoding; expected %d for topic", len(b), types.HashLength)
		err = errors.New("hex has invalid length, for topic")
	}
	return bytesToHash(b), err
}

const (
	SafeBlockNumber      = rpc.BlockNumber(-4)
	FinalizedBlockNumber = rpc.BlockNumber(-3)
	PendingBlockNumber   = rpc.BlockNumber(-2)
	LatestBlockNumber    = rpc.BlockNumber(-1)
	EarliestBlockNumber  = rpc.BlockNumber(0)
)

// The above has been modified to conform to Cardianl-rpc types and will need to be alligned with geth at some point


type BlockNumberOrHash struct {
	BlockNumber      *rpc.BlockNumber `json:"blockNumber,omitempty"`
	BlockHash        *types.Hash `json:"blockHash,omitempty"`
	RequireCanonical bool         `json:"requireCanonical,omitempty"`
}

func (bnh *BlockNumberOrHash) UnmarshalJSON(data []byte) error {
	type erased BlockNumberOrHash
	e := erased{}
	err := json.Unmarshal(data, &e)
	if err == nil {
		if e.BlockNumber != nil && e.BlockHash != nil {
			return fmt.Errorf("cannot specify both BlockHash and BlockNumber, choose one or the other")
		}
		bnh.BlockNumber = e.BlockNumber
		bnh.BlockHash = e.BlockHash
		bnh.RequireCanonical = e.RequireCanonical
		return nil
	}
	var input string
	err = json.Unmarshal(data, &input)
	if err != nil {
		return err
	}
	switch input {
	case "earliest":
		bn := EarliestBlockNumber
		bnh.BlockNumber = &bn
		return nil
	case "latest":
		bn := LatestBlockNumber
		bnh.BlockNumber = &bn
		return nil
	case "pending":
		bn := PendingBlockNumber
		bnh.BlockNumber = &bn
		return nil
	case "finalized":
		bn := FinalizedBlockNumber
		bnh.BlockNumber = &bn
		return nil
	case "safe":
		bn := SafeBlockNumber
		bnh.BlockNumber = &bn
		return nil
	default:
		if len(input) == 66 {
			hash := types.Hash{}
			err := hash.UnmarshalText([]byte(input))
			if err != nil {
				return err
			}
			bnh.BlockHash = &hash
			return nil
		} else {
			blckNum, err := hexutil.DecodeUint64(input)
			if err != nil {
				return err
			}
			if blckNum > math.MaxInt64 {
				return fmt.Errorf("blocknumber too high")
			}
			bn := rpc.BlockNumber(blckNum)
			bnh.BlockNumber = &bn
			return nil
		}
	}
}

func (bnh *BlockNumberOrHash) Number() (rpc.BlockNumber, bool) {
	if bnh.BlockNumber != nil {
		return *bnh.BlockNumber, true
	}
	return rpc.BlockNumber(0), false
}

func (bnh *BlockNumberOrHash) String() string {
	if bnh.BlockNumber != nil {
		return strconv.Itoa(int(*bnh.BlockNumber))
	}
	if bnh.BlockHash != nil {
		return bnh.BlockHash.String()
	}
	return "nil"
}

func (bnh *BlockNumberOrHash) Hash() (types.Hash, bool) {
	if bnh.BlockHash != nil {
		return *bnh.BlockHash, true
	}
	return types.Hash{}, false
}

func BlockNumberOrHashWithNumber(blockNr rpc.BlockNumber) BlockNumberOrHash {
	return BlockNumberOrHash{
		BlockNumber:      &blockNr,
		BlockHash:        nil,
		RequireCanonical: false,
	}
}

func BlockNumberOrHashWithHash(hash types.Hash, canonical bool) BlockNumberOrHash {
	return BlockNumberOrHash{
		BlockNumber:      nil,
		BlockHash:        &hash,
		RequireCanonical: canonical,
	}
}