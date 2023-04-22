package plugins

import (
	"fmt"
	"math"
	"encoding/json"


	"github.com/openrelayxyz/cardinal-evm/common"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-rpc"
	"github.com/openrelayxyz/cardinal-types/hexutil"
)

type BlockNumberOrHash struct {
	BlockNumber      *rpc.BlockNumber `json:"blockNumber,omitempty"`
	BlockHash        *types.Hash  `json:"blockHash,omitempty"`
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
		bn := rpc.EarliestBlockNumber
		bnh.BlockNumber = &bn
		return nil
	case "latest":
		bn := rpc.LatestBlockNumber
		bnh.BlockNumber = &bn
		return nil
	case "pending":
		bn := rpc.PendingBlockNumber
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

type LogType struct {
	Address common.Address `json:"address" gencodec:"required"`
	Topics []types.Hash `json:"topics" gencodec:"required"`
	Data hexutil.Bytes `json:"data" gencodec: "required"`
	BlockNumber string `json:"blockNumber"`
	TxHash types.Hash `json:"transactionHash" gencodec:"required"`
	TxIndex hexutil.Uint `json:"transactionIndex"`
	BlockHash types.Hash `json:"blockHash"`
	Index hexutil.Uint `json:"logIndex"`
	Removed bool `json:"removed"`
}

type SortLogs []*LogType

func (ms SortLogs) Len() int {
	return len(ms)
}

func (ms SortLogs) Less(i, j int) bool {
	if ms[i].BlockNumber != ms[j].BlockNumber {
		return ms[i].BlockNumber < ms[j].BlockNumber
	}
	return ms[i].Index < ms[j].Index
}

func (ms SortLogs) Swap(i, j int) {
	ms[i], ms[j] = ms[j], ms[i]
}
