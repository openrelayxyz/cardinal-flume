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
	h := &types.Hash{}
	if err := json.Unmarshal(data, h); err == nil {
		bnh.BlockHash = h
		return nil
	}
	bn := new(rpc.BlockNumber)
	err = json.Unmarshal(data, bn)
	if err == nil {
		bnh.BlockNumber = bn
	}
	return err
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
