package api

import (
	// "sort"
  // "bytes"
  // "github.com/klauspost/compress/zlib"
  // "strings"
  // "time"
  // "encoding/json"
  // "math/big"
  // "net/http"
  "database/sql"
  "github.com/ethereum/go-ethereum/common"
  "github.com/ethereum/go-ethereum/common/hexutil"
  // "github.com/ethereum/go-ethereum/core/types"
  // "github.com/ethereum/go-ethereum/eth/filters"
  "github.com/ethereum/go-ethereum/rlp"
  "github.com/ethereum/go-ethereum/rpc"
  // "io/ioutil"
  // "io"
  "context"
  // "fmt"
  // "log"
  // "sync"
)

type BlockAPI struct {
	db *sql.DB
	network uint64
}

func NewBlockAPI (db *sql.DB, network uint64 ) *BlockAPI {
	return &BlockAPI{
		db: db,
		network: network,
	}
}

func (api *BlockAPI) Block() string {
	return "goodbuy horses"
}

// func (api *BlockAPI) ReturnBlock(ctx context.Context) [22]rpc.BlockNumber {
// 	return blockNumbers
// }

	func (api *BlockAPI) BlockNumber(ctx context.Context) (hexutil.Uint64, error) {
	  blockNo, err := getLatestBlock(ctx, api.db)
		if err != nil {return 0, err}
	  return hexutil.Uint64(blockNo), nil
	}

	func (api *BlockAPI) GetBlockByNumber(ctx context.Context, blockNumber rpc.BlockNumber, includeTxns bool) (map[string]interface{}, error) {

		if blockNumber.Int64() < 0 {
	    latestBlock, err := getLatestBlock(ctx, api.db)
	    if err != nil {
	      return nil, err
	    }
	    blockNumber = rpc.BlockNumber(latestBlock)
	  }

	  blocks, err := getBlocks(ctx, api.db, includeTxns, api.network, "number = ?", blockNumber)
	  if err != nil {
	    return nil, err
	  }
	  var blockVal map[string]interface{}
	  if len(blocks) > 0 {
	    blockVal = blocks[0]
	  }
	return blockVal, nil
	}


	func (api *BlockAPI) GetBlockByHash(ctx context.Context, blockHash common.Hash, includeTxs bool) (map[string]interface{}, error) {

	  blocks, err := getBlocks(ctx, api.db, includeTxs, api.network, "hash = ?", trimPrefix(blockHash.Bytes()))
		if err != nil {return nil, err}
	  var blockVal map[string]interface{}
	  if len(blocks) > 0 {
	    blockVal = blocks[0]
	  }
	return blockVal, nil
}

func (api *BlockAPI) GetBlockTransactionCountByNumber(ctx context.Context, blockNumber rpc.BlockNumber) (hexutil.Uint64, error) {
	var err error
	var count hexutil.Uint64

	if blockNumber.Int64() < 0 {
		latestBlock, err := getLatestBlock(ctx, api.db)
		if err != nil {
			return 0, err
		}
		blockNumber = rpc.BlockNumber(latestBlock)
	}
  count, err = txCount(ctx, api.db, "block = ?", blockNumber)
  if err != nil {return 0, err}
return count, nil
}

func (api *BlockAPI) GetBlockTransactionCountByHash(ctx context.Context, blockHash common.Hash) (hexutil.Uint64, error) {
  var count hexutil.Uint64

  if err := api.db.QueryRowContext(ctx, "SELECT count(*) FROM v_transactions WHERE blockHash = ?", trimPrefix(blockHash.Bytes())).Scan(&count); err != nil {
    return 0, err
  }
	return count, nil
}

func (api *BlockAPI) GetUncleCountByBlockNumber(ctx context.Context, blockNumber rpc.BlockNumber) (hexutil.Uint64, error) {
	var uncles []byte
	unclesList := []common.Hash{}

	if blockNumber.Int64() < 0 {
		latestBlock, err := getLatestBlock(ctx, api.db)
		if err != nil {
			return 0, err
		}
		blockNumber = rpc.BlockNumber(latestBlock)
	}
  if err := api.db.QueryRowContext(ctx, "SELECT uncles FROM blocks WHERE number = ?", blockNumber).Scan(&uncles); err != nil {
    return 0, err
  }
  rlp.DecodeBytes(uncles, &unclesList)

	return hexutil.Uint64(len(unclesList)), nil

	//is supposed to return count len of uncles list both here and below
}

func (api *BlockAPI) GetUncleCountByBlockHash(ctx context.Context, blockHash common.Hash) (hexutil.Uint64, error) {
	var uncles []byte
	unclesList := []common.Hash{}

  if err := api.db.QueryRowContext(ctx, "SELECT uncles FROM blocks WHERE hash = ?", trimPrefix(blockHash.Bytes())).Scan(&uncles); err != nil {
    return 0, err
  }
  rlp.DecodeBytes(uncles, &unclesList)

	return hexutil.Uint64(len(unclesList)), nil
}
