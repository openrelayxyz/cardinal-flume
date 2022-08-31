package api

import (
	"context"
	"database/sql"

	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-evm/rlp"
	"github.com/openrelayxyz/cardinal-evm/vm"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-types/hexutil"

	"github.com/openrelayxyz/flume/plugins"
)

type BlockAPI struct {
	db      *sql.DB
	network uint64
	pl      *plugins.PluginLoader
}

func NewBlockAPI(db *sql.DB, network uint64, pl *plugins.PluginLoader) *BlockAPI {
	return &BlockAPI{
		db:      db,
		network: network,
		pl:      pl,
	}
}

func (api *BlockAPI) BlockNumber(ctx context.Context) (hexutil.Uint64, error) {

	blockNo, err := getLatestBlock(ctx, api.db)
	if err != nil {
		return 0, err
	}

	return hexutil.Uint64(blockNo), nil
}

func (api *BlockAPI) GetBlockByNumber(ctx context.Context, blockNumber vm.BlockNumber, includeTxns bool) (map[string]interface{}, error) {

	pluginMethods := api.pl.Lookup("GetBlockByNumber", func(v interface{}) bool {
		_, ok := v.(func(map[string]interface{}, *sql.DB) (map[string]interface{}, error))
		return ok
	})

	if blockNumber.Int64() < 0 {
		latestBlock, err := getLatestBlock(ctx, api.db)
		if err != nil {
			return nil, err
		}
		blockNumber = vm.BlockNumber(latestBlock)
	}

	blocks, err := getBlocks(ctx, api.db, includeTxns, api.network, "number = ?", blockNumber)
	if err != nil {
		return nil, err
	}
	var blockVal map[string]interface{}
	if len(blocks) > 0 {
		blockVal = blocks[0]
	}

	for _, fni := range pluginMethods {
		fn := fni.(func(map[string]interface{}, *sql.DB) (map[string]interface{}, error))
		if blockVal, err = fn(blockVal, api.db); err != nil {
			log.Warn("Error in plugin", "err", err.Error())
			return nil, nil
		}
	}

	return blockVal, nil
}

func (api *BlockAPI) GetBlockByHash(ctx context.Context, blockHash types.Hash, includeTxs bool) (map[string]interface{}, error) {

	pluginMethods := api.pl.Lookup("GetBlockByHash", func(v interface{}) bool {
		_, ok := v.(func(map[string]interface{}, *sql.DB) (map[string]interface{}, error))
		return ok
	})

	blocks, err := getBlocks(ctx, api.db, includeTxs, api.network, "hash = ?", trimPrefix(blockHash.Bytes()))
	if err != nil {
		return nil, err
	}
	var blockVal map[string]interface{}
	if len(blocks) > 0 {
		blockVal = blocks[0]
	}

	for _, fni := range pluginMethods {
		fn := fni.(func(map[string]interface{}, *sql.DB) (map[string]interface{}, error))
		if blockVal, err = fn(blockVal, api.db); err != nil {
			log.Warn("Error in plugin", "err", err.Error())
			return nil, nil
		}
	}

	return blockVal, nil
}

func (api *BlockAPI) GetBlockTransactionCountByNumber(ctx context.Context, blockNumber vm.BlockNumber) (hexutil.Uint64, error) {
	var err error
	var count hexutil.Uint64

	if blockNumber.Int64() < 0 {
		latestBlock, err := getLatestBlock(ctx, api.db)
		if err != nil {
			return 0, err
		}
		blockNumber = vm.BlockNumber(latestBlock)
	}
	count, err = txCount(ctx, api.db, "block = ?", blockNumber)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (api *BlockAPI) GetBlockTransactionCountByHash(ctx context.Context, blockHash types.Hash) (hexutil.Uint64, error) {
	var count hexutil.Uint64
	block, err := getBlocks(ctx, api.db, false, api.network, "hash = ?", trimPrefix(blockHash.Bytes()))
	if err != nil {
		return 0, err
	}
	var blockVal map[string]interface{}
	if len(block) > 0 {
		blockVal = block[0]
	}
	blockNumber := int64(blockVal["number"].(hexutil.Uint64))

	count, err = txCount(ctx, api.db, "block = ?", vm.BlockNumber(blockNumber))
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (api *BlockAPI) GetUncleCountByBlockNumber(ctx context.Context, blockNumber vm.BlockNumber) (hexutil.Uint64, error) {
	var uncles []byte
	unclesList := []types.Hash{}

	if blockNumber.Int64() < 0 {
		latestBlock, err := getLatestBlock(ctx, api.db)
		if err != nil {
			return 0, err
		}
		blockNumber = vm.BlockNumber(latestBlock)
	}
	if err := api.db.QueryRowContext(ctx, "SELECT uncles FROM blocks WHERE number = ?", blockNumber).Scan(&uncles); err != nil {
		return 0, err
	}
	rlp.DecodeBytes(uncles, &unclesList)

	return hexutil.Uint64(len(unclesList)), nil

}

func (api *BlockAPI) GetUncleCountByBlockHash(ctx context.Context, blockHash types.Hash) (hexutil.Uint64, error) {
	var uncles []byte
	unclesList := []types.Hash{}

	if err := api.db.QueryRowContext(ctx, "SELECT uncles FROM blocks WHERE hash = ?", trimPrefix(blockHash.Bytes())).Scan(&uncles); err != nil {
		return 0, err
	}
	rlp.DecodeBytes(uncles, &unclesList)

	return hexutil.Uint64(len(unclesList)), nil
}
