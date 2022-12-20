package api

import (
	"context"
	"database/sql"

	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-evm/rlp"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-types/hexutil"
	"github.com/openrelayxyz/cardinal-types/metrics"

	"github.com/openrelayxyz/cardinal-flume/config"
	"github.com/openrelayxyz/cardinal-flume/heavy"
	"github.com/openrelayxyz/cardinal-flume/plugins"
)

var (
	hitMeter  = metrics.NewMajorMeter("/flume/hit")
	missMeter = metrics.NewMajorMeter("/flume/miss")
)

type BlockAPI struct {
	db      *sql.DB
	network uint64
	pl      *plugins.PluginLoader
	cfg     *config.Config
}

func NewBlockAPI(db *sql.DB, network uint64, pl *plugins.PluginLoader, cfg *config.Config) *BlockAPI {
	return &BlockAPI{
		db:      db,
		network: network,
		pl:      pl,
		cfg:     cfg,
	}
}

func (api *BlockAPI) ChainId(ctx context.Context) hexutil.Uint64 {

	log.Debug("eth_chainId served from flume light by default")
	hitMeter.Mark(1)

	return hexutil.Uint64(api.cfg.Chainid)
}

func (api *BlockAPI) BlockNumber(ctx context.Context) (hexutil.Uint64, error) {

	log.Debug("eth_blockNumber served from flume light by default")
	hitMeter.Mark(1)

	blockNo, err := getLatestBlock(ctx, api.db)
	if err != nil {
		return 0, err
	}
	return hexutil.Uint64(blockNo), nil
}

var (
	gbbnHitMeter  = metrics.NewMinorMeter("/flume/gbbn/hit")
	gbbnMissMeter = metrics.NewMinorMeter("/flume/gbbn/miss")
)

func (api *BlockAPI) GetBlockByNumber(ctx context.Context, blockNumber plugins.BlockNumber, includeTxns bool) (*map[string]interface{}, error) {


	if len(api.cfg.HeavyServer) > 0 && !blockDataPresent(blockNumber, api.cfg, api.db) {
		log.Debug("eth_blockByNumber sent to flume heavy")
		missMeter.Mark(1)
		gbbnMissMeter.Mark(1)
		responseShell, err := heavy.CallHeavy[map[string]interface{}](ctx, api.cfg.HeavyServer, "eth_getBlockByNumber", hexutil.Uint64(blockNumber), includeTxns)
		if err != nil {
			return nil, err
		}
		return responseShell, nil
	}

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("eth_getBlockByNumber served from flume light")
		hitMeter.Mark(1)
		gbbnHitMeter.Mark(1)
	}

	pluginMethods := api.pl.Lookup("GetBlockByNumber", func(v interface{}) bool {
		_, ok := v.(func(map[string]interface{}, *sql.DB) (map[string]interface{}, error))
		return ok
	})

	if blockNumber.Int64() < 0 {
		latestBlock, err := getLatestBlock(ctx, api.db)
		if err != nil {
			return nil, err
		}
		blockNumber = plugins.BlockNumber(latestBlock)
	}

	blocks, err := getBlocks(ctx, api.db, includeTxns, api.network, "number = ?", blockNumber)
	if err != nil {
		return nil, err
	}
	var blockVal map[string]interface{}
	if len(blocks) > 0 {
		blockVal = blocks[0]
	

		for _, fni := range pluginMethods {
			fn := fni.(func(map[string]interface{}, *sql.DB) (map[string]interface{}, error))
			if pluginBlockVal, err := fn(blockVal, api.db); err == nil {
				return &pluginBlockVal, nil
			} else {
				log.Warn("Error evoking GetBlockByNumber in plugin", "err", err.Error())
				return nil, err
			}
		}
	}

	if blockVal == nil {
		return nil, nil
	}
	return &blockVal, nil
}

var (
	gbbhHitMeter  = metrics.NewMinorMeter("/flume/gbbh/hit")
	gbbhMissMeter = metrics.NewMinorMeter("/flume/gbbh/miss")
)

func (api *BlockAPI) GetBlockByHash(ctx context.Context, blockHash types.Hash, includeTxns bool) (*map[string]interface{}, error) {

	if len(api.cfg.HeavyServer) > 0 && !blockDataPresent(blockHash, api.cfg, api.db) {
		log.Debug("eth_getBlockByHash sent to flume heavy")
		missMeter.Mark(1)
		gbbhMissMeter.Mark(1)
		responseShell, err := heavy.CallHeavy[map[string]interface{}](ctx, api.cfg.HeavyServer, "eth_getBlockByHash", blockHash, includeTxns)
		if err != nil {
			return nil, err
		}
		return responseShell, nil
	}

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("eth_getBlockByHash served from flume light")
		hitMeter.Mark(1)
		gbbhHitMeter.Mark(1)
	}

	pluginMethods := api.pl.Lookup("GetBlockByHash", func(v interface{}) bool {
		_, ok := v.(func(map[string]interface{}, *sql.DB) (map[string]interface{}, error))
		return ok
	})

	blocks, err := getBlocks(ctx, api.db, includeTxns, api.network, "hash = ?", trimPrefix(blockHash.Bytes()))
	if err != nil {
		return nil, err
	}
	var blockVal map[string]interface{}
	if len(blocks) > 0 {
		blockVal = blocks[0]
	
		for _, fni := range pluginMethods {
			fn := fni.(func(map[string]interface{}, *sql.DB) (map[string]interface{}, error))
			if pluginBlockVal, err := fn(blockVal, api.db); err == nil {
				return &pluginBlockVal, nil
			} else {
				log.Warn("Error evoking GetBlockByHash in plugin", "err", err.Error())
				return nil, err
			}
		}

	}

	if blockVal == nil {
		return nil, nil
	}

	return &blockVal, nil
}

var (
	gtcbnHitMeter  = metrics.NewMinorMeter("/flume/gtcbn/hit")
	gtcbnMissMeter = metrics.NewMinorMeter("/flume/gtcbn/miss")
)

func (api *BlockAPI) GetBlockTransactionCountByNumber(ctx context.Context, blockNumber plugins.BlockNumber) (hexutil.Uint64, error) {

	if len(api.cfg.HeavyServer) > 0 && !blockDataPresent(blockNumber, api.cfg, api.db) {
		log.Debug("eth_getBlockTransactionCountByNumber sent to flume heavy")
		missMeter.Mark(1)
		gtcbnMissMeter.Mark(1)
		count, err := heavy.CallHeavy[hexutil.Uint64](ctx, api.cfg.HeavyServer, "eth_getBlockTransactionCountByNumber", blockNumber)
		if err != nil {
			return 0, err
		}
		return *count, nil
	}

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("eth_getBlockTransactionCountByNumber served from flume light")
		hitMeter.Mark(1)
		gtcbhHitMeter.Mark(1)
	}

	var err error
	var count hexutil.Uint64

	if blockNumber.Int64() < 0 {
		latestBlock, err := getLatestBlock(ctx, api.db)
		if err != nil {
			return 0, err
		}
		blockNumber = plugins.BlockNumber(latestBlock)
	}

	count, err = txCount(ctx, api.db, "block = ?", blockNumber)
	if err != nil {
		return 0, err
	}
	return count, nil
}

var (
	gtcbhHitMeter  = metrics.NewMinorMeter("/flume/gtcbh/hit")
	gtcbhMissMeter = metrics.NewMinorMeter("/flume/gtcbh/miss")
)

func (api *BlockAPI) GetBlockTransactionCountByHash(ctx context.Context, blockHash types.Hash) (hexutil.Uint64, error) {

	if len(api.cfg.HeavyServer) > 0 && !blockDataPresent(blockHash, api.cfg, api.db) {
		log.Debug("eth_getBlockTransactionCountByHash sent to flume heavy")
		missMeter.Mark(1)
		gtcbhMissMeter.Mark(1)
		count, err := heavy.CallHeavy[hexutil.Uint64](ctx, api.cfg.HeavyServer, "eth_getBlockTransactionCountByHash", blockHash)
		if err != nil {
			return 0, err
		}
		return *count, nil
	}

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("eth_getBlockTransactionCountByHash served from flume light")
		hitMeter.Mark(1)
		gtcbhHitMeter.Mark(1)
	}

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

	count, err = txCount(ctx, api.db, "block = ?", plugins.BlockNumber(blockNumber))
	if err != nil {
		return 0, err
	}
	return count, nil
}

var (
	gucbnHitMeter  = metrics.NewMinorMeter("/flume/gucbn/hit")
	gucbnMissMeter = metrics.NewMinorMeter("/flume/gucbn/miss")
)

func (api *BlockAPI) GetUncleCountByBlockNumber(ctx context.Context, blockNumber plugins.BlockNumber) (hexutil.Uint64, error) {

	if len(api.cfg.HeavyServer) > 0 && !blockDataPresent(blockNumber, api.cfg, api.db) {
		log.Debug("eth_getetUncleCountByBlockNumber sent to flume heavy")
		missMeter.Mark(1)
		gucbnMissMeter.Mark(1)
		count, err := heavy.CallHeavy[hexutil.Uint64](ctx, api.cfg.HeavyServer, "eth_getUncleCountByBlockNumber", blockNumber)
		if err != nil {
			return 0, err
		}
		return *count, nil
	}

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("eth_getetUncleCountByBlockNumber served from flume light")
		hitMeter.Mark(1)
		gucbnHitMeter.Mark(1)
	}

	var uncles []byte
	unclesList := []types.Hash{}

	if blockNumber.Int64() < 0 {
		latestBlock, err := getLatestBlock(ctx, api.db)
		if err != nil {
			return 0, err
		}
		blockNumber = plugins.BlockNumber(latestBlock)
	}
	if err := api.db.QueryRowContext(ctx, "SELECT uncles FROM blocks WHERE number = ?", blockNumber).Scan(&uncles); err != nil {
		return 0, err
	}
	rlp.DecodeBytes(uncles, &unclesList)

	return hexutil.Uint64(len(unclesList)), nil

}

var (
	gucbhHitMeter  = metrics.NewMinorMeter("/flume/gucbh/hit")
	gucbhMissMeter = metrics.NewMinorMeter("/flume/gucbh/miss")
)

func (api *BlockAPI) GetUncleCountByBlockHash(ctx context.Context, blockHash types.Hash) (hexutil.Uint64, error) {

	if len(api.cfg.HeavyServer) > 0 && !blockDataPresent(blockHash, api.cfg, api.db) {
		log.Debug("eth_getUncleCountByBlockHash sent to flume heavy", "hash", blockHash)
		missMeter.Mark(1)
		gucbhMissMeter.Mark(1)
		count, err := heavy.CallHeavy[hexutil.Uint64](ctx, api.cfg.HeavyServer, "eth_getUncleCountByBlockHash", blockHash)
		if err != nil {
			return 0, err
		}
		return *count, nil
	}

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("eth_getUncleCountByBlockHash served from flume light")
		hitMeter.Mark(1)
		gucbhHitMeter.Mark(1)
	}

	var uncles []byte
	unclesList := []types.Hash{}

	if err := api.db.QueryRowContext(ctx, "SELECT uncles FROM blocks WHERE hash = ?", trimPrefix(blockHash.Bytes())).Scan(&uncles); err != nil {
		return 0, err
	}
	rlp.DecodeBytes(uncles, &unclesList)

	return hexutil.Uint64(len(unclesList)), nil
}
