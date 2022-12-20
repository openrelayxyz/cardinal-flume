package api

import (
	"context"
	"database/sql"

	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-evm/common"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-types/hexutil"
	"github.com/openrelayxyz/cardinal-types/metrics"

	"github.com/openrelayxyz/cardinal-flume/config"
	"github.com/openrelayxyz/cardinal-flume/heavy"
	"github.com/openrelayxyz/cardinal-flume/plugins"
)

type TransactionAPI struct {
	db      *sql.DB
	network uint64
	pl      *plugins.PluginLoader
	cfg     *config.Config
}

func NewTransactionAPI(db *sql.DB, network uint64, pl *plugins.PluginLoader, cfg *config.Config) *TransactionAPI {
	return &TransactionAPI{
		db:      db,
		network: network,
		pl:      pl,
		cfg:     cfg,
	}
}

var (
	gtbhHitMeter  = metrics.NewMinorMeter("/flume/gtbh/hit")
	gtbhMissMeter = metrics.NewMinorMeter("/flume/gtbh/miss")
)

func (api *TransactionAPI) GetTransactionByHash(ctx context.Context, txHash types.Hash) (*map[string]interface{}, error) {

	if len(api.cfg.HeavyServer) > 0 && !txDataPresent(txHash, api.cfg, api.db) {
		log.Debug("eth_getTransactionByHash sent to flume heavy")
		missMeter.Mark(1)
		gtbhMissMeter.Mark(1)
		responseShell, err := heavy.CallHeavy[map[string]interface{}](ctx, api.cfg.HeavyServer, "eth_getTransactionByHash", txHash)
		if err != nil {
			return nil, err
		}
		return responseShell, nil
	}

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("eth_getTransactionByHash servered from flume light")
		hitMeter.Mark(1)
		gtbhHitMeter.Mark(1)
	}

	pluginMethods := api.pl.Lookup("GetTransactionByHash", func(v interface{}) bool {
		_, ok := v.(func(map[string]interface{}, types.Hash, *sql.DB) (map[string]interface{}, error))
		return ok
	})

	var err error
	txs, err := getTransactionsBlock(ctx, api.db, 0, 1, api.network, "transactions.hash = ?", trimPrefix(txHash.Bytes()))
	if err != nil {
		return nil, err
	}
	if len(txs) == 0 {
		txs, err = getPendingTransactions(ctx, api.db, 0, 1, api.network, "transactions.hash = ?", trimPrefix(txHash.Bytes()))
	}
	if err != nil {
		return nil, err
	}

	result := returnSingleTransaction(txs)

	for _, fni := range pluginMethods {
		fn := fni.(func(map[string]interface{}, types.Hash, *sql.DB) (map[string]interface{}, error))
		if pluginResult, err := fn(result, txHash, api.db); err == nil {
			return &pluginResult, nil
		} else {
			log.Warn("Error evoking GetTransactionByhash in plugin", "err", err.Error())
			return nil, err
		}
	}

	return &result, nil
}

var (
	gtbhiHitMeter  = metrics.NewMinorMeter("/flume/gtbhi/hit")
	gtbhiMissMeter = metrics.NewMinorMeter("/flume/gtbhi/miss")
)

func (api *TransactionAPI) GetTransactionByBlockHashAndIndex(ctx context.Context, blockHash types.Hash, index hexutil.Uint64) (*map[string]interface{}, error) {

	if len(api.cfg.HeavyServer) > 0 && !blockDataPresent(blockHash, api.cfg, api.db) {
		log.Debug("eth_getTransactionByBlockHashAndIndex sent to flume heavy")
		missMeter.Mark(1)
		gtbhiMissMeter.Mark(1)
		responseShell, err := heavy.CallHeavy[map[string]interface{}](ctx, api.cfg.HeavyServer, "eth_getTransactionByBlockHashAndIndex", blockHash, index)
		if err != nil {
			return nil, err
		}
		return responseShell, nil
	}

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("eth_getTransactionByBlockHashAndIndex servered from flume light")
		hitMeter.Mark(1)
		gtbhiHitMeter.Mark(1)
	}

	var err error
	txs, err := getTransactionsBlock(ctx, api.db, 0, 1, api.network, "blocks.hash = ? AND transactionIndex = ?", trimPrefix(blockHash.Bytes()), uint64(index))
	if err != nil {
		return nil, err
	}
	result := returnSingleTransaction(txs)

	return &result, nil
}

var (
	gtbniHitMeter  = metrics.NewMinorMeter("/flume/gtbni/hit")
	gtbniMissMeter = metrics.NewMinorMeter("/flume/gtbni/miss")
)

func (api *TransactionAPI) GetTransactionByBlockNumberAndIndex(ctx context.Context, blockNumber plugins.BlockNumber, index hexutil.Uint64) (*map[string]interface{}, error) {

	if len(api.cfg.HeavyServer) > 0 && !blockDataPresent(blockNumber, api.cfg, api.db) {
		log.Debug("eth_getTransactionByBlockNumberAndIndex sent to flume heavy")
		missMeter.Mark(1)
		gtbniMissMeter.Mark(1)
		responseShell, err := heavy.CallHeavy[map[string]interface{}](ctx, api.cfg.HeavyServer, "eth_getTransactionByBlockNumberAndIndex", blockNumber, index)
		if err != nil {
			return nil, err
		}
		return responseShell, nil
	}

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("eth_getTransactionByBlockNumberAndIndex served from flume light")
		hitMeter.Mark(1)
		gtbniHitMeter.Mark(1)
	}

	if blockNumber.Int64() < 0 {
		latestBlock, err := getLatestBlock(ctx, api.db)
		if err != nil {
			return nil, err
		}
		blockNumber = plugins.BlockNumber(latestBlock)
	}

	txs, err := getTransactionsBlock(ctx, api.db, 0, 1, api.network, "block = ? AND transactionIndex = ?", uint64(blockNumber), uint64(index))
	if err != nil {
		return nil, err
	}

	result := returnSingleTransaction(txs)

	return &result, nil
}

var (
	gtrcHitMeter  = metrics.NewMinorMeter("/flume/gtrc/hit")
	gtrcMissMeter = metrics.NewMinorMeter("/flume/gtrc/miss")
)

func (api *TransactionAPI) GetTransactionReceipt(ctx context.Context, txHash types.Hash) (*map[string]interface{}, error) {
	if len(api.cfg.HeavyServer) > 0 && !txDataPresent(txHash, api.cfg, api.db) {
		log.Debug("eth_getTransactionReceipt sent to flume heavy")
		missMeter.Mark(1)
		gtrcMissMeter.Mark(1)
		responseShell, err := heavy.CallHeavy[map[string]interface{}](ctx, api.cfg.HeavyServer, "eth_getTransactionReceipt", txHash)
		if err != nil {
			return nil, err
		}
		return responseShell, nil
	}

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("eth_getTransactionReceipt served from flume light")
		hitMeter.Mark(1)
		gtrcHitMeter.Mark(1)
	}

	pluginMethods := api.pl.Lookup("GetTransactionReceipt", func(v interface{}) bool {
		_, ok := v.(func(map[string]interface{}, types.Hash, *sql.DB) (map[string]interface{}, error))
		return ok
	})

	var err error
	receipts, err := getTransactionReceiptsBlock(ctx, api.db, 0, 1, api.network, "transactions.hash = ?", trimPrefix(txHash.Bytes()))
	if err != nil {
		return nil, err
	}
	result := returnSingleReceipt(receipts)

	for _, fni := range pluginMethods {
		fn := fni.(func(map[string]interface{}, types.Hash, *sql.DB) (map[string]interface{}, error))
		if pluginResult, err := fn(result, txHash, api.db); err == nil {
			return &pluginResult, nil
		} else {
			log.Warn("Error evoking GetTransactionReceipt in plugin", "err", err.Error())
			return nil, err
		}
	}

	return &result, nil
}

func (api *TransactionAPI) GetTransactionCount(ctx context.Context, addr common.Address) (hexutil.Uint64, error) {

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("eth_getTransactionCount sent to flume heavy by default")
		missMeter.Mark(1)
		count, err := heavy.CallHeavy[hexutil.Uint64](ctx, api.cfg.HeavyServer, "eth_getTransactionCount", addr)
		if err != nil {
			return 0, err
		}
		return *count, nil
	}

	nonce, err := getSenderNonce(ctx, api.db, addr)
	if err != nil {
		return 0, err
	}

	return nonce, nil
}
