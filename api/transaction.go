package api

import (
	"context"
	"database/sql"

	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-evm/common"
	"github.com/openrelayxyz/cardinal-evm/vm"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-types/hexutil"

	"github.com/openrelayxyz/flume/config"
	"github.com/openrelayxyz/flume/plugins"
)

type TransactionAPI struct {
	db      *sql.DB
	network uint64
	pl      *plugins.PluginLoader
	cfg		*config.Config
}

func NewTransactionAPI(db *sql.DB, network uint64, pl *plugins.PluginLoader, cfg *config.Config) *TransactionAPI {
	return &TransactionAPI{
		db:      db,
		network: network,
		pl:      pl,
		cfg:     cfg,
	}
}

func (api *TransactionAPI) GetTransactionByHash(ctx context.Context, txHash types.Hash) (map[string]interface{}, error) {

	if cfg.LightServer && txDataPresent(txHash, api.cfg, api.db) {
		log.Info("send to flume heavy")
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
			return pluginResult, nil
		} else {
			log.Warn("Error evoking GetTransactionByhash in plugin", "err", err.Error())
			return nil, err
		}
	}

	return result, nil
}

func (api *TransactionAPI) GetTransactionByBlockHashAndIndex(ctx context.Context, blockHash types.Hash, index hexutil.Uint64) (map[string]interface{}, error) {

	if cfg.LightServer && blockDataPresent(blockHash, api.cfg, api.db) {
		log.Info("send to flume heavy")
	}

	var err error
	txs, err := getTransactionsBlock(ctx, api.db, 0, 1, api.network, "blocks.hash = ? AND transactionIndex = ?", trimPrefix(blockHash.Bytes()), uint64(index))
	if err != nil {
		return nil, err
	}
	result := returnSingleTransaction(txs)

	return result, nil
}

func (api *TransactionAPI) GetTransactionByBlockNumberAndIndex(ctx context.Context, blockNumber vm.BlockNumber, index hexutil.Uint64) (map[string]interface{}, error) {
	
	if cfg.LightServer && blockDataPresent(blockNumber, api.cfg, api.db) {
		log.Info("send to flume heavy")
	}
	
	if blockNumber.Int64() < 0 {
		latestBlock, err := getLatestBlock(ctx, api.db)
		if err != nil {
			return nil, err
		}
		blockNumber = vm.BlockNumber(latestBlock)
	}

	txs, err := getTransactionsBlock(ctx, api.db, 0, 1, api.network, "block = ? AND transactionIndex = ?", uint64(blockNumber), uint64(index))
	if err != nil {
		return nil, err
	}

	result := returnSingleTransaction(txs)

	return result, nil
}

func (api *TransactionAPI) GetTransactionReceipt(ctx context.Context, txHash types.Hash) (map[string]interface{}, error) {

	if cfg.LightServer && txDataPresent(txHash, api.cfg, api.db) {
		log.Info("send to flume heavy")
	}

	pluginMethods := api.pl.Lookup("GetTransactionReceipt", func(v interface{}) bool {
		_, ok := v.(func(map[string]interface{}, *sql.DB, types.Hash) (map[string]interface{}, error))
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
			return pluginResult, nil
		} else {
			log.Warn("Error evoking GetTransactionReceipt in plugin", "err", err.Error())
			return nil, err
		}
	}

	return result, nil
}

func (api *TransactionAPI) GetTransactionCount(ctx context.Context, addr common.Address) (hexutil.Uint64, error) {
	nonce, err := getSenderNonce(ctx, api.db, addr)
	if err != nil {
		return 0, err
	}

	return nonce, nil
}
