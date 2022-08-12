package api

import (
	"context"
	"database/sql"

	// log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-evm/common"
	"github.com/openrelayxyz/cardinal-evm/vm"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-types/hexutil"

	"github.com/openrelayxyz/flume/plugins"
)

type TransactionAPI struct {
	db      *sql.DB
	network uint64
	pl      *plugins.PluginLoader
}

func NewTransactionAPI(db *sql.DB, network uint64, pl *plugins.PluginLoader) *TransactionAPI {
	return &TransactionAPI{
		db:      db,
		network: network,
		pl:      pl,
	}
}

func (api *TransactionAPI) GetTransactionByHash(ctx context.Context, txHash types.Hash) (map[string]interface{}, error) {

	pluginMethods := api.pl.Lookup("GetTransactionByHash", func(v interface{}) bool {
		_, ok := v.(func(types.Hash, *sql.DB) (map[string]interface{}, error))
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
		fn := fni.(func(types.Hash, *sql.DB) (map[string]interface{}, error))
		if result, err = fn(txHash, api.db); err != nil {
			return nil, err
		}
	}

	return result, nil
}

func (api *TransactionAPI) GetTransactionByBlockHashAndIndex(ctx context.Context, blockHash types.Hash, index hexutil.Uint64) (map[string]interface{}, error) {
	var err error
	txs, err := getTransactionsBlock(ctx, api.db, 0, 1, api.network, "blocks.hash = ? AND transactionIndex = ?", trimPrefix(blockHash.Bytes()), uint64(index))
	if err != nil {
		return nil, err
	}
	result := returnSingleTransaction(txs)

	return result, nil
}

func (api *TransactionAPI) GetTransactionByBlockNumberAndIndex(ctx context.Context, blockNumber vm.BlockNumber, index hexutil.Uint64) (map[string]interface{}, error) {
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

	pluginMethods := api.pl.Lookup("GetTransactionReceipt", func(v interface{}) bool {
		_, ok := v.(func(types.Hash, *sql.DB) (map[string]interface{}, error))
		return ok
	})

	var err error
	receipts, err := getTransactionReceiptsBlock(ctx, api.db, 0, 1, api.network, "transactions.hash = ?", trimPrefix(txHash.Bytes()))
	if err != nil {
		return nil, err
	}
	result := returnSingleReceipt(receipts)

	for _, fni := range pluginMethods {
		fn := fni.(func(types.Hash, *sql.DB) (map[string]interface{}, error))
		if result, err = fn(txHash, api.db); err != nil {
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
