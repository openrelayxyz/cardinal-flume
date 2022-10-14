package api

import (
	"context"
	"database/sql"
	
	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-types/metrics"
	"github.com/openrelayxyz/cardinal-evm/common"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-types/hexutil"
	"github.com/openrelayxyz/flume/plugins"
	"github.com/openrelayxyz/flume/heavy"
	"github.com/openrelayxyz/flume/config"
)

type FlumeAPI struct {
	db      *sql.DB
	network uint64
	pl      *plugins.PluginLoader
	cfg     *config.Config

}

func NewFlumeAPI(db *sql.DB, network uint64, pl *plugins.PluginLoader, cfg *config.Config) *FlumeAPI {
	return &FlumeAPI{
		db:      db,
		network: network,
		pl:      pl,
		cfg:     cfg,
	}
}

func (api *FlumeAPI) GetTransactionsBySender(ctx context.Context, address common.Address, offset *int) (*paginator[map[string]interface{}], error) {

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("get transactions by sender sent to flume heavy", "address", address)
		tx, err := heavy.CallHeavy[*paginator[map[string]interface{}]](ctx, api.cfg.HeavyServer, "flume_getTransactionsBySender", address, offset)
		if err != nil {
			return nil, err
		}
		return *tx, nil
	}

	if offset == nil {
		offset = new(int)
	}
	txs, err := getPendingTransactions(ctx, api.db, *offset, 1000, api.network, "sender = ?", trimPrefix(address.Bytes()))
	if err != nil {
		log.Error("Error getting pending txs", "err", err.Error())
		return nil, err
	}
	ctxs, err := getFlumeTransactions(ctx, api.db, *offset, 1000, api.network, "sender = ?", trimPrefix(address.Bytes()))
	if err != nil {
		log.Error("Error getting txs", "err", err.Error())
		return nil, err
	}
	txs = append(txs, ctxs...)
	result := paginator[map[string]interface{}]{Items: txs}
	if len(txs) >= 1000 {
		result.Token = *offset + len(txs)
	}
	return &result, nil
}

func (api *FlumeAPI) GetTransactionReceiptsBySender(ctx context.Context, address common.Address, offset *int) (*paginator[map[string]interface{}], error) {

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("get transaction receipts by sender sent to flume heavy", "address", address)
		rt, err := heavy.CallHeavy[*paginator[map[string]interface{}]](ctx, api.cfg.HeavyServer, "flume_getTransactionReceiptsBySender", address, offset)
		if err != nil {
			return nil, err
		}
		return *rt, nil
	}
	
	if offset == nil {
		offset = new(int)
	}
	receipts, err := getFlumeTransactionReceipts(ctx, api.db, *offset, 1000, api.network, "sender = ?", trimPrefix(address.Bytes()))
	if err != nil {
		log.Error("Error getting receipts", "err", err.Error())
		return nil, err
	}
	result := paginator[map[string]interface{}]{Items: receipts}
	if len(receipts) == 1000 {
		result.Token = *offset + len(receipts)
	}

	return &result, nil
}

func (api *FlumeAPI) GetTransactionsByRecipient(ctx context.Context, address common.Address, offset *int) (*paginator[map[string]interface{}], error) {

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("get transaction by recipeint sent to flume heavy", "address", address)
		tx, err := heavy.CallHeavy[*paginator[map[string]interface{}]](ctx, api.cfg.HeavyServer, "flume_getTransactionsByRecipient", address, offset)
		if err != nil {
			return nil, err
		}
		return *tx, nil
	}

	if offset == nil {
		offset = new(int)
	}
	txs, err := getPendingTransactions(ctx, api.db, *offset, 1000, api.network, "recipient = ?", trimPrefix(address.Bytes()))
	if err != nil {
		log.Error("Error getting pending txs", "err", err.Error())
		return nil, err
	}
	ctxs, err := getFlumeTransactions(ctx, api.db, *offset, 1000, api.network, "recipient = ?", trimPrefix(address.Bytes()))
	if err != nil {
		log.Error("Error getting txs", "err", err.Error())
		return nil, err
	}
	txs = append(txs, ctxs...)
	result := paginator[map[string]interface{}]{Items: txs}
	if len(txs) >= 1000 {
		result.Token = *offset + len(txs)
	}
	return &result, nil
}

func (api *FlumeAPI) GetTransactionReceiptsByRecipient(ctx context.Context, address common.Address, offset *int) (*paginator[map[string]interface{}], error) {

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("get transaction receipts by recipient sent to flume heavy", "address", address)
		tx, err := heavy.CallHeavy[*paginator[map[string]interface{}]](ctx, api.cfg.HeavyServer, "flume_getTransactionReceiptsByRecipient", address, offset)
		if err != nil {
			return nil, err
		}
		return *tx, nil
	}

	if offset == nil {
		offset = new(int)
	}
	receipts, err := getFlumeTransactionReceipts(ctx, api.db, *offset, 1000, api.network, "recipient = ?", trimPrefix(address.Bytes()))
	if err != nil {
		log.Error("Error getting receipts", "err", err.Error())
		return nil, err
	}
	result := paginator[map[string]interface{}]{Items: receipts}
	if len(receipts) == 1000 {
		result.Token = *offset + len(receipts)
	}
	return &result, nil
}

func (api *FlumeAPI) GetTransactionsByParticipant(ctx context.Context, address common.Address, offset *int) (*paginator[map[string]interface{}], error) {

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("get transaction by participant sent to flume heavy", "address", address)
		tx, err := heavy.CallHeavy[*paginator[map[string]interface{}]](ctx, api.cfg.HeavyServer, "flume_getTransactionsByParticipant", address, offset)
		if err != nil {
			return nil, err
		}
		return *tx, nil
	}
	
	if offset == nil {
		offset = new(int)
	}
	txs, err := getPendingTransactions(ctx, api.db, *offset, 1000, api.network, "sender = ? OR recipient = ?", trimPrefix(address.Bytes()), trimPrefix(address.Bytes()))
	if err != nil {
		log.Error("Error getting pending txs", "err", err.Error())
		return nil, err
	}
	ctxs, err := getFlumeTransactions(ctx, api.db, *offset, 1000, api.network, "sender = ? OR recipient = ?", trimPrefix(address.Bytes()), trimPrefix(address.Bytes()))
	if err != nil {
		log.Error("Error getting txs", "err", err.Error())
		return nil, err
	}
	txs = append(txs, ctxs...)
	result := paginator[map[string]interface{}]{Items: txs}
	if len(txs) >= 1000 {
		result.Token = *offset + len(txs)
	}

	return &result, nil
}

func (api *FlumeAPI) GetTransactionReceiptsByParticipant(ctx context.Context, address common.Address, offset *int) (*paginator[map[string]interface{}], error) {

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("get transaction receipts by participant sent to flume heavy", "address", address)
		rt, err := heavy.CallHeavy[*paginator[map[string]interface{}]](ctx, api.cfg.HeavyServer, "flume_getTransactionReceiptsByParticipant", address, offset)
		if err != nil {
			return nil, err
		}
		return *rt, nil
	}
	
	if offset == nil {
		offset = new(int)
	}
	receipts, err := getFlumeTransactionReceipts(ctx, api.db, *offset, 1000, api.network, "sender = ? OR recipient = ?", trimPrefix(address.Bytes()), trimPrefix(address.Bytes()))
	if err != nil {
		log.Error("Error getting receipts", "err", err.Error())
		return nil, err
	}
	result := paginator[map[string]interface{}]{Items: receipts}
	if len(receipts) == 1000 {
		result.Token = *offset + len(receipts)
	}

	return &result, nil
}

var (
	gtrbhHitMeter = metrics.NewMinorMeter("/flume/gtrbh/hit")
	gtrbhMissMeter = metrics.NewMinorMeter("/flume/gtrbh/miss")
)

func (api *FlumeAPI) GetTransactionReceiptsByBlockHash(ctx context.Context, blockHash types.Hash) ([]map[string]interface{}, error) {

	if len(api.cfg.HeavyServer) > 0 && !blockDataPresent(blockHash, api.cfg, api.db) {
		log.Debug("transaction receipts by block hash sent to flume heavy", "hash", blockHash)
		missMeter.Mark(1)
		gtrbhMissMeter.Mark(1)
		rt, err := heavy.CallHeavy[[]map[string]interface{}](ctx, api.cfg.HeavyServer, "eth_getTransactionReceiptsByBlockHash", blockHash)
		if err != nil {
			return nil, err
		}
		return *rt, nil 
	}

	log.Debug("transaction receipts by block hash processed in light server", "hash", blockHash)
	hitMeter.Mark(1)
	gtrbhHitMeter.Mark(1)

	receipts, err := getFlumeTransactionReceiptsBlock(ctx, api.db, 0, 100000, api.network, "blocks.hash = ?", trimPrefix(blockHash.Bytes()))
	if err != nil {
		log.Error("Error getting receipts", "err", err.Error())
		return nil, err
	}
	return receipts, nil
}

var (
	gtrbnHitMeter = metrics.NewMinorMeter("/flume/gtrbn/hit")
	gtrbnMissMeter = metrics.NewMinorMeter("/flume/gtrbn/miss")
)

func (api *FlumeAPI) GetTransactionReceiptsByBlockNumber(ctx context.Context, blockNumber hexutil.Uint64) ([]map[string]interface{}, error) {

	if len(api.cfg.HeavyServer) > 0 && !blockDataPresent(blockNumber, api.cfg, api.db) {
		log.Debug("transaction receipts by block number sent to flume heavy", "number", blockNumber)
		missMeter.Mark(1)
		gtrbnMissMeter.Mark(1)
		rt, err := heavy.CallHeavy[[]map[string]interface{}](ctx, api.cfg.HeavyServer, "eth_getTransactionReceiptsByBlockNumber", blockNumber)
		if err != nil {
			return nil, err
		}
		return *rt, nil 
	}

	log.Debug("transaction receipts by block number processed in light server", "number", blockNumber)
	hitMeter.Mark(1)
	gtrbnHitMeter.Mark(1)

	receipts, err := getFlumeTransactionReceiptsBlock(ctx, api.db, 0, 100000, api.network, "block = ?", uint64(blockNumber))
	if err != nil {
		log.Error("Error getting receipts", "err", err.Error())
		return nil, err
	}
	return receipts, nil
}

