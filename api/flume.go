package api

import (
	"context"
	"database/sql"
	"runtime"

	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-evm/common"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-rpc"
	"github.com/openrelayxyz/cardinal-types/metrics"
	"github.com/openrelayxyz/cardinal-flume/build"
	"github.com/openrelayxyz/cardinal-flume/config"
	"github.com/openrelayxyz/cardinal-flume/heavy"
	"github.com/openrelayxyz/cardinal-flume/plugins"
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


func (api *FlumeAPI) ClientVersion(ctx context.Context) string {
	version := build.Version
	if version == "" {
		version = "unset-use-make-to-build"
	}
	name := "Cardinal-Flume"
	name += "/" + version
	name += "/" + runtime.GOOS + "-" + runtime.GOARCH
	name += "/" + runtime.Version()
	return name
}

func (api *FlumeAPI) HeavyClientVersion(ctx context.Context) (string, error) {
	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("flume_heavyClientVersion sent to flume heavy by default")
		name, err := heavy.CallHeavy[string](ctx, api.cfg.HeavyServer, "flume_clientVersion")
		if err != nil {
			return "", err
		}
		return *name, nil
	}
	name := api.ClientVersion(ctx)
	return name, nil
}


func (api *FlumeAPI) GetTransactionsBySender(ctx context.Context, address common.Address, offset *int) (*paginator[map[string]interface{}], error) {

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("flume_getTransactionsBySender sent to flume heavy by default")
		missMeter.Mark(1)
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
		log.Debug("flume_getTransactionReceiptsBySender sent to flume heavy by default")
		missMeter.Mark(1)
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
		log.Debug("flume_getTransactionsByRecipient sent to flume heavy by default")
		missMeter.Mark(1)
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
		log.Debug("flume_getTransactionReceiptsByRecipient sent to flume heavy by default")
		missMeter.Mark(1)
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
		log.Debug("flume_getTransactionByParticipant sent to flume heavy by default")
		missMeter.Mark(1)
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
		log.Debug("flume_getTransactionReceiptsByParticipant sent to flume heavy by default")
		missMeter.Mark(1)
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
	gtrbhHitMeter  = metrics.NewMinorMeter("/flume/gtrbh/hit")
	gtrbhMissMeter = metrics.NewMinorMeter("/flume/gtrbh/miss")
)

func (api *FlumeAPI) GetTransactionReceiptsByBlockHash(ctx context.Context, blockHash types.Hash) ([]map[string]interface{}, error) {

	if len(api.cfg.HeavyServer) > 0 && !blockDataPresent(blockHash, api.cfg, api.db) {
		log.Debug("flume_getTransactionReceiptsByBlockHash sent to flume heavy")
		missMeter.Mark(1)
		missMeter.Mark(1)
		gtrbhMissMeter.Mark(1)
		rt, err := heavy.CallHeavy[[]map[string]interface{}](ctx, api.cfg.HeavyServer, "flume_getTransactionReceiptsByBlockHash", blockHash)
		if err != nil {
			return nil, err
		}
		return *rt, nil
	}

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("flume_getTransactionReceiptsByBlockHash served from flume light")
		hitMeter.Mark(1)
		gtrbhHitMeter.Mark(1)
	}

	receipts, err := getFlumeTransactionReceiptsBlock(ctx, api.db, 0, 100000, api.network, "blocks.hash = ?", trimPrefix(blockHash.Bytes()))
	if err != nil {
		log.Error("Error getting receipts", "err", err.Error())
		return nil, err
	}
	return receipts, nil
}

var (
	gtrbnHitMeter  = metrics.NewMinorMeter("/flume/gtrbn/hit")
	gtrbnMissMeter = metrics.NewMinorMeter("/flume/gtrbn/miss")
)

func (api *FlumeAPI) GetTransactionReceiptsByBlockNumber(ctx context.Context, blockNumber rpc.BlockNumber) ([]map[string]interface{}, error) {

	if len(api.cfg.HeavyServer) > 0 && !blockDataPresent(blockNumber, api.cfg, api.db) {
		log.Debug("flume_getTransactionReceiptsByBlockNumber sent to flume heavy")
		missMeter.Mark(1)
		gtrbnMissMeter.Mark(1)
		rt, err := heavy.CallHeavy[[]map[string]interface{}](ctx, api.cfg.HeavyServer, "flume_getTransactionReceiptsByBlockNumber", blockNumber)
		if err != nil {
			return nil, err
		}
		return *rt, nil
	}


	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("flume_getTransactionReceiptsByBlockNumber served from flume light")
		hitMeter.Mark(1)
		gtrbnHitMeter.Mark(1)
	}

	if int64(blockNumber) < 0 {
		latestBlock, err := getLatestBlock(ctx, api.db)
		if err != nil {
			return nil, err
		}
		blockNumber = rpc.BlockNumber(latestBlock)
	}

	receipts, err := getFlumeTransactionReceiptsBlock(ctx, api.db, 0, 100000, api.network, "block = ?", uint64(blockNumber))
	if err != nil {
		log.Error("Error getting receipts", "err", err.Error())
		return nil, err
	}
	return receipts, nil
}

var (
	gbthHitMeter  = metrics.NewMinorMeter("/flume/gbth/hit")
	gbthMissMeter = metrics.NewMinorMeter("/flume/gbth/miss")
)

func (api *FlumeAPI) GetBlockByTransactionHash(ctx context.Context, txHash types.Hash) (*map[string]interface{}, error) {
	// this function is not included in the api test but needs to be. 
	
	if len(api.cfg.HeavyServer) > 0 && !txDataPresent(txHash, api.cfg, api.db) {
		log.Debug("eth_getBlockByTransactionHash sent to flume heavy")
		missMeter.Mark(1)
		gbthMissMeter.Mark(1)
		responseShell, err := heavy.CallHeavy[map[string]interface{}](ctx, api.cfg.HeavyServer, "flume_getBlockByTransactionHash", txHash)
		if err != nil {
			return nil, err
		}
		return responseShell, nil
	}

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("flume_getBlockByTransactionHash served from flume light")
		hitMeter.Mark(1)
		gbthHitMeter.Mark(1)
	}

	var blockNumber int64
	if err := api.db.QueryRowContext(ctx, "SELECT block FROM transactions.transactions WHERE hash = ?;", txHash).Scan(&blockNumber); err != nil {
		log.Error("GetBlockByTransactionHash returned an error", "err", err.Error())
	}

	blocks, err := getBlocks(ctx, api.db, true, api.network, "number = ?", blockNumber)
	if err != nil {
		return nil, err
	}
	var blockVal *map[string]interface{}
	if len(blocks) > 0 {
		blockVal = &blocks[0]
	}

	return blockVal, nil
}
