package api

import (
	"context"
	"database/sql"
	"runtime"
	"encoding/hex"

	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-evm/common"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-rpc"
	"github.com/openrelayxyz/cardinal-types/hexutil"
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
	mempool bool
}

func NewFlumeAPI(db *sql.DB, network uint64, pl *plugins.PluginLoader, cfg *config.Config, mempool bool) *FlumeAPI {
	if !mempool {
		log.Warn("Flume API initiated without mempool database")
	}
	return &FlumeAPI{
		db:      db,
		network: network,
		pl:      pl,
		cfg:     cfg,
		mempool: mempool,
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

func (api *FlumeAPI) GetTransactionsBySender(ctx *rpc.CallContext, address common.Address, offset *int) (*paginator[map[string]interface{}], error) {

	heavyResult := make(chan *paginator[map[string]interface{}])
	errChan := make(chan error)

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("flume_getTransactionsBySender sent to flume heavy by default")
		missMeter.Mark(1)
		go func() {
			tx, err := heavy.CallHeavyDiscrete[*paginator[map[string]interface{}]](ctx.Context(), api.cfg.HeavyServer, api.cfg.EarliestBlock, "flume_getTransactionsBySender", address, offset)
			if err != nil {
				log.Error("Error processing request in flume_getTransactionsBySender", "err", err) 
				errChan <- err
			}
			heavyResult <- *tx
		}()
	} else {
		close(heavyResult)
	}

	if offset == nil {
		offset = new(int)
	}

	var ctxs []map[string]interface{}
	var err error
	if ctx.Latest > 0 {
		ctxs, err = getFlumeTransactions(ctx.Context(), api.db, *offset, 1000, api.network, "sender = ? AND block < ?", trimPrefix(address.Bytes()), ctx.Latest)
	} else {
		ctxs, err = getFlumeTransactions(ctx.Context(), api.db, *offset, 1000, api.network, "sender = ?", trimPrefix(address.Bytes()))
	}
	if err != nil {
		exhaustChannels[*paginator[map[string]interface{}]](heavyResult, errChan)
		log.Error("Error getting txs, getTransactionsBySender", "err", err)
		return nil, err
	}
	
	txs, err := getPendingTransactions(ctx.Context(), api.db, api.mempool, *offset, 1000, api.network, "sender = ?", trimPrefix(address.Bytes()))
	if err != nil {
		exhaustChannels[*paginator[map[string]interface{}]](heavyResult, errChan)
		log.Error("Error getting pending txs, getTransactionsBySender", "err", err)
		return nil, err
	}

	ctxs = append(ctxs, txs...)

	result := paginator[map[string]interface{}]{Items: ctxs}
	if len(ctxs) >= 1000 {
		result.Token = *offset + len(ctxs)
	}

	select {
	case hr, ok := <- heavyResult:
		if ok {
			result.Items = append(hr.Items, result.Items...)
			if result.Token == nil {
				result.Token = hr.Token
			}
		}
	case err := <- errChan:
		return nil, err
	}
	
	return &result, nil
}

func (api *FlumeAPI) GetTransactionReceiptsBySender(ctx *rpc.CallContext, address common.Address, offset *int) (*paginator[map[string]interface{}], error) {

	heavyResult := make(chan *paginator[map[string]interface{}])
	errChan := make(chan error)
	
	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("flume_getTransactionReceiptsBySender sent to flume heavy by default")
		missMeter.Mark(1)
		go func() {
			rt, err := heavy.CallHeavyDiscrete[*paginator[map[string]interface{}]](ctx.Context(), api.cfg.HeavyServer, api.cfg.EarliestBlock, "flume_getTransactionReceiptsBySender", address, offset)
			if err != nil {
				log.Error("Error processing request in flume_getTransactionReceiptsBySender", "err", err)
				errChan <- err
			}
			heavyResult <- *rt
		}()
	} else {
		close(heavyResult)
	}

	if offset == nil {
		offset = new(int)
	}

	var receipts []map[string]interface{}
	var err error

	if ctx.Latest > 0 {
		receipts, err = getFlumeTransactionReceipts(ctx.Context(), api.db, *offset, 1000, api.network, "sender = ? AND block < ?", trimPrefix(address.Bytes()), ctx.Latest)
	} else {
		receipts, err = getFlumeTransactionReceipts(ctx.Context(), api.db, *offset, 1000, api.network, "sender = ?", trimPrefix(address.Bytes()))
	}
	
	if err != nil {
		exhaustChannels[*paginator[map[string]interface{}]](heavyResult, errChan)
		log.Error("Error getting receipts, getTransactionReceiptsBySender", "err", err.Error())
		return nil, err
	}
	
	result := paginator[map[string]interface{}]{Items: receipts}
	if len(receipts) >= 1000 {
		result.Token = *offset + len(receipts)
	}

	select {
	case hr, ok := <- heavyResult:
		if ok {
			result.Items = append(hr.Items, result.Items...)
			if result.Token == nil {
				result.Token = hr.Token
			}
		}
	case err := <- errChan:
		return nil, err
	}

	return &result, nil
}

func (api *FlumeAPI) GetTransactionsByRecipient(ctx *rpc.CallContext, address common.Address, offset *int) (*paginator[map[string]interface{}], error) {

	heavyResult := make(chan *paginator[map[string]interface{}])
	errChan := make(chan error)

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("flume_getTransactionsByRecipient sent to flume heavy by default")
		missMeter.Mark(1)
		go func() {
			tx, err := heavy.CallHeavyDiscrete[*paginator[map[string]interface{}]](ctx.Context(), api.cfg.HeavyServer, api.cfg.EarliestBlock, "flume_getTransactionsByRecipient", address, offset)
			if err != nil {
				log.Error("Error processing request in flume_getTransactionsByRecipient", "err", err)
				errChan <- err
			}
			heavyResult <- *tx
		}()
	} else {
		close(heavyResult)
	}

	if offset == nil {
		offset = new(int)
	}

	var ctxs []map[string]interface{}
	var err error
	if ctx.Latest > 0 {
		ctxs, err = getFlumeTransactions(ctx.Context(), api.db, *offset, 1000, api.network, "recipient = ? AND block < ?", trimPrefix(address.Bytes()), ctx.Latest)
	} else {
		ctxs, err = getFlumeTransactions(ctx.Context(), api.db, *offset, 1000, api.network, "recipient = ?", trimPrefix(address.Bytes()))
	}
	if err != nil {
		exhaustChannels[*paginator[map[string]interface{}]](heavyResult, errChan)
		log.Error("Error getting txs in GetTransactionsByRecipient", "err", err.Error())
		return nil, err
	}

	txs, err := getPendingTransactions(ctx.Context(), api.db, api.mempool, *offset, 1000, api.network, "recipient = ?", trimPrefix(address.Bytes()))
	if err != nil {
		exhaustChannels[*paginator[map[string]interface{}]](heavyResult, errChan)
		log.Error("Error getting pending txs, GetTransactionsByRecipient", "err", err.Error())
		return nil, err
	}
	ctxs = append(ctxs, txs...)
	
	result := paginator[map[string]interface{}]{Items: ctxs}
	if len(ctxs) >= 1000 {
		result.Token = *offset + len(ctxs)
	}

	select {
	case hr, ok := <- heavyResult:
		if ok {
			result.Items = append(hr.Items, result.Items...)
			if result.Token == nil {
				result.Token = hr.Token
			}
		}
	case err := <- errChan:
		return nil, err
	}

	return &result, nil
}

func (api *FlumeAPI) GetTransactionReceiptsByRecipient(ctx *rpc.CallContext, address common.Address, offset *int) (*paginator[map[string]interface{}], error) {


	heavyResult := make(chan *paginator[map[string]interface{}])
	errChan := make(chan error)

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("flume_getTransactionReceiptsByRecipient sent to flume heavy by default")
		missMeter.Mark(1)
		go func() {
			rt, err := heavy.CallHeavyDiscrete[*paginator[map[string]interface{}]](ctx.Context(), api.cfg.HeavyServer, api.cfg.EarliestBlock, "flume_getTransactionReceiptsByRecipient", address, offset)
			if err != nil {
				log.Error("Error processing request in flume_getTransactionReceiptsByRecipient", "err", err) 
				errChan <- err
			}
			heavyResult <- *rt
		}()
	} else {
		close(heavyResult)
	}

	if offset == nil {
		offset = new(int)
	}

	var receipts []map[string]interface{}
	var err error

	if ctx.Latest > 0 {
		receipts, err = getFlumeTransactionReceipts(ctx.Context(), api.db, *offset, 1000, api.network, "recipient = ? AND block < ?", trimPrefix(address.Bytes()), ctx.Latest)
	} else {
		receipts, err = getFlumeTransactionReceipts(ctx.Context(), api.db, *offset, 1000, api.network, "recipient = ?", trimPrefix(address.Bytes()))
	}

	if err != nil {
		exhaustChannels[*paginator[map[string]interface{}]](heavyResult, errChan)
		log.Error("Error getting receipts in getTransactionReceiptsByRecipient", "err", err.Error())
		return nil, err
	}

	result := paginator[map[string]interface{}]{Items: receipts}
	if len(receipts) == 1000 {
		result.Token = *offset + len(receipts)
	}

	select {
	case hr, ok := <- heavyResult:
		if ok {
			result.Items = append(hr.Items, result.Items...)
			if result.Token == nil {
				result.Token = hr.Token
			}
		}
	case err := <- errChan:
		return nil, err
	}

	return &result, nil
}

func (api *FlumeAPI) GetTransactionsByParticipant(ctx *rpc.CallContext, address common.Address, offset *int) (*paginator[map[string]interface{}], error) {

	heavyResult := make(chan *paginator[map[string]interface{}])
	errChan := make(chan error)

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("flume_getTransactionByParticipant sent to flume heavy by default")
		missMeter.Mark(1)
		go func() {
			tx, err := heavy.CallHeavyDiscrete[*paginator[map[string]interface{}]](ctx.Context(), api.cfg.HeavyServer, api.cfg.EarliestBlock, "flume_getTransactionsByParticipant", address, offset)
			if err != nil {
				log.Error("Error processing request in flume_getTransactionByParticipant", "err", err) 
				errChan <- err
			}
			heavyResult <- *tx
		}()
	} else {
		close(heavyResult)
	}

	if offset == nil {
		offset = new(int)
	}

	var ctxs []map[string]interface{}
	var err error

	if ctx.Latest > 0 {
		ctxs, err = getFlumeTransactions(ctx.Context(), api.db, *offset, 1000, api.network, "(sender = ? OR recipient = ?) AND block < ?", trimPrefix(address.Bytes()), trimPrefix(address.Bytes()), ctx.Latest)
	} else {
		ctxs, err = getFlumeTransactions(ctx.Context(), api.db, *offset, 1000, api.network, "sender = ? OR recipient = ?", trimPrefix(address.Bytes()), trimPrefix(address.Bytes()))
	}
	if err != nil {
		exhaustChannels[*paginator[map[string]interface{}]](heavyResult, errChan)
		log.Error("Error getting txs in getTransactionByParticipant", "err", err.Error())
		return nil, err
	}

	txs, err := getPendingTransactions(ctx.Context(), api.db, api.mempool, *offset, 1000, api.network, "sender = ? OR recipient = ?", trimPrefix(address.Bytes()), trimPrefix(address.Bytes()))
	if err != nil {
		exhaustChannels[*paginator[map[string]interface{}]](heavyResult, errChan)
		log.Error("Error getting pending txs in getTransactionByParticipant", "err", err.Error())
		return nil, err
	}

	ctxs = append(ctxs, txs...)

	result := paginator[map[string]interface{}]{Items: ctxs}
	if len(ctxs) >= 1000 {
		result.Token = *offset + len(ctxs)
	}

	select {
	case hr, ok := <- heavyResult:
		if ok {
			result.Items = append(hr.Items, result.Items...)
			if result.Token == nil {
				result.Token = hr.Token
			}
		}
	case err := <- errChan:
		return nil, err
	}

	return &result, nil
}

func (api *FlumeAPI) GetTransactionReceiptsByParticipant(ctx *rpc.CallContext, address common.Address, offset *int) (*paginator[map[string]interface{}], error) {

	heavyResult := make(chan *paginator[map[string]interface{}])
	errChan := make(chan error)

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("flume_getTransactionReceiptsByParticipant sent to flume heavy by default")
		missMeter.Mark(1)
		go func() {
			rt, err := heavy.CallHeavy[*paginator[map[string]interface{}]](ctx.Context(), api.cfg.HeavyServer, "flume_getTransactionReceiptsByParticipant", address, offset)
			if err != nil {
				log.Error("Error processing request in flume_getTransactionReceiptsByParticipant", "err", err)
				errChan <- err
			}
			heavyResult <- *rt
		}()
	}

	if offset == nil {
		offset = new(int)
	}

	var receipts []map[string]interface{}
	var err error

	if ctx.Latest > 0 {
		receipts, err = getFlumeTransactionReceipts(ctx.Context(), api.db, *offset, 1000, api.network, "(sender = ? OR recipient = ?) AND block < ?", trimPrefix(address.Bytes()), trimPrefix(address.Bytes()), ctx.Latest)
	} else {
		receipts, err = getFlumeTransactionReceipts(ctx.Context(), api.db, *offset, 1000, api.network, "sender = ? OR recipient = ?", trimPrefix(address.Bytes()), trimPrefix(address.Bytes()))
	}
	if err != nil {
		exhaustChannels[*paginator[map[string]interface{}]](heavyResult, errChan)
		log.Error("Error getting receipts in getTransactionReceiptsByParticipant", "err", err.Error())
		return nil, err
	}

	result := paginator[map[string]interface{}]{Items: receipts}
	if len(receipts) == 1000 {
		result.Token = *offset + len(receipts)
	}

	select {
	case hr, ok := <- heavyResult:
		if ok {
			result.Items = append(hr.Items, result.Items...)
			if result.Token == nil {
				result.Token = hr.Token
			}
		}
	case err := <- errChan:
		return nil, err
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
		log.Error("Error getting receipts, flume_getTransactionReceiptsByBlockHash", "err", err)
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
		log.Error("Error getting receipts, flume_getTransactionReceiptsByBlockNumber", "err", err)
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
	
	if len(api.cfg.HeavyServer) > 0 && !txDataPresent(txHash, api.cfg, api.db, api.mempool) {
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

func (api *FlumeAPI) BlockHashesWithPrefix(ctx *rpc.CallContext, partialHexString string) ([]string, error) {

	if len(partialHexString) == 66 {
		return []string{partialHexString}, nil
	}

	heavyResult := make(chan []string)
	errChan := make(chan error)

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("flume_blockHashesWithPrefix sent to flume heavy by default")
		missMeter.Mark(1)
		go func() {
			hashes, err := heavy.CallHeavyDiscrete[[]string](ctx.Context(), api.cfg.HeavyServer, api.cfg.EarliestBlock, "flume_blockHashesWithPrefix", partialHexString)
			if err != nil {
				log.Error("Error calling heavy server, flume_blockHashesWithPrefix", "err", err)
				errChan <- err
			}
			heavyResult <- *hashes
		}()
	} else {
		close(heavyResult)
	}

	bytes, err := hex.DecodeString(partialHexString[2:])
	if err != nil {
		exhaustChannels[[]string](heavyResult, errChan)
		log.Error("Error decoding partial Hex String, flume_blockHashesWithPrefix", "err", err)
		return nil, err
	} 

	zeros, err := countLeadingZeros(bytes)
	if err != nil {
		exhaustChannels[[]string](heavyResult, errChan)
		log.Error("Error trimming input, flume_blockHashesWithPrefix")
		return nil, err
	}

	bytes = bytes[zeros:]

	augmentedBytes := incrementLastByte(bytes)

	var statement string
	var rows *sql.Rows
	var sqlErr error

	if ctx.Latest > 0 {
		statement = "SELECT hash FROM blocks.blocks WHERE hash > ? AND hash < ? AND LENGTH(hash) = ? AND block < ? LIMIT 20"
		rows, sqlErr = api.db.QueryContext(ctx.Context(), statement, bytes, augmentedBytes, 32 - zeros, ctx.Latest)
	} else {
		statement = "SELECT hash FROM blocks.blocks WHERE hash > ? AND hash < ? AND LENGTH(hash) = ? LIMIT 20"
		rows, sqlErr = api.db.QueryContext(ctx.Context(), statement, bytes, augmentedBytes, 32 - zeros)
	}
	if sqlErr != nil {
		exhaustChannels[[]string](heavyResult, errChan)
		log.Error("Error returned from query in flume_blockHashWithPrefix", "err", err)
		return nil, nil
	}

	defer rows.Close()
	var result []string

	for rows.Next() {
		var hashBytes []byte
		err := rows.Scan(&hashBytes)
		if err != nil {
			exhaustChannels[[]string](heavyResult, errChan)
			log.Error("Error scanning rows flume_blockHashesWithPrefix")
			return nil, err
		}
		result = append(result, hexutil.Encode(hashBytes))
	}

	select {
		case hr, ok := <- heavyResult:
			if ok {
				result = append(hr, result...)
			}
		case err := <- errChan:
		return nil, err
	}

	return result, nil

}

func (api *FlumeAPI) TransactionHashesWithPrefix(ctx *rpc.CallContext, partialHexString string) ([]string, error) {

	if len(partialHexString) == 66 {
		return []string{partialHexString}, nil
	}

	heavyResult := make(chan []string)
	errChan := make(chan error)

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("flume_TransactionHashesWithPrefix sent to flume heavy by default")
		missMeter.Mark(1)
		go func() {
			hashes, err := heavy.CallHeavy[[]string](ctx.Context(), api.cfg.HeavyServer, "flume_TransactionHashesWithPrefix", partialHexString)
			if err != nil {
				log.Error("Error calling heavy server, flume_transactionHashesWithPrefix", "err", err)
				errChan <- err
			}
			heavyResult <- *hashes
		}()
	} else {
		close(heavyResult)
	}

	bytes, err := hex.DecodeString(partialHexString[2:])
	if err != nil {
		exhaustChannels[[]string](heavyResult, errChan)
		log.Error("Error decoding partial Hex String, flume_transactionHashesWithPrefix", "err", err)
		return nil, err
	}

	zeros, err := countLeadingZeros(bytes)
	if err != nil {
		exhaustChannels[[]string](heavyResult, errChan)
		log.Error("Error trimming input, flume_transactionHashesWithPrefix")
		return nil, err
	}

	bytes = bytes[zeros:]

	augmentedBytes := incrementLastByte(bytes)

	var result []string
	
	if api.mempool {
		mpStatement := "SELECT hash FROM mempool.transactions WHERE hash > ? AND hash < ? AND LENGTH(hash) = ? LIMIT 20"
		
		mpRows, err := api.db.QueryContext(ctx.Context(), mpStatement, bytes, augmentedBytes, 32 - zeros)
		if err != nil {
			exhaustChannels[[]string](heavyResult, errChan)
			log.Error("Error returned from mempool query in flume_transactionHashesWithPrefix", "err", err)
			return nil, nil
		}
		defer mpRows.Close()
		for mpRows.Next() {
			var hashBytes []byte
			err := mpRows.Scan(&hashBytes)
			if err != nil {
				exhaustChannels[[]string](heavyResult, errChan)
				log.Error("Error scanning mempool rows flume_transactionHashesWithPrefix")
				return nil, err
			}
			result = append(result, hexutil.Encode(hashBytes))
		}
	}

	var txStatement string
	var txRows *sql.Rows
	var sqlErr error

	if ctx.Latest > 0 {
		txStatement = "SELECT hash FROM transactions.transactions WHERE hash > ? AND hash < ? AND LENGTH(hash) = ? AND block < ?LIMIT 20"
		txRows, sqlErr = api.db.QueryContext(ctx.Context(), txStatement, bytes, augmentedBytes, 32 - zeros, ctx.Latest)
	} else {
		txStatement = "SELECT hash FROM transactions.transactions WHERE hash > ? AND hash < ? AND LENGTH(hash) = ? LIMIT 20"
		txRows, sqlErr = api.db.QueryContext(ctx.Context(), txStatement, bytes, augmentedBytes, 32 - zeros)
	}
	if sqlErr != nil {
		exhaustChannels[[]string](heavyResult, errChan)
		log.Error("Error returned from transaction query in flume_transactionHashesWithPrefix", "err", err)
		return nil, nil
	}

	defer txRows.Close()
	for txRows.Next() {
		var hashBytes []byte
		err := txRows.Scan(&hashBytes)
		if err != nil {
			log.Error("Error scanning transaction rows flume_transactionHashesWithPrefix")
			return nil, err
		}
		result = append(result, hexutil.Encode(hashBytes))
	}

	select {
		case hr, ok := <- heavyResult:
			if ok {
				result = append(hr, result...)
			}
		case err := <- errChan:
	return nil, err
}

	return result, nil
}

func (api *FlumeAPI) AddressWithPrefix(ctx *rpc.CallContext, partialHexString string) ([]string, error) {

	if len(partialHexString) == 42 {
		return []string{partialHexString}, nil
	}

	heavyResult := make(chan []string)
	errChan := make(chan error)

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("flume_addressWithPrefix sent to flume heavy by default")
		missMeter.Mark(1)
		go func() {
			addresses, err := heavy.CallHeavyDiscrete[[]string](ctx.Context(), api.cfg.HeavyServer, api.cfg.EarliestBlock, "flume_addressWithPrefix", partialHexString)
			if err != nil {
				log.Error("Error calling heavy server, flume_addressWithPrefix", "err", err)
				errChan <- err
			}
			heavyResult <- *addresses
		}()
	} else {
		close(heavyResult)
	}

	bytes, err := hex.DecodeString(partialHexString[2:])
	if err != nil {
		exhaustChannels[[]string](heavyResult, errChan)
		log.Error("Error decoding partial Hex String, flume_addressWithPrefix", "err", err)
		return nil, err
	}

	zeros, err := countLeadingZeros(bytes)
	if err != nil {
		exhaustChannels[[]string](heavyResult, errChan)
		log.Error("Error trimming input, flume_addressWithPrefix")
		return nil, err
	}

	bytes = bytes[zeros:]

	augmentedBytes := incrementLastByte(bytes)

	var statement string
	var rows *sql.Rows
	var sqlErr error

	if ctx.Latest > 0 {
		statement = "SELECT DISTINCT(address) FROM event_logs WHERE address > ? AND address < ? AND LENGTH(address) = ? AND block < 3LIMIT 20"
		rows, sqlErr = api.db.QueryContext(ctx.Context(), statement, bytes, augmentedBytes, 20 - zeros, ctx.Latest)
	} else {
		statement = "SELECT DISTINCT(address) FROM event_logs WHERE address > ? AND address < ? AND LENGTH(address) = ? LIMIT 20"
		rows, sqlErr = api.db.QueryContext(ctx.Context(), statement, bytes, augmentedBytes, 20 - zeros)
	}
	if sqlErr != nil {
		exhaustChannels[[]string](heavyResult, errChan)
		log.Error("Error returned from query in flume_addressWithPrefix", "err", err)
		return nil, nil
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
			var addressBytes []byte
			err := rows.Scan(&addressBytes)
			if err != nil {
				exhaustChannels[[]string](heavyResult, errChan)
				log.Error("Error scanning rows flume_addressWithPrefix")
				return nil, err
			}
			result = append(result, hexutil.Encode(addressBytes))
	}

	select {
		case hr, ok := <- heavyResult:
			if ok {
				result = append(hr, result...)
			}
		case err := <- errChan:
			return nil, err
	}

	return result, nil
}

func (api *FlumeAPI) ResolvePrefix(ctx *rpc.CallContext, partialHexString string) (map[string][]string, error) {

	heavyResult := make(chan map[string][]string)
	errChan := make(chan error)

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("flume_resolvePrefix sent to flume heavy by default")
		missMeter.Mark(1)
		go func() {
			hashes, err := heavy.CallHeavyDiscrete[map[string][]string](ctx.Context(), api.cfg.HeavyServer, api.cfg.EarliestBlock, "flume_resolvePrefix", partialHexString)
			if err != nil {
				log.Error("Error calling heavy server, flume_ResolvePrefix", "err", err)
				errChan <- err
			}
			heavyResult <- *hashes
		}()
	} else {
		close(heavyResult)
	}

	blockHashes, err := api.BlockHashesWithPrefix(ctx, partialHexString)
	if err != nil {
		exhaustChannels[map[string][]string](heavyResult, errChan)
		log.Error("Error returned from flume_blockHashesWithPrefix, flume_resolvePrefix", "err", err)
		return nil, err
	}
	txHashes, err := api.TransactionHashesWithPrefix(ctx, partialHexString)
	if err != nil {
		exhaustChannels[map[string][]string](heavyResult, errChan)
		log.Error("Error returned from flume_transactionHashesWithPrefix, flume_resolvePrefix", "err", err)
		return nil, err
	}
	addresses, err := api.AddressWithPrefix(ctx, partialHexString)
	if err != nil {
		exhaustChannels[map[string][]string](heavyResult, errChan)
		log.Error("Error returned from flume_addressWithPrefix, flume_resolvePrefix", "err", err)
		return nil, err
	}

	result := map[string][]string{
		"blockHashes": blockHashes,
		"transactionHashes": txHashes,
		"addresses": addresses,
	}

	select {
		case hr, ok := <- heavyResult:
			if ok {
				result["blockHashes"] = append(hr["blockHashes"], result["blockHashes"]...)
				result["transactionHashes"] = append(hr["transactionHashes"], result["transactionHashes"]...)
				result["addresses"] = append(hr["addresses"], result["addresses"]...)
			}
		case err := <- errChan:
		return nil, err
	}


	return result, nil
}