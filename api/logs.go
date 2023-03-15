package api

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-types/hexutil"
	"github.com/openrelayxyz/cardinal-types/metrics"
	"github.com/openrelayxyz/cardinal-flume/config"
	"github.com/openrelayxyz/cardinal-flume/heavy"
	"github.com/openrelayxyz/cardinal-flume/plugins"
)

type LogsAPI struct {
	db      *sql.DB
	network uint64
	pl      *plugins.PluginLoader
	cfg     *config.Config
}

func NewLogsAPI(db *sql.DB, network uint64, pl *plugins.PluginLoader, cfg *config.Config) *LogsAPI {
	return &LogsAPI{
		db:      db,
		network: network,
		pl:      pl,
		cfg:     cfg,
	}
}

var (
	glgHitMeter  = metrics.NewMinorMeter("/flume/glg/hit")
	glgMissMeter = metrics.NewMinorMeter("/flume/glg/miss")
)

func (api *LogsAPI) GetLogs(ctx context.Context, crit FilterQuery) ([]*logType, error) {

	latestBlock, err := getLatestBlock(ctx, api.db)
	if err != nil {
		log.Error("Error retrieving latest block, call.ID, 500", "err", err.Error())
		return nil, err
	}

	whereClause := []string{}
	indexClause := ""
	params := []interface{}{}
	var goHeavy bool
	if crit.BlockHash != nil {
		var num int64
		api.db.QueryRowContext(ctx, "SELECT number FROM blocks WHERE hash = ?", crit.BlockHash.Bytes()).Scan(&num)
		whereClause = append(whereClause, "blockHash = ? AND block = ?")
		goHeavy = (num == 0)
		params = append(params, trimPrefix(crit.BlockHash.Bytes()), num)
	} else {
		var fromBlock, toBlock int64
		if crit.FromBlock == nil || crit.FromBlock.Int64() < 0 {
			fromBlock = latestBlock
		} else {
			fromBlock = crit.FromBlock.Int64()
		}
		goHeavy = (uint64(fromBlock) < api.cfg.EarliestBlock)

		if crit.ToBlock == nil || crit.ToBlock.Int64() < 0 {
			toBlock = latestBlock
		} else {
			toBlock = crit.ToBlock.Int64()
		}
		if fromBlock == toBlock {
			whereClause = append(whereClause, "block = ?")
			params = append(params, fromBlock)
		} else {
			whereClause = append(whereClause, "block >= ?", "block <= ?")
			params = append(params, fromBlock, toBlock)
		}
	}

	if goHeavy && len(api.cfg.HeavyServer) > 0 {
		log.Debug("eth_getLogs sent to flume heavy")
		missMeter.Mark(1)
		glgMissMeter.Mark(1)
		logs, err := heavy.CallHeavy[[]*logType](ctx, api.cfg.HeavyServer, "eth_getLogs", crit)
		if err != nil {
			return nil, err
		}
		return *logs, nil
	}

	justBlock := true

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("eth_getLogs served from flume light")
		hitMeter.Mark(1)
		glgHitMeter.Mark(1)
	}

	addressClause := []string{}
	for _, address := range crit.Addresses {
		addressClause = append(addressClause, fmt.Sprintf("%vaddress = ?", badAddressValues[address]))
		params = append(params, trimPrefix(address.Bytes()))
	}
	if len(addressClause) > 0 {
		justBlock = false
		whereClause = append(whereClause, fmt.Sprintf("(%v)", strings.Join(addressClause, " OR ")))
	}
	topicsClause := []string{}
	for i, topics := range crit.Topics {
		topicClause := []string{}
		for _, topic := range topics {
			topicClause = append(topicClause, fmt.Sprintf("topic%v = ?", i))
			params = append(params, trimPrefix(topic.Bytes()))
		}
		if len(topicClause) > 0 {
			topicsClause = append(topicsClause, fmt.Sprintf("(%v)", strings.Join(topicClause, " OR ")))
		} else {
			topicsClause = append(topicsClause, fmt.Sprintf("topic%v IS NOT NULL", i))
		}
	}
	if len(topicsClause) > 0 {
		justBlock = false
		whereClause = append(whereClause, fmt.Sprintf("(%v)", strings.Join(topicsClause, " AND ")))
	}
	if justBlock {
		// I *really* don't like that this is necessary, but the query planner is picking topic0_compound in these situations, which has awful performance.
		indexClause = "INDEXED BY sqlite_autoindex_event_logs_1"
	}
	query := fmt.Sprintf("SELECT address, topic0, topic1, topic2, topic3, data, block, transactionHash, transactionIndex, blockHash, logIndex FROM event_logs %v WHERE %v;", indexClause, strings.Join(whereClause, " AND "))
	doneCh := make(chan struct{})
	defer func() { close(doneCh) }()
	go func() {
		select {
		case <-doneCh:
		case <-time.NewTimer(5 * time.Second).C:
			log.Warn("Query taking > 5 seconds", "query", query, "params", params)
		}
	}()
	rows, err := api.db.QueryContext(ctx, query, params...)
	if err != nil {
		log.Error("Error selecting query", "query", query, "err", err.Error())
		return nil, err
	}
	defer rows.Close()
	logs := sortLogs{}
	blockNumbersInResponse := make(map[uint64]struct{})
	for rows.Next() {
		var address, topic0, topic1, topic2, topic3, data, transactionHash, blockHash []byte
		var blockNumber uint64
		var transactionIndex, logIndex uint
		err := rows.Scan(&address, &topic0, &topic1, &topic2, &topic3, &data, &blockNumber, &transactionHash, &transactionIndex, &blockHash, &logIndex)
		if err != nil {
			log.Error("Error scanning", "err", err.Error())
			// handleError("database error", call.ID, 500)
			return nil, fmt.Errorf("database error")
		}
		blockNumbersInResponse[blockNumber] = struct{}{}
		topics := []types.Hash{}
		if len(topic0) > 0 {
			topics = append(topics, bytesToHash(topic0))
		}
		if len(topic1) > 0 {
			topics = append(topics, bytesToHash(topic1))
		}
		if len(topic2) > 0 {
			topics = append(topics, bytesToHash(topic2))
		}
		if len(topic3) > 0 {
			topics = append(topics, bytesToHash(topic3))
		}
		input, err := decompress(data)
		if err != nil {
			log.Error("Error decompressing data", "err", err.Error())
			// handleError("database error", call.ID, 500)
			return nil, fmt.Errorf("database error")
		}
		logs = append(logs, &logType{
			Address:     bytesToAddress(address),
			Topics:      topics,
			Data:        hexutil.Bytes(input),
			BlockNumber: hexutil.EncodeUint64(blockNumber),
			TxHash:      bytesToHash(transactionHash),
			TxIndex:     hexutil.Uint(transactionIndex),
			BlockHash:   bytesToHash(blockHash),
			Index:       hexutil.Uint(logIndex),
		})
		if len(logs) > 10000 && len(blockNumbersInResponse) > 1 {
			// handleError("query returned more than 10,000 results spanning multiple blocks", call.ID, 413)
			return nil, fmt.Errorf("query returned more than 10,000 results spanning multiple blocks")
		}
	}
	if err := rows.Err(); err != nil {
		log.Error("Error scanning", "err", err.Error())
		// handleError("database error", call.ID, 500)
		return nil, fmt.Errorf("database error")
	}
	sort.Sort(logs)

	return logs, nil
}
