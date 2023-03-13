package api

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-types/hexutil"
	"github.com/openrelayxyz/cardinal-types/metrics"
	"github.com/openrelayxyz/cardinal-flume/config"
	"github.com/openrelayxyz/cardinal-flume/heavy"
	"github.com/openrelayxyz/cardinal-flume/plugins"
	"time"
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

	blockClause := []string{}
	whereClause := []string{}
	indexClause := ""
	params := []interface{}{}
	blockParams := []interface{}{}
	var goHeavy bool
	if crit.BlockHash != nil {
		var num int64
		api.db.QueryRowContext(ctx, "SELECT number FROM blocks WHERE hash = ?", crit.BlockHash.Bytes()).Scan(&num)
		blockClause = append(blockClause, "blockHash = ? AND block = ?")
		
		goHeavy = (num == 0)
		params = append(params, trimPrefix(crit.BlockHash.Bytes()), num)
		blockParams = append(blockParams, trimPrefix(crit.BlockHash.Bytes()), num)
	} else {
		var fromBlock, toBlock int64
		if crit.FromBlock == nil || crit.FromBlock.Int64() < 0 {
			fromBlock = latestBlock
		} else {
			fromBlock = crit.FromBlock.Int64()
		}
		goHeavy = (uint64(fromBlock) < api.cfg.EarliestBlock)

		blockClause = append(blockClause, "block >= ?")
		params = append(params, fromBlock)
		blockParams = append(blockParams, fromBlock)
		if crit.ToBlock == nil || crit.ToBlock.Int64() < 0 {
			toBlock = latestBlock
		} else {
			toBlock = crit.ToBlock.Int64()
		}
		blockClause = append(blockClause, "block <= ?")
		params = append(params, toBlock)
		blockParams = append(params, toBlock)
	}
	whereClause = append(whereClause, blockClause...)

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

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("eth_getLogs served from flume light")
		hitMeter.Mark(1)
		glgHitMeter.Mark(1)
	}

	addressClause := []string{}
	addressParams := []interface{}{}
	for _, address := range crit.Addresses {
		addressClause = append(addressClause, "address = ?")
		addressParams = append(addressParams, trimPrefix(address.Bytes()))
		params = append(params, trimPrefix(address.Bytes()))
	}
	if len(addressClause) > 0 {
		whereClause = append(whereClause, fmt.Sprintf("(%v)", strings.Join(addressClause, " OR ")))
	}
	
	var highestTopic int
	topic0Clause := []string{}
	topic0Params := []interface{}{}
	topicsClause := []string{}
	for i, topics := range crit.Topics {
		topicClause := []string{}
		for _, topic := range topics {
			if i == 0 {
				topic0Clause = append(topic0Clause, "topic0 = ?")
				topic0Params = append(topic0Params, trimPrefix(topic.Bytes()))
			}
			topicClause = append(topicClause, fmt.Sprintf("topic%v = ?", i))
			params = append(params, trimPrefix(topic.Bytes()))
		}
		if len(topicClause) > 0 {
			highestTopic = i
			topicsClause = append(topicsClause, fmt.Sprintf("(%v)", strings.Join(topicClause, " OR ")))
		} else {
			topicsClause = append(topicsClause, fmt.Sprintf("topic%v IS NOT NULL", i))
		}
	}
	if len(topicsClause) > 0 {
		whereClause = append(whereClause, fmt.Sprintf("(%v)", strings.Join(topicsClause, " AND ")))
	}
	if highestTopic == 0 && len(topicsClause) > 0 && len(addressClause) > 0{
		// If these conditions are met, we're stuck choosing between address and topic0 indexes. We want to find out which is better.
		var addrCount, topicCount int
		start := time.Now()
		addrWhereClause := append(blockClause, strings.Join(addressClause, " OR "))
		if err := api.db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM event_logs WHERE %v", strings.Join(addrWhereClause, " AND ")), append(blockParams, addressParams...)...).Scan(&addrCount); err != nil {
			log.Warn("Error getting address clause count", "err", err)
		}
		topicWhereClause := append(blockClause, strings.Join(topic0Clause, " OR "))
		if err := api.db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM event_logs WHERE %v", strings.Join(topicWhereClause, " AND ")), append(blockParams, topic0Params...)...).Scan(&topicCount); err != nil {
			log.Warn("Error getting topic clause count", "err", err)
		}
		if topicCount < addrCount {
			indexClause = "INDEXED BY topic0_compound"
			log.Debug("Selected topic0_compound index", "addrCount", addrCount, "topicCount", topicCount, "duration", time.Since(start))
		} else {
			indexClause = "INDEXED BY address_compound"
			log.Debug("Selected address_compound index", "addrCount", addrCount, "topicCount", topicCount, "duration", time.Since(start))
		}
	}
	query := fmt.Sprintf("SELECT address, topic0, topic1, topic2, topic3, data, block, transactionHash, transactionIndex, blockHash, logIndex FROM event_logs %v WHERE %v;", indexClause, strings.Join(whereClause, " AND "))
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
