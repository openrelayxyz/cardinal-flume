package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/big"
	"regexp"
	"strings"

	log "github.com/inconshreveable/log15"
	streamsTransports "github.com/openrelayxyz/cardinal-streams/transports"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-types/hexutil"
	"github.com/openrelayxyz/cardinal-flume/config"
	"github.com/openrelayxyz/cardinal-flume/heavy"
	"github.com/openrelayxyz/cardinal-flume/plugins"
)

var trackedPrefixes = []*regexp.Regexp{
	regexp.MustCompile("c/[0-9a-z]+/b/[0-9a-z]+/h"),
	regexp.MustCompile("c/[0-9a-z]+/b/[0-9a-z]+/d"),
	regexp.MustCompile("c/[0-9a-z]+/b/[0-9a-z]+/u"),
	regexp.MustCompile("c/[0-9a-z]+/b/[0-9a-z]+/t/"),
	regexp.MustCompile("c/[0-9a-z]+/b/[0-9a-z]+/r/"),
	regexp.MustCompile("c/[0-9a-z]+/b/[0-9a-z]+/l/"),
	regexp.MustCompile("c/[0-9a-z]+/b/[0-9a-z]+/w"),
}

func deliverConsumer(brokerParams []streamsTransports.BrokerParams, resumption string, reorgThreshold, resumptionTime, lastNumber int64, lastHash, lastWeight []byte, tp []*regexp.Regexp) (streamsTransports.Consumer, error) { // brokerParams := cfg.BrokerParams
	rt := []byte(resumption)
	if resumptionTime > 0 {
		r, err := streamsTransports.ResumptionForTimestamp(brokerParams, resumptionTime)
		if err != nil {
			log.Warn("Could not load resumption from timestamp:", "error", err.Error())
		} else {
			rt = r
		}
	}
	return streamsTransports.ResolveMuxConsumer(brokerParams, rt, lastNumber, types.BytesToHash(lastHash), new(big.Int).SetBytes(lastWeight), reorgThreshold, tp, nil)
}

func AcquireConsumer(db *sql.DB, cfg *config.Config, resumptionTime int64, useBlockTime bool, pl *plugins.PluginLoader) (streamsTransports.Consumer, error) {
	brokerParams := cfg.BrokerParams
	reorgThreshold := cfg.ReorgThreshold
	var err error
	var tableName string

	ptpi := pl.Lookup("TrackedPrefixes", func(v interface{}) bool {
		_, ok := v.(*[]*regexp.Regexp)
		return ok
	})

	ptp := []*regexp.Regexp{}
	for _, v := range ptpi {
		if prefixes, ok := v.(*[]*regexp.Regexp); ok {
			ptp = append(ptp, (*prefixes)...)
		}
	}
	db.QueryRowContext(context.Background(), "SELECT name FROM blocks.sqlite_master WHERE type='table' and name='cardinal_offsets';").Scan(&tableName)
	if tableName != "cardinal_offsets" {
		if _, err = db.Exec("CREATE TABLE blocks.cardinal_offsets (partition INT, offset BIGINT, topic STRING, PRIMARY KEY (topic, partition));"); err != nil {
			return nil, err
		}
	}
	startOffsets := []string{}
	for _, broker := range brokerParams {
		for _, topic := range broker.Topics {
			var partition int32
			var offset int64
			rows, err := db.QueryContext(context.Background(), "SELECT partition, offset FROM cardinal_offsets WHERE topic = ?;", topic)
			if err != nil {
				return nil, err
			}
			for rows.Next() {
				if err := rows.Scan(&partition, &offset); err != nil {
					return nil, err
				}
				startOffsets = append(startOffsets, fmt.Sprintf("%v:%v=%v", topic, partition, offset))
			}
		}
	}
	resumption := strings.Join(startOffsets, ";")
	var lastHash, lastWeight []byte
	var lastNumber, timestamp int64
	db.QueryRowContext(context.Background(), "SELECT max(number), hash, td, time FROM blocks;").Scan(&lastNumber, &lastHash, &lastWeight, &timestamp)
	if len(cfg.HeavyServer) > 0 && lastNumber == 0 {
		highestBlock, err := heavy.CallHeavy[plugins.BlockNumber](context.Background(), cfg.HeavyServer, "eth_blockNumber")
		if err != nil {
			log.Info("Failed to connect with heavy server, flume light service initiated from most recent block")
			consumer, err := deliverConsumer(brokerParams, resumption, reorgThreshold, resumptionTime, lastNumber, lastHash, lastWeight, append(trackedPrefixes, ptp...))
			if err != nil {
				log.Error("Error constructing consumer from stand alone light instance", "err", err.Error())
				return nil, err
			}
			return consumer, nil
		}
		log.Debug("Current block aquired from heavy", "block", highestBlock.Int64())

		resumptionBlockNumber := highestBlock.Int64() - reorgThreshold

		resumptionBlock, err := heavy.CallHeavy[map[string]json.RawMessage](context.Background(), cfg.HeavyServer, "eth_getBlockByNumber", hexutil.Uint64(resumptionBlockNumber), false)
		if err != nil {
			return nil, err
		}

		var rb map[string]json.RawMessage = *resumptionBlock

		var rT hexutil.Uint64
		var lH types.Hash
		var lW hexutil.Bytes

		if err := json.Unmarshal(rb["totalDifficulty"], &lW); err != nil {
			log.Warn("Json unmarshalling error AcquireConsumer, lightserver condition totoal difficulty", "err", err)
		}
		if err := json.Unmarshal(rb["hash"], &lH); err != nil {
			log.Warn("Json unmarshalling error AcquireConsumer, lightserver condition hash", "err", err)
		}
		if err := json.Unmarshal(rb["timestamp"], &rT); err != nil {
			log.Warn("Json unmarshalling error AcquireConsumer, lightserver condition timestamp", "err", err)
		}

		lastWeight = lW
		lastNumber = resumptionBlockNumber
		lastHash = lH.Bytes()
		resumptionTime = int64(rT) * 1000

		consumer, err := deliverConsumer(brokerParams, resumption, reorgThreshold, resumptionTime, lastNumber, lastHash, lastWeight, append(trackedPrefixes, ptp...))
		if err != nil {
			log.Error("Error constructing consumer from heavy connected flume light instance", "err", err.Error())
			return nil, err
		}
		log.Info("Flume light service initiated, beginning from block:", "number", lastNumber)
		return consumer, nil
	}
	if resumptionTime < 0 && timestamp > 0 && useBlockTime {
		resumptionTime = timestamp * 1000
	}
	consumer, err := deliverConsumer(brokerParams, resumption, reorgThreshold, resumptionTime, lastNumber, lastHash, lastWeight, append(trackedPrefixes, ptp...))
	if err != nil {
		log.Error("Error constructing consumer from flume heavy instance", "err", err.Error())
		return nil, err
	}
	log.Info("Flume heavey service initiated, Resuming to block", "number", lastNumber)
	return consumer, nil
}
