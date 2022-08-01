package main

import (
	"regexp"
	"strconv"
	"encoding/binary"
	"bytes"
	"database/sql"

	gtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	log "github.com/inconshreveable/log15"
	"github.com/klauspost/compress/zlib"
	"github.com/openrelayxyz/cardinal-streams/delivery"
	"github.com/openrelayxyz/cardinal-evm/rlp"
	"github.com/openrelayxyz/flume/config"
	"github.com/openrelayxyz/flume/plugins"
	"github.com/openrelayxyz/flume/indexer"
)

type PolygonIndexer struct {
	Chainid uint64
}

type cardinalBorReceiptMeta struct {
	ContractAddress   common.Address
	CumulativeGasUsed uint64
	GasUsed           uint64
	LogsBloom         []byte
	Status            uint64
	LogCount          uint
	LogOffset         uint
}

var (
	borReceiptRegexp *regexp.Regexp = regexp.MustCompile("c/[0-9a-z]+/b/([0-9a-z]+)/br/([0-9a-z]+)")
    borLogRegexp *regexp.Regexp = regexp.MustCompile("c/[0-9a-z]+/b/([0-9a-z]+)/bl/([0-9a-z]+)/([0-9a-z]+)")
)

func getTopicIndex(topics []common.Hash, idx int) []byte {
	if len(topics) > idx {
		return trimPrefix(topics[idx].Bytes())
	}
	return []byte{}
}

func trimPrefix(data []byte) []byte {
	if len(data) == 0 {
		return data
	}
	v := bytes.TrimLeft(data, string([]byte{0}))
	if len(v) == 0 {
		return []byte{0}
	}
	return v
}

var compressor *zlib.Writer
var compressionBuffer = bytes.NewBuffer(make([]byte, 0, 5*1024*1024))

func compress(data []byte) []byte {
	if len(data) == 0 {
		return data
	}
	compressionBuffer.Reset()
	if compressor == nil {
		compressor = zlib.NewWriter(compressionBuffer)
	} else {
		compressor.Reset(compressionBuffer)
	}
	compressor.Write(data)
	compressor.Close()
	return compressionBuffer.Bytes()
}

func Initialize(cfg *config.Config, pl *plugins.PluginLoader) {
	log.Info("Polygon plugin loaded")
}

func Indexer(cfg *config.Config) indexer.Indexer { 
	return &PolygonIndexer{Chainid: cfg.Chainid}
}

func (pg *PolygonIndexer) Index(pb *delivery.PendingBatch) ([]string, error) {

	encNum := make([]byte, 8)
	binary.BigEndian.PutUint64(encNum, uint64(pb.Number))
	txHash := crypto.Keccak256(append(append([]byte("matic-bor-receipt-"), encNum...), pb.Hash.Bytes()...))

	receiptData := make(map[int][]byte)
	logData := make(map[int64]*gtypes.Log)

	statements := []string{}

	for k, v := range pb.Values {
		switch {
		case borReceiptRegexp.MatchString(k):
			parts := borReceiptRegexp.FindSubmatch([]byte(k))
			txIndex, _ := strconv.ParseInt(string(parts[2]), 16, 64)
			receiptData[int(txIndex)] = v
		
		case borLogRegexp.MatchString(k):
			parts := borLogRegexp.FindSubmatch([]byte(k))
			txIndex, _ := strconv.ParseInt(string(parts[2]), 16, 64)
			logIndex, _ := strconv.ParseInt(string(parts[3]), 16, 64)

			logRecord := &gtypes.Log{}
			rlp.DecodeBytes(v, logRecord)
			logRecord.BlockNumber = uint64(pb.Number)
			logRecord.TxIndex = uint(txIndex)
			logRecord.BlockHash = common.Hash(pb.Hash)
			logRecord.Index = uint(logIndex)
			logData[int64(logIndex)] = logRecord
	}

	statements = append(statements,
		indexer.ApplyParameters("DELETE FROM bor_receipts WHERE number >= %v", pb.Number),
	)
	for txIndex, logsBloom := range receiptData { 
		statements = append(statements, indexer.ApplyParameters(
			"INSERT INTO bor_receipts(hash, transactionIndex, number) VALUES (%v, %v, %v)",
			txHash,
			txIndex,
			compress(logsBloom),
			pb.Number,
		))}
	statements = append(statements, indexer.ApplyParameters(
	"DELETE FROM bor_logs WHERE blockHash >= %v", pb.Number),
	)
	for logIndex, logRecord := range logData {
		statements = append(statements, indexer.ApplyParameters(
			"INSERT INTO bor_logs(address, topic0, topic1, topic2, topic3, data, transactionHash, transactionIndex, blockHash, block, logIndex) VALUES (%v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v)",
			logRecord.Address,
			getTopicIndex(logRecord.Topics, 0),
			getTopicIndex(logRecord.Topics, 1),
			getTopicIndex(logRecord.Topics, 2),
			getTopicIndex(logRecord.Topics, 3),
			compress(logRecord.Data),
			txHash,
			logRecord.TxIndex,
			pb.Hash,
			pb.Number,
			logIndex,
		))}
	}
	return statements, nil
}


func Migrate(db *sql.DB, chainid uint64) error {
	var tableName string
	db.QueryRow("SELECT name FROM bor.sqlite_master WHERE type='table' and name='migrations';").Scan(&tableName);
	if tableName != "migrations" {
		db.Exec("CREATE TABLE bor.migrations (version integer PRIMARY KEY);")

		db.Exec("INSERT INTO bor.migrations(version) VALUES (0);")
	}
	var schemaVersion uint
	db.QueryRow("SELECT version FROM bor.migrations;").Scan(&schemaVersion)
	if schemaVersion < 1 {
		if _, err := db.Exec(`CREATE TABLE bor.bor_receipts (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			hash varchar(32) UNIQUE,
			transactionIndex MEDIUMINT,
			logsBloom blob,
			block BIGINT
	        );`); err != nil { return err }
		
		if _, err := db.Exec(`CREATE INDEX bor.receiptBlock ON bor_receipts(block)`); err != nil { log.Error("bor_receiptBlock CREATE INDEX error", "err", err.Error()) }

		if _, err := db.Exec(`CREATE TABLE bor.bor_logs (
			address varchar(20),
			topic0 varchar(32),
			topic1 varchar(32),
			topic2 varchar(32),
			topic3 varchar(32),
			data blob,
			transactionHash varchar(32),
			transactionIndex varcahr(32),
			blockHash varchar(32),
			block BIGINT,
			logIndex MEDIUMINT,
			PRIMARY KEY (block, logIndex)
			);`); err != nil { return err }
	
	}
	return nil
}

