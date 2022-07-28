package main

import (
	"regexp"
	"strconv"
	"encoding/binary"

	gtypes "github.com/ethereum/go-ethereum/core/types"
	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-evm/rlp"
	"github.com/openrelayxyz/cardinal-evm/crypto"
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

// receiptRegexp = regexp.MustCompile("c/[0-9a-z]+/b/([0-9a-z]+)/r/([0-9a-z]+)")
// c/89/b/xxx(bk hash)/br/yy(tx index)

borReceiptRegexp = regexp..MustCompile("c/89+/b/([0-9a-z]+)/br/([0-9a-z]+)")

// logRegexp = regexp.MustCompile("c/[0-9a-z]+/b/([0-9a-z]+)/l/([0-9a-z]+)/([0-9a-z]+)")
// c/89/b/xxx/bl/yy/zz

borLogRegexp = regexp.MustCompile("c/89/b/([0-9a-z]+)/bl/([0-9a-z]+)/([0-9a-z]+)")

func getTopicIndex(topics []common.Hash, idx int) []byte {
	if len(topics) > idx {
		return trimPrefix(topics[idx].Bytes())
	}
	return []byte{}
}

func Initialize(cfg *config.Config, pl *pluins.PluginLoader) {
	log.Info("Polygon plugin loaded")
}

func Indexer(cfg config.Config) indexer.Indexer { 
	return &PolygonIndexer{Chainid: cfg.Chainid}
}

func (pg *PolygonIndexer) index(pb *delivery.PendingBatch) ([]string, error) {

	blockNumber := pb.Number
	blockHash := pb.Hash

	receiptData := make(map[int]*cardinalBorReceiptMeta)
	logData := make(map[int64]*gtypes.Log)

	for k, v := range pb.Values {
		switch {
		case borReceiptRegexp.MatchString(k):
			parts := receiptRegexp.FindSubmatch([]byte(k))
			txIndex, _ := strconv.ParseInt(string(parts[2]), 16, 64)
			rmeta := &cardinalReceiptMeta{}
			rlp.DecodeBytes(v, rmeta)
			receiptData[int(txIndex)] = rmeta
		
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

	statements := []string{ //we need to either make applyParameters public or recreate in this package
		applyParameters("DELETE FROM bor_receipts WHERE number >= %v", pb.Number),
	}
	statements = append(statements, applyParameters(
		"INSERT INTO bor_receipts(hash, transactionIndex, number) VALUES (%v, %v, %v)",
		pb.Hash,
		txIndex,
		getCopy(compress(receipt.LogsBloom)),
		pb.Number,
	))
	statements = append(statements, applyParameters(
		"DELETE FROM bor_logs WHERE blockHash >= %v", pb.Hash),
	)
	statements = append(statements, applyParameters(
		"INSERT INTO bor_logs(address, topic0, topic1, topic2, topic3, topic4, data, blockHash, block) VALUES (%v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v)",
		logRecord.Address,
		getTopicIndex(logRecord.Topics, 0),
		getTopicIndex(logRecord.Topics, 1),
		getTopicIndex(logRecord.Topics, 2),
		getTopicIndex(logRecord.Topics, 3),
		getTopicIndex(logRecord.Topics, 4), //this is an assumption that needs to be checked
		compress(logRecord.Data),
		// bortxhash Keccak256("matic-bor-receipt-",binencode(number),blockhash)
		crypto.Keccak256("matic-bor-receipt" + binary.BigEndian.PutUint64(pb.Number) + pb.Hash))
		logRecord.TxIndex,
		pb.Hash,
		pb.Number,
		logRecord.Index,
	)
	return statements, nil
}


func Migrate(db *sql.DB, chainid uint64) error {
	var tableName string
	db.QueryRow("SELECT name FROM bor_receipts WHERE type='table' and name='migrations';").Scan(&tableName);
	if tableName != "migrations" {
		db.Exec("CREATE TABLE blocks.migrations (version integer PRIMARY KEY);")

		db.Exec("INSERT INTO blocks.migrations(version) VALUES (0);")
	}
	var schemaVersion uint
	db.QueryRow("SELECT version FROM bor_receipts.migrations;").Scan(&schemaVersion)
	if schemaVersion < 1 {
		if _, err := db.Exec(`CREATE TABLE bor_receipts (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			hash varchar(32) UNIQUE,
			transactionIndex MEDIUMINT,
			logsBloom blob,
			status TINYINT, // we actually dont need this, should be removed at migration, we should check once we have the old db live to connect to 
			block BIGINT
	        );`); err != nil { return err }
		}
	db.QueryRow("SELECT name FROM bor_logs WHERE type='table' and name='migrations';").Scan(&tableName);
	if tableName != "migrations" {
		db.Exec("CREATE TABLE bor_logs.migrations (version integer PRIMARY KEY);")

		db.Exec("INSERT INTO blocks.migrations(version) VALUES (0);")
	}
	db.QueryRow("SELECT version FROM bor_receipts.migrations;").Scan(&schemaVersion)
	if schemaVersion < 1 {
		if _, err := db.Exec(`CREATE TABLE bor_logs (
			address varchar(20),
			topic0 varchar(32),
			topic1 varchar(32),
			topic2 varchar(32),
			topic3 varchar(32),
			topic4 varchar(32),
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

