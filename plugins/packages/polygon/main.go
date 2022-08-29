package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"regexp"
	"strconv"
	"fmt"
	"math/big"


	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-evm/common"
	"github.com/openrelayxyz/cardinal-evm/crypto"
	"github.com/openrelayxyz/cardinal-evm/rlp"
	evm "github.com/openrelayxyz/cardinal-evm/types"
	"github.com/openrelayxyz/cardinal-streams/delivery"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-types/hexutil"
	"github.com/openrelayxyz/flume/config"
	"github.com/openrelayxyz/flume/indexer"
	"github.com/openrelayxyz/flume/plugins"
	rpcTransports "github.com/openrelayxyz/cardinal-rpc/transports"
)

func Initialize(cfg *config.Config, pl *plugins.PluginLoader) {
	log.Info("Polygon migrate and indexing plugin loaded")
}

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
	borLogRegexp     *regexp.Regexp = regexp.MustCompile("c/[0-9a-z]+/b/([0-9a-z]+)/bl/([0-9a-z]+)/([0-9a-z]+)")
)

func Indexer(cfg *config.Config) indexer.Indexer {
	return &PolygonIndexer{Chainid: cfg.Chainid}
}

func (pg *PolygonIndexer) Index(pb *delivery.PendingBatch) ([]string, error) {

	encNum := make([]byte, 8)
	binary.BigEndian.PutUint64(encNum, uint64(pb.Number))
	txHash := crypto.Keccak256(append(append([]byte("matic-bor-receipt-"), encNum...), pb.Hash.Bytes()...))

	receiptData := make(map[int][]byte)
	logData := make(map[int64]*evm.Log)

	statements := []string{indexer.ApplyParameters("DELETE FROM bor_receipts WHERE block >= %v", pb.Number), indexer.ApplyParameters("DELETE FROM bor_logs WHERE block >= %v", pb.Number)}

	headerBytes := pb.Values[fmt.Sprintf("c/%x/b/%x/h", pg.Chainid, pb.Hash.Bytes())]
	header := &evm.Header{}
	if err := rlp.DecodeBytes(headerBytes, &header); err != nil {
		panic(err.Error())
	}

	author, err := plugins.GetBlockAuthor(header)
	if err != nil {
		log.Info("getBlockAuthor error", "err", err.Error())
	}

	stmt := indexer.ApplyParameters("UPDATE blocks.blocks SET coinbase = %v WHERE number = %v", author, pb.Number)
	statements = append(statements, stmt)

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

			logRecord := &evm.Log{}
			rlp.DecodeBytes(v, logRecord)
			logRecord.BlockNumber = uint64(pb.Number)
			logRecord.TxIndex = uint(txIndex)
			logRecord.BlockHash = types.Hash(pb.Hash)
			logRecord.Index = uint(logIndex)
			logData[int64(logIndex)] = logRecord
			}
		}
	
		for txIndex, logsBloom := range receiptData {
			statements = append(statements, indexer.ApplyParameters(
				"INSERT INTO bor_receipts(hash, transactionIndex, logsBloom, block) VALUES (%v, %v, %v, %v)",
				txHash,
				txIndex,
				plugins.Compress(logsBloom),
				pb.Number,
			))
		}
		for logIndex, logRecord := range logData {
			statements = append(statements, indexer.ApplyParameters(
				"INSERT INTO bor_logs(address, topic0, topic1, topic2, topic3, data, transactionHash, transactionIndex, blockHash, block, logIndex) VALUES (%v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v)",
				logRecord.Address,
				plugins.GetTopicIndex(logRecord.Topics, 0),
				plugins.GetTopicIndex(logRecord.Topics, 1),
				plugins.GetTopicIndex(logRecord.Topics, 2),
				plugins.GetTopicIndex(logRecord.Topics, 3),
				plugins.Compress(logRecord.Data),
				txHash,
				logRecord.TxIndex,
				pb.Hash,
				pb.Number,
				logIndex,
			))
		}
	return statements, nil
}


func Migrate(db *sql.DB, chainid uint64) error {
	var tableName string
	db.QueryRow("SELECT name FROM bor.sqlite_master WHERE type='table' and name='migrations';").Scan(&tableName)
	if tableName != "migrations" {
		db.Exec("CREATE TABLE bor.migrations (version integer PRIMARY KEY);")

		db.Exec("INSERT INTO bor.migrations(version) VALUES (0);")
	}
	var schemaVersion uint
	db.QueryRow("SELECT version FROM bor.migrations;").Scan(&schemaVersion)
	if schemaVersion < 1 {
		log.Info("Applying bor v1 migration")
		db.Exec(`CREATE TABLE bor.bor_receipts (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			hash varchar(32) UNIQUE,
			transactionIndex MEDIUMINT,
			logsBloom blob,
			block BIGINT
	        );`)

		if _, err := db.Exec(`CREATE INDEX bor.receiptBlock ON bor_receipts(block)`); err != nil {
			log.Error("bor_receiptBlock CREATE INDEX error", "err", err.Error())
			return nil
		}

		db.Exec(`CREATE TABLE bor.bor_logs (
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
			);`)
			
		if _, err := db.Exec(`CREATE INDEX bor.logsTxHash ON bor_logs(transactionHash)`); err != nil {
			log.Error("bor_receiptBlock CREATE INDEX error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`CREATE INDEX bor.logsBkHash ON bor_logs(blockHash)`); err != nil {
			log.Error("bor_receiptBlock CREATE INDEX error", "err", err.Error())
			return nil
		}
		db.Exec("UPDATE bor.migrations SET version = 1;")
	}
	if schemaVersion < 2 {
		log.Info("Applying mempool v2 migration")

		var highestBlock uint64
		db.QueryRow("SELECT MAX(number) FROM blocks.blocks;").Scan(&highestBlock)
		terminus := highestBlock / 500 * 500

		dbtx, err := db.BeginTx(context.Background(), nil)
		if err != nil {
			log.Warn("Error creating a transaction polygon plugin", "err", err.Error())
		}
		rows, _ := db.QueryContext(context.Background(), "SELECT parentHash, uncleHash, root, txRoot, receiptRoot, bloom, difficulty, number, gasLimit, gasUsed, `time`, extra, mixDigest, nonce, baseFee FROM blocks.blocks WHERE coinbase = X'00';")
		defer rows.Close()

		for rows.Next() {
			var bloomBytes, parentHash, uncleHash, root, txRoot, receiptRoot, extra, mixDigest, baseFee []byte
			var number, gasLimit, gasUsed, time, difficulty uint64
			var nonce int64
			err := rows.Scan(&parentHash, &uncleHash, &root, &txRoot, &receiptRoot, &bloomBytes, &difficulty, &number, &gasLimit, &gasUsed, &time, &extra, &mixDigest, &nonce, &baseFee)
			if err != nil {
				log.Info("scan error", "err", err.Error())
				return nil
			}
			

			logsBloom, _ := plugins.Decompress(bloomBytes)
			if err != nil {
				log.Info("Error decompressing data", "err", err.Error())
			}
			
			var lb [256]byte
			copy(lb[:], logsBloom)
			var bn [8]byte
			binary.BigEndian.PutUint64(bn[:], uint64(nonce))
			dif := new(big.Int).SetUint64(difficulty)
			num := new(big.Int).SetUint64(number)
			hdr := &evm.Header{
				ParentHash: plugins.BytesToHash(parentHash),
				UncleHash: plugins.BytesToHash(uncleHash),
				Root: plugins.BytesToHash(root),
				TxHash: plugins.BytesToHash(txRoot),
				ReceiptHash: plugins.BytesToHash(receiptRoot),
				Bloom: lb,
				Difficulty: dif,
				Number: num,
				GasLimit: gasLimit,
				GasUsed: gasUsed,
				Time: time,
				Extra: extra,
				MixDigest: plugins.BytesToHash(mixDigest),
				Nonce: bn,
			}
			if len(baseFee) > 0 {
				hdr.BaseFee = new(big.Int).SetBytes(baseFee)
			}
			var miner common.Address
			if len(hdr.Extra) == 0 {
				miner = common.Address{}
			} else {
				miner, _ = plugins.GetBlockAuthor(hdr)
			}

			statement := indexer.ApplyParameters("UPDATE blocks.blocks SET coinbase = %v WHERE number = %v", miner, number) 

			
			if _, err := dbtx.Exec(statement); err != nil {
				dbtx.Rollback()
				log.Warn("Failed to insert statement polygong migration v2", "err", err.Error())
				return nil
			}
			if number <= terminus {
				if number%500 == 0 { 
					if err := dbtx.Commit(); err != nil {
						log.Error("Failed to commit statements in loop, polygon plugin", "blockNumber", number, "err", err.Error())
						return nil
					}
					log.Info("blocks migration in progress", "blockNumber", number)
					dbtx, err = db.BeginTx(context.Background(), nil)
					if err != nil {
						log.Error("Error creating a transaction in loop, polygon plugin", "err", err.Error())
						return nil
					}
				} 
			}
			if number == highestBlock {
				if err := dbtx.Commit(); err != nil {
					log.Error("Failed to insert statements at terminus, polygon plugin", "blockNumber", number, "err", err.Error())
					return nil
				}
				log.Info("polygon migration v2 finished on block", "blockNumber", number)
			}
		}
		db.Exec("UPDATE bor.migrations SET version = 2;")
		log.Info("bor migrations done")
	}
	
	log.Info("bor migrations up to date")
	return nil
}

type PolygonService struct {
	db *sql.DB
	cfg *config.Config
}

func RegisterAPI(tm *rpcTransports.TransportManager, db *sql.DB, cfg *config.Config) error {
	tm.Register("eth", &PolygonService{
			db: db,
			cfg: cfg,
	})
	log.Info("PolygonService registered")
	return nil	
}

func (service *PolygonService) ChainId(ctx context.Context) hexutil.Uint64 {
	return hexutil.Uint64(service.cfg.Chainid)
}

func GetBlockByNumber(blockVal map[string]interface{}, db *sql.DB) (map[string]interface{}, error) {
	var txHash  []byte
	var txIndex  uint64

	if err := db.QueryRowContext(context.Background(), "SELECT hash, transactionIndex FROM bor.bor_receipts WHERE block = ?;", uint64(blockVal["number"].(hexutil.Uint64))).Scan(&txHash, &txIndex);
	err != nil {
		log.Info("sql response", "err", err)
	}

	if txHash != nil {
		switch blockVal["transactions"].(type) {
		case []types.Hash:
			txns := blockVal["transactions"].([]types.Hash)
			blockVal["transactions"] = append(txns, plugins.BytesToHash(txHash))
			return blockVal, nil

		case []map[string]interface{}:
			txns := blockVal["transactions"].([]map[string]interface{})
			borTx := map[string]interface{}{
				"blockHash": blockVal["hash"],
				"blockNumber": blockVal["number"],
				"from": "0x0000000000000000000000000000000000000000",
				"gas": "0x0",
				"gasPrice": "0x0",
				"hash": plugins.BytesToHash(txHash),
				"input": "0x",
				"nonce": "0x0",
				"r": "0x0",
				"s": "0x0",
				"to": "0x0000000000000000000000000000000000000000",
				"transactionIndex": hexutil.Uint64(txIndex),
				"type": "0x0",
				"v": "0x0",
				"value": "0x0",
			}
			blockVal["transactions"] = append(txns, borTx)
			return blockVal, nil
		}
	}
	return nil, nil
}

func GetBlockByHash(blockVal map[string]interface{}, db *sql.DB) (map[string]interface{}, error) {
	var txHash  []byte
	var txIndex  uint64

	if err := db.QueryRowContext(context.Background(), "SELECT transactionHash, transactionIndex FROM bor.bor_logs WHERE blockHash = ?;", blockVal["hash"]).Scan(&txHash, &txIndex)
	err != nil {
		log.Info("sql response", "err", err)
	}

	if txHash != nil {

		switch blockVal["transactions"].(type) {

		case []types.Hash:
			txns := blockVal["transactions"].([]types.Hash)
			blockVal["transactions"] = append(txns, plugins.BytesToHash(txHash))
			return blockVal, nil

		case []map[string]interface{}:
			txns := blockVal["transactions"].([]map[string]interface{})
			borTx := map[string]interface{}{
				"blockHash": blockVal["hash"],
				"blockNumber": blockVal["number"],
				"from": "0x0000000000000000000000000000000000000000",
				"gas": "0x0",
				"gasPrice": "0x0",
				"hash": plugins.BytesToHash(txHash),
				"input": "0x",
				"nonce": "0x0",
				"r": "0x0",
				"s": "0x0",
				"to": "0x0000000000000000000000000000000000000000",
				"transactionIndex": hexutil.Uint64(txIndex),
				"type": "0x0",
				"v": "0x0",
				"value": "0x0",
			}
			blockVal["transactions"] = append(txns, borTx)
			return blockVal, nil

		}
	}

	return nil, nil
}

func GetTransactionByHash(txHash types.Hash, db *sql.DB) (map[string]interface{}, error) {  // method is very slow to respond 10+ secs
	var blockHash []byte
	var blockNumber, txIndex  uint64

	if err := db.QueryRowContext(context.Background(), "SELECT DISTINCT block, blockHash, transactionIndex FROM bor.bor_logs INDEXED BY logsTxHash WHERE transactionHash = ?;", txHash).Scan(&blockNumber, &blockHash, &txIndex);
	err != nil {
		log.Info("sql response", "err", err)
		return nil, nil
	} 

	if blockHash != nil {

		borTxObj :=  map[string]interface{}{
				"blockHash": plugins.BytesToHash(blockHash),
				"blockNumber": hexutil.Uint64(blockNumber),
				"from": "0x0000000000000000000000000000000000000000",
				"gas": "0x0",
				"gasPrice": "0x0",
				"hash": txHash,
				"input": "0x",
				"nonce": "0x0",
				"to": "0x0000000000000000000000000000000000000000",
				"transactionIndex": hexutil.Uint64(txIndex),
				"value": "0x0",
				"type": "0x0",
				"v": "0x0",
				"r": "0x0",
				"s": "0x0",
		}
		return borTxObj, nil
	}
	return nil, nil
}

func GetTransactionReceipt(db *sql.DB, txHash types.Hash) (map[string]interface{}, error) { 
	var blockHash []byte
	var blockNumber, txIndex uint64

	if err := db.QueryRowContext(context.Background(), "SELECT DISTINCT block, blockHash, transactionIndex FROM bor.bor_logs INDEXED BY logsTxHash WHERE transactionHash = ?;", txHash).Scan(&blockNumber, &blockHash, &txIndex);
	err != nil {
		log.Info("GTR sql response", "err", err)
		return nil, nil
	} 

	if blockHash != nil {
		
		bkHash := plugins.BytesToHash(blockHash)

		logsBloom, err := plugins.GetLogsBloom(db, blockNumber)
		if err != nil {
			log.Error("Error fetching logsBloom", "err", err.Error())
			return nil, err
		}

		txLogs, err := plugins.GetLogs(db, blockNumber, bkHash, txIndex) 
		if err != nil {
			log.Error("Error fetching logs", "err", err.Error())
			return nil, err
		}

		txObj := map[string]interface{}{
			"blockHash": plugins.BytesToHash(blockHash),
    		"blockNumber": hexutil.Uint64(blockNumber),
    		"contractAddress": nil,
    		"cumulativeGasUsed": "0x0",
    		"effectiveGasPrice": "0x0",
    		"from": "0x0000000000000000000000000000000000000000",
    		"gasUsed": "0x0",
    		"logs": txLogs,
			"logsBloom": hexutil.Bytes(logsBloom),
    		"status": "0x1",
    		"to": "0x0000000000000000000000000000000000000000",
    		"transactionHash": txHash,
    		"transactionIndex": hexutil.Uint64(txIndex),
    		"type": "0x0",
		}

		return txObj, nil
	}
	return nil, nil
}


func (service *PolygonService) GetBorBlockReceipt(ctx context.Context, bkHash types.Hash) (map[string]interface{}, error) {
	var transactionHash []byte
	var blockNumber, txIndex uint64

	if err := service.db.QueryRowContext(context.Background(), "SELECT DISTINCT block, transactionHash, transactionIndex FROM bor.bor_logs WHERE blockHash = ?;", bkHash).Scan(&blockNumber, &transactionHash, &txIndex);
	err != nil {
		log.Info("sql response", "err", err)
		return nil, nil
	}

	if transactionHash != nil {

		txHash := plugins.BytesToHash(transactionHash)

		logsBloom, err := plugins.GetLogsBloom(service.db, blockNumber)
		if err != nil {
			log.Error("Error fetching logsBloom", "err", err.Error())
			return nil, err
		}

		txLogs, err := plugins.GetLogs(service.db, blockNumber, bkHash, txIndex) 
		if err != nil {
			log.Error("Error fetching logs", "err", err.Error())
			return nil, err
		}

		borBlockObj := map[string]interface{}{
			"root": "0x",
			"status": "0x1",
			"cumulativeGasUsed": "0x0",
			"logsBloom": hexutil.Bytes(logsBloom),
			"logs": txLogs,
			"transactionHash": txHash,
			"contractAddress": "0x0000000000000000000000000000000000000000",
			"gasUsed": "0x0",
			"blockHash": bkHash,
			"blockNumber": hexutil.Uint64(blockNumber),
			"transactionIndex": hexutil.Uint(txIndex),
			}
	
		return borBlockObj, nil
	}
	
	return nil, nil
}

func (service *PolygonService) GetTransactionReceiptsByBlock(ctx context.Context, blockNrOrHash plugins.BlockNumberOrHash) ([]map[string]interface{}, error) {
	errResponse := make(map[string]interface{})
	receipts := []map[string]interface{}{}

	number, numOk := blockNrOrHash.Number()
	hash, hshOk := blockNrOrHash.Hash()

	var column interface{}
	var whereClause string
	var borTxQuery string

	switch true {
		case numOk: 
			column = number
			whereClause = "block = ?"
			borTxQuery = "SELECT transactionHash FROM bor.bor_logs WHERE block = ?;"
		case hshOk:
			column = plugins.TrimPrefix(hash.Bytes())
			whereClause = "blocks.hash = ?"
			borTxQuery = "SELECT transactionHash FROM bor.bor_logs WHERE blockHash = ?;"
		default:
			errResponse["message"] = "invalid argument 0: json: cannot unmarshal number into Go value of type string"
			receipts = append(receipts, errResponse)
			return receipts, nil
	}

	var err error
	receipts, err = plugins.GetTransactionReceiptsBlock(context.Background(), service.db, 0, 100000, service.cfg.Chainid, whereClause, column)
	if err != nil {
		return nil, err
	}

	var borTxHashBytes  []byte
	
	if err := service.db.QueryRowContext(context.Background(), borTxQuery, column).Scan(&borTxHashBytes)
	err != nil {
		log.Error("bor tx query", "err", err)
		return nil, err
	}
	
	borReceipt, err := GetTransactionReceipt(service.db, plugins.BytesToHash(borTxHashBytes))
	
	
	receipts = append(receipts, borReceipt)
	
	// if len(receipts) > 0 {
	// 	for _, receipt := range receipts {
	// 		delete(receipt, "effectiveGasPrice")
	// 		delete(receipt, "type")
	// 		receipt["transactionHash"] = plugins.BytesToHash(borTxHashBytes)
	// 	}
	// } I am leaving this here for debugging / exploring purposes. 

	return receipts, nil
}
