package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"regexp"
	"strconv"
	"fmt"
	"math"
	"math/big"
	"errors"
	"sort"


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
	lru "github.com/hashicorp/golang-lru"
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

	statements := []string{indexer.ApplyParameters("DELETE FROM bor_receipts WHERE block >= %v", pb.Number), 
	indexer.ApplyParameters("DELETE FROM bor_logs WHERE block >= %v", pb.Number), 
	indexer.ApplyParameters("DELETE FROM bor_snapshots WHERE block >= %v", pb.Number)}

	snapshotBytes := pb.Values[fmt.Sprintf("c/%x/b/%x/bs", pg.Chainid, pb.Hash.Bytes())]

	if len(snapshotBytes) > 0 {
		log.Error("found bor snapshot on block", "block", pb.Number)
		statements = append(statements, indexer.ApplyParameters(
			"INSERT INTO bor_snapshots(block, blockHash, snapshot) VALUES (%v, %v, %v)",
			pb.Number, 
			pb.Hash,
			plugins.Compress(snapshotBytes),
		)) 
	}

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
	}

	if schemaVersion < 3 {
		log.Info("Applying mempool v3 migration")
		db.Exec(`CREATE TABLE bor.bor_snapshots (block BIGINT PRIMARY KEY, blockHash varchar(32) UNIQUE, snapshot blob);`)
		
		log.Info("bor snapshot table created")

		if _, err := db.Exec(`CREATE INDEX bor.bkHash ON bor_snapshots(blockHash);`); err != nil {
			log.Error("Migrate bor CREATE INDEX bkHash error", "err", err.Error())
			return nil
		}
		db.Exec("UPDATE bor.migrations SET version = 3;")
		log.Info("bor migrations done")
	}
	
	log.Info("bor migrations up to date")
	return nil
}

type PolygonService struct {
	db *sql.DB
	cfg *config.Config
	recents *lru.ARCCache
}

type PolygonBorService struct {
	db *sql.DB
	cfg *config.Config
}

func RegisterAPI(tm *rpcTransports.TransportManager, db *sql.DB, cfg *config.Config) error {
	tm.Register("eth", &PolygonService{
			db: db,
			cfg: cfg,
	})
	log.Info("PolygonService registered")
	tm.Register("bor", &PolygonBorService{
		db: db,
		cfg: cfg,
	})
	log.Info("PolygonBorService registered")
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


func (service *PolygonBorService) GetRootHash(ctx context.Context, starBlockNr uint64, endBlockNr uint64) (string, error) {
	return "goodbye horses", nil
}

func (service *PolygonService) InspectSnapshot(ctx context.Context, blockNumber uint64) (interface{}, error) {
	var snapshotBytes []byte

	if err := service.db.QueryRowContext(context.Background(), "SELECT snapshot FROM bor.bor_snapshots WHERE block = ?;", blockNumber).Scan(&snapshotBytes);
	err != nil {
		log.Error("sql snapshot fetch error", "err", err)
		return nil, err
	}


	ssb, err := plugins.Decompress(snapshotBytes)
	if err != nil {
		log.Error("sql snapshot decompress error", "err", err)
		return nil, err
	}

	var snapshot interface{}

	json.Unmarshal(ssb, &snapshot)


	return snapshot, nil
}

func (service *PolygonService) Snapshot(ctx context.Context, blockNumber uint64) (*Snapshot, error) {
	var snapshotBytes []byte

	log.Info("Inside of snapshot()", "bknum", blockNumber)

	if err := service.db.QueryRowContext(context.Background(), "SELECT snapshot FROM bor.bor_snapshots WHERE block = ?;", blockNumber).Scan(&snapshotBytes);
	err != nil {
		log.Error("sql snapshot fetch error Snapshot()", "err", err)
		return nil, err
	}

	log.Info("fetching fucntio pre compress snapshot")

	ssb, err := plugins.Decompress(snapshotBytes)
	if err != nil {
		log.Error("sql snapshot decompress error Snapshot()", "err", err)
		return nil, err
	}

	var snapshot *Snapshot

	json.Unmarshal(ssb, &snapshot)


	return snapshot, nil
}

// type Snapshot struct {
// 	Hash types.Hash `json:"hash,omitempty"`
// 	Number uint64 `json:"number,omitempty"`
// 	Recents map[uint64]common.Address `json:"recents,omitempty"`
// 	ValidatorSet *Validator
	
// }

type Snapshot struct {
	// config   *params.BorConfig // Consensus engine parameters to fine tune behavior
	// sigcache *lru.ARCCache     // Cache of recent block signatures to speed up ecrecover

	Number       uint64                    `json:"number"`       // Block number where the snapshot was created
	Hash         types.Hash               `json:"hash"`         // Block hash where the snapshot was created
	ValidatorSet *ValidatorSet      `json:"validatorSet"` // Validator set at this moment
	Recents      map[uint64]common.Address `json:"recents"`      // Set of recent signers for spam protections
}

type Validator struct {
	ID               uint64         `json:"ID"`
	Address          common.Address `json:"signer"`
	VotingPower      int64          `json:"power"`
	ProposerPriority int64          `json:"accum"`
}

type ValidatorSet struct {
	// NOTE: persisted via reflect, must be exported.
	Validators []*Validator `json:"validators"`
	Proposer   *Validator   `json:"proposer"`

	// cached (unexported)
	totalVotingPower int64
	validatorsMap    map[common.Address]int // address -> index
}


func (v *Validator) Copy() *Validator {
	vCopy := *v
	return &vCopy
}

func (vals *ValidatorSet) Copy() *ValidatorSet {
	valCopy := validatorListCopy(vals.Validators)
	validatorsMap := make(map[common.Address]int, len(vals.Validators))

	for i, val := range valCopy {
		validatorsMap[val.Address] = i
	}

	return &ValidatorSet{
		Validators:       validatorListCopy(vals.Validators),
		Proposer:         vals.Proposer,
		totalVotingPower: vals.totalVotingPower,
		validatorsMap:    validatorsMap,
	}
}

var (
	extraVanity = 32 // Fixed number of extra-data prefix bytes reserved for signer vanity
	extraSeal   = 65 // Fixed number of extra-data suffix bytes reserved for signer seal
)

const (
	MaxTotalVotingPower      = int64(math.MaxInt64) / 8
	PriorityWindowSizeFactor = 2
)

type ValidatorsByAddress []*Validator

func (service *PolygonService) getPreviousSnapshot(blockNumber uint64) (*Snapshot, error) {
	var lastSnapBlock uint64

	if err := service.db.QueryRowContext(context.Background(), "SELECT block FROM bor_snapshots WHERE block < ? ORDER BY block DESC LIMIT 1;", blockNumber).Scan(&lastSnapBlock);
	err != nil {
		log.Error("sql previous snapshot fetch error", "err", err)
		return nil, err
	}

	log.Info("fetching previous snapshot")

	snap, err := service.Snapshot(context.Background(), lastSnapBlock)
	if err != nil {
		log.Error("error fetching previous snapshot")
		return nil, err
	}

	return snap, nil
}


func (service *PolygonService) GetSnapshot(ctx context.Context, hash types.Hash) (*Snapshot, error) {

	var blockNumber uint64

	if err := service.db.QueryRowContext(context.Background(), "SELECT number FROM blocks.blocks WHERE hash = ?;", hash).Scan(&blockNumber); 
	err != nil {
		log.Error("Sql blocknumber fetch error", "err", err.Error())
		return nil, err
	}

	snap := &Snapshot{}
	
	if snapshot, err := service.Snapshot(context.Background(), blockNumber); err == nil {
		log.Info("got a snapshot", "snapshot", snapshot)
		snap = snapshot
		return snap, nil
	} else {

		previousSnap, _ := service.getPreviousSnapshot(blockNumber)

		var blockRange []uint64
		recents := make(map[uint64]common.Address)

		for i := blockNumber - 63; i <= blockNumber; i++ {
			blockRange = append(blockRange, i)
		}

		var extra []byte

		if err := service.db.QueryRowContext(context.Background(), "SELECT extra FROM blocks.blocks WHERE number = ?;", blockNumber).Scan(&extra); 
		err != nil {
			log.Error("Sql blocknumber fetch error", "err", err.Error())
			return nil, err
		}
		
		validatorBytes := extra[extraVanity : len(extra)-extraSeal]

		newVals, _ := ParseValidators(validatorBytes)
		v := getUpdatedValidatorSet(previousSnap.ValidatorSet.Copy(), newVals)
		v.IncrementProposerPriority(1)
		snap.ValidatorSet = v

		for _, block := range blockRange {
			var signer common.Address
			if err := service.db.QueryRowContext(context.Background(), "SELECT coinbase FROM blocks.blocks WHERE number = ?;", block).Scan(&signer); 
			err != nil {
				return nil, err
			}
			recents[block] = signer
		}

		snap.Number = blockNumber
		snap.Hash = hash
		snap.Recents = recents

		return snap, nil


		// return &Snapshot{
		// 	Hash: hash, 
		// 	Number: blockNumber,
		// 	Recents: recents,
		// 	ValidatorSet: v,
		// }, nil

	}

}


func NewValidator(address common.Address, votingPower int64) *Validator {
	return &Validator{
		Address:          address,
		VotingPower:      votingPower,
		ProposerPriority: 0,
	}
}

func ParseValidators(validatorsBytes []byte) ([]*Validator, error) {
	if len(validatorsBytes)%40 != 0 {
		log.Error("Invalid validator bytes")
		return nil, errors.New("Invalid validators bytes")
	}

	result := make([]*Validator, len(validatorsBytes)/40)

	for i := 0; i < len(validatorsBytes); i += 40 {
		address := make([]byte, 20)
		power := make([]byte, 20)

		copy(address, validatorsBytes[i:i+20])
		copy(power, validatorsBytes[i+20:i+40])

		result[i/40] = NewValidator(plugins.BytesToAddress(address), big.NewInt(0).SetBytes(power).Int64())
	}

	return result, nil
}

func getUpdatedValidatorSet(oldValidatorSet *ValidatorSet, newVals []*Validator) *ValidatorSet {
	v := oldValidatorSet
	oldVals := v.Validators

	changes := make([]*Validator, 0, len(oldVals))

	for _, ov := range oldVals {
		if f, ok := validatorContains(newVals, ov); ok {
			ov.VotingPower = f.VotingPower
		} else {
			ov.VotingPower = 0
		}

		changes = append(changes, ov)
	}

	for _, nv := range newVals {
		if _, ok := validatorContains(changes, nv); !ok {
			changes = append(changes, nv)
		}
	}

	if err := v.UpdateWithChangeSet(changes); err != nil {
		log.Error("Error while updating change set", "error", err)
	}

	return v
}

func validatorListCopy(valsList []*Validator) []*Validator {
	if valsList == nil {
		return nil
	}

	valsCopy := make([]*Validator, len(valsList))

	for i, val := range valsList {
		valsCopy[i] = val.Copy()
	}

	return valsCopy
}

func (vals *ValidatorSet) IncrementProposerPriority(times int) {
	if vals.IsNilOrEmpty() {
		panic("empty validator set")
	}

	if times <= 0 {
		panic("Cannot call IncrementProposerPriority with non-positive times")
	}

	// Cap the difference between priorities to be proportional to 2*totalPower by
	// re-normalizing priorities, i.e., rescale all priorities by multiplying with:
	//  2*totalVotingPower/(maxPriority - minPriority)
	diffMax := PriorityWindowSizeFactor * vals.TotalVotingPower()
	vals.RescalePriorities(diffMax)
	vals.shiftByAvgProposerPriority()

	var proposer *Validator
	// Call IncrementProposerPriority(1) times times.
	for i := 0; i < times; i++ {
		proposer = vals.incrementProposerPriority()
	}

	vals.Proposer = proposer
}

func validatorContains(a []*Validator, x *Validator) (*Validator, bool) {
	for _, n := range a {
		if n.Address == x.Address {
			return n, true
		}
	}

	return nil, false
}

func (vals *ValidatorSet) IsNilOrEmpty() bool {
	return vals == nil || len(vals.Validators) == 0
}

func (vals *ValidatorSet) TotalVotingPower() int64 {
	if vals.totalVotingPower == 0 {
		log.Info("invoking updateTotalVotingPower before returning it")

		if err := vals.UpdateTotalVotingPower(); err != nil {
			// Can/should we do better?
			panic(err)
		}
	}

	return vals.totalVotingPower
}


func (vals *ValidatorSet) shiftByAvgProposerPriority() {
	if vals.IsNilOrEmpty() {
		panic("empty validator set")
	}

	avgProposerPriority := vals.computeAvgProposerPriority()

	for _, val := range vals.Validators {
		val.ProposerPriority = safeSubClip(val.ProposerPriority, avgProposerPriority)
	}
}


func (vals *ValidatorSet) incrementProposerPriority() *Validator {
	for _, val := range vals.Validators {
		// Check for overflow for sum.
		newPrio := safeAddClip(val.ProposerPriority, val.VotingPower)
		val.ProposerPriority = newPrio
	}
	// Decrement the validator with most ProposerPriority.
	mostest := vals.getValWithMostPriority()
	// Mind the underflow.
	mostest.ProposerPriority = safeSubClip(mostest.ProposerPriority, vals.TotalVotingPower())

	return mostest
}

func safeSubClip(a, b int64) int64 {
	c, overflow := safeSub(a, b)
	if overflow {
		if b > 0 {
			return math.MinInt64
		}

		return math.MaxInt64
	}

	return c
}

func safeSub(a, b int64) (int64, bool) {
	if b > 0 && a < math.MinInt64+b {
		return -1, true
	} else if b < 0 && a > math.MaxInt64+b {
		return -1, true
	}

	return a - b, false
}

func safeAddClip(a, b int64) int64 {
	c, overflow := safeAdd(a, b)
	if overflow {
		if b < 0 {
			return math.MinInt64
		}

		return math.MaxInt64
	}

	return c
}

func safeAdd(a, b int64) (int64, bool) {
	if b > 0 && a > math.MaxInt64-b {
		return -1, true
	} else if b < 0 && a < math.MinInt64-b {
		return -1, true
	}

	return a + b, false
}

func (vals *ValidatorSet) RescalePriorities(diffMax int64) {
	if vals.IsNilOrEmpty() {
		panic("empty validator set")
	}
	// NOTE: This check is merely a sanity check which could be
	// removed if all tests would init. voting power appropriately;
	// i.e. diffMax should always be > 0
	if diffMax <= 0 {
		return
	}

	// Calculating ceil(diff/diffMax):
	// Re-normalization is performed by dividing by an integer for simplicity.
	// NOTE: This may make debugging priority issues easier as well.
	diff := computeMaxMinPriorityDiff(vals)
	ratio := (diff + diffMax - 1) / diffMax

	if diff > diffMax {
		for _, val := range vals.Validators {
			val.ProposerPriority = val.ProposerPriority / ratio
		}
	}
}

func computeMaxMinPriorityDiff(vals *ValidatorSet) int64 {
	if vals.IsNilOrEmpty() {
		panic("empty validator set")
	}

	max := int64(math.MinInt64)
	min := int64(math.MaxInt64)

	for _, v := range vals.Validators {
		if v.ProposerPriority < min {
			min = v.ProposerPriority
		}

		if v.ProposerPriority > max {
			max = v.ProposerPriority
		}
	}

	diff := max - min

	if diff < 0 {
		return -1 * diff
	} else {
		return diff
	}
}

func (vals *ValidatorSet) getValWithMostPriority() *Validator {
	var res *Validator
	for _, val := range vals.Validators {
		res = res.Cmp(val)
	}

	return res
}

func (v *Validator) Cmp(other *Validator) *Validator {
	// if both of v and other are nil, nil will be returned and that could possibly lead to nil pointer dereference bubbling up the stack
	if v == nil {
		return other
	}

	if other == nil {
		return v
	}

	if v.ProposerPriority > other.ProposerPriority {
		return v
	}

	if v.ProposerPriority < other.ProposerPriority {
		return other
	}

	result := bytes.Compare(v.Address.Bytes(), other.Address.Bytes())

	if result == 0 {
		panic("Cannot compare identical validators")
	}

	if result < 0 {
		return v
	}

	// result > 0
	return other
}

func (vals *ValidatorSet) computeAvgProposerPriority() int64 {
	n := int64(len(vals.Validators))
	sum := big.NewInt(0)

	for _, val := range vals.Validators {
		sum.Add(sum, big.NewInt(val.ProposerPriority))
	}

	avg := sum.Div(sum, big.NewInt(n))

	if avg.IsInt64() {
		return avg.Int64()
	}

	// This should never happen: each val.ProposerPriority is in bounds of int64.
	panic(fmt.Sprintf("Cannot represent avg ProposerPriority as an int64 %v", avg))
}

func (vals *ValidatorSet) UpdateTotalVotingPower() error {
	sum := int64(0)
	for _, val := range vals.Validators {
		// mind overflow
		sum = safeAddClip(sum, val.VotingPower)
		if sum > MaxTotalVotingPower {
			return &TotalVotingPowerExceededError{sum, vals.Validators}
		}
	}

	vals.totalVotingPower = sum

	return nil
}

// func (vals *ValidatorSet) IncrementProposerPriority(times int) {
// 	if vals.IsNilOrEmpty() {
// 		panic("empty validator set")
// 	}

// 	if times <= 0 {
// 		panic("Cannot call IncrementProposerPriority with non-positive times")
// 	}

// 	// Cap the difference between priorities to be proportional to 2*totalPower by
// 	// re-normalizing priorities, i.e., rescale all priorities by multiplying with:
// 	//  2*totalVotingPower/(maxPriority - minPriority)
// 	diffMax := PriorityWindowSizeFactor * vals.TotalVotingPower()
// 	vals.RescalePriorities(diffMax)
// 	vals.shiftByAvgProposerPriority()

// 	var proposer *Validator
// 	// Call IncrementProposerPriority(1) times times.
// 	for i := 0; i < times; i++ {
// 		proposer = vals.incrementProposerPriority()
// 	}

// 	vals.Proposer = proposer
// }

type TotalVotingPowerExceededError struct {
	Sum        int64
	Validators []*Validator
}

func (e *TotalVotingPowerExceededError) Error() string {
	return fmt.Sprintf(
		"Total voting power should be guarded to not exceed %v; got: %v; for validator set: %v",
		MaxTotalVotingPower,
		e.Sum,
		e.Validators,
	)
}

func (vals *ValidatorSet) UpdateWithChangeSet(changes []*Validator) error {
	return vals.updateWithChangeSet(changes, true)
}


func (vals *ValidatorSet) updateWithChangeSet(changes []*Validator, allowDeletes bool) error {
	if len(changes) <= 0 {
		return nil
	}

	// Check for duplicates within changes, split in 'updates' and 'deletes' lists (sorted).
	updates, deletes, err := processChanges(changes)
	if err != nil {
		return err
	}

	if !allowDeletes && len(deletes) != 0 {
		return fmt.Errorf("cannot process validators with voting power 0: %v", deletes)
	}

	// Verify that applying the 'deletes' against 'vals' will not result in error.
	if err := verifyRemovals(deletes, vals); err != nil {
		return err
	}

	// Verify that applying the 'updates' against 'vals' will not result in error.
	updatedTotalVotingPower, numNewValidators, err := verifyUpdates(updates, vals)
	if err != nil {
		return err
	}

	// Check that the resulting set will not be empty.
	if numNewValidators == 0 && len(vals.Validators) == len(deletes) {
		return fmt.Errorf("applying the validator changes would result in empty set")
	}

	// Compute the priorities for updates.
	computeNewPriorities(updates, vals, updatedTotalVotingPower)

	// Apply updates and removals.
	vals.updateValidators(updates, deletes)

	if err := vals.UpdateTotalVotingPower(); err != nil {
		return err
	}

	// Scale and center.
	vals.RescalePriorities(PriorityWindowSizeFactor * vals.TotalVotingPower())
	vals.shiftByAvgProposerPriority()

	return nil
}

func processChanges(origChanges []*Validator) (updates, removals []*Validator, err error) {
	// Make a deep copy of the changes and sort by address.
	changes := validatorListCopy(origChanges)
	sort.Sort(ValidatorsByAddress(changes))

	sliceCap := len(changes) / 2
	if sliceCap == 0 {
		sliceCap = 1
	}

	removals = make([]*Validator, 0, sliceCap)
	updates = make([]*Validator, 0, sliceCap)

	var prevAddr common.Address

	// Scan changes by address and append valid validators to updates or removals lists.
	for _, valUpdate := range changes {
		if valUpdate.Address == prevAddr {
			err = fmt.Errorf("duplicate entry %v in %v", valUpdate, changes)
			return nil, nil, err
		}

		if valUpdate.VotingPower < 0 {
			err = fmt.Errorf("voting power can't be negative: %v", valUpdate)
			return nil, nil, err
		}

		if valUpdate.VotingPower > MaxTotalVotingPower {
			err = fmt.Errorf("to prevent clipping/ overflow, voting power can't be higher than %v: %v ",
				MaxTotalVotingPower, valUpdate)
			return nil, nil, err
		}

		if valUpdate.VotingPower == 0 {
			removals = append(removals, valUpdate)
		} else {
			updates = append(updates, valUpdate)
		}

		prevAddr = valUpdate.Address
	}

	return updates, removals, err
}

func verifyRemovals(deletes []*Validator, vals *ValidatorSet) error {
	for _, valUpdate := range deletes {
		address := valUpdate.Address
		_, val := vals.GetByAddress(address)

		if val == nil {
			return fmt.Errorf("failed to find validator %X to remove", address)
		}
	}

	if len(deletes) > len(vals.Validators) {
		panic("more deletes than validators")
	}

	return nil
}

func verifyUpdates(updates []*Validator, vals *ValidatorSet) (updatedTotalVotingPower int64, numNewValidators int, err error) {
	updatedTotalVotingPower = vals.TotalVotingPower()

	for _, valUpdate := range updates {
		address := valUpdate.Address
		_, val := vals.GetByAddress(address)

		if val == nil {
			// New validator, add its voting power the the total.
			updatedTotalVotingPower += valUpdate.VotingPower
			numNewValidators++
		} else {
			// Updated validator, add the difference in power to the total.
			updatedTotalVotingPower += valUpdate.VotingPower - val.VotingPower
		}

		overflow := updatedTotalVotingPower > MaxTotalVotingPower

		if overflow {
			err = fmt.Errorf(
				"failed to add/update validator %v, total voting power would exceed the max allowed %v",
				valUpdate, MaxTotalVotingPower)

			return 0, 0, err
		}
	}

	return updatedTotalVotingPower, numNewValidators, nil
}

func computeNewPriorities(updates []*Validator, vals *ValidatorSet, updatedTotalVotingPower int64) {
	for _, valUpdate := range updates {
		address := valUpdate.Address
		_, val := vals.GetByAddress(address)

		if val == nil {
			// add val
			// Set ProposerPriority to -C*totalVotingPower (with C ~= 1.125) to make sure validators can't
			// un-bond and then re-bond to reset their (potentially previously negative) ProposerPriority to zero.
			//
			// Contract: updatedVotingPower < MaxTotalVotingPower to ensure ProposerPriority does
			// not exceed the bounds of int64.
			//
			// Compute ProposerPriority = -1.125*totalVotingPower == -(updatedVotingPower + (updatedVotingPower >> 3)).
			valUpdate.ProposerPriority = -(updatedTotalVotingPower + (updatedTotalVotingPower >> 3))
		} else {
			valUpdate.ProposerPriority = val.ProposerPriority
		}
	}
}

func (vals *ValidatorSet) GetByAddress(address common.Address) (index int, val *Validator) {
	idx, ok := vals.validatorsMap[address]
	if ok {
		return idx, vals.Validators[idx].Copy()
	}

	return -1, nil
}

func (vals *ValidatorSet) updateValidators(updates []*Validator, deletes []*Validator) {
	vals.applyUpdates(updates)
	vals.applyRemovals(deletes)

	vals.UpdateValidatorMap()
}

func (vals *ValidatorSet) applyUpdates(updates []*Validator) {
	existing := vals.Validators
	merged := make([]*Validator, len(existing)+len(updates))
	i := 0

	for len(existing) > 0 && len(updates) > 0 {
		if bytes.Compare(existing[0].Address.Bytes(), updates[0].Address.Bytes()) < 0 { // unchanged validator
			merged[i] = existing[0]
			existing = existing[1:]
		} else {
			// Apply add or update.
			merged[i] = updates[0]
			if existing[0].Address == updates[0].Address {
				// Validator is present in both, advance existing.
				existing = existing[1:]
			}

			updates = updates[1:]
		}
		i++
	}

	// Add the elements which are left.
	for j := 0; j < len(existing); j++ {
		merged[i] = existing[j]
		i++
	}

	// OR add updates which are left.
	for j := 0; j < len(updates); j++ {
		merged[i] = updates[j]
		i++
	}

	vals.Validators = merged[:i]
}

func (vals *ValidatorSet) applyRemovals(deletes []*Validator) {
	existing := vals.Validators

	merged := make([]*Validator, len(existing)-len(deletes))
	i := 0

	// Loop over deletes until we removed all of them.
	for len(deletes) > 0 {
		if existing[0].Address == deletes[0].Address {
			deletes = deletes[1:]
		} else { // Leave it in the resulting slice.
			merged[i] = existing[0]
			i++
		}

		existing = existing[1:]
	}

	// Add the elements which are left.
	for j := 0; j < len(existing); j++ {
		merged[i] = existing[j]
		i++
	}

	vals.Validators = merged[:i]
}

func (vals *ValidatorSet) UpdateValidatorMap() {
	vals.validatorsMap = make(map[common.Address]int, len(vals.Validators))

	for i, val := range vals.Validators {
		vals.validatorsMap[val.Address] = i
	}
}

func (valz ValidatorsByAddress) Len() int {
	return len(valz)
}

func (valz ValidatorsByAddress) Less(i, j int) bool {
	return bytes.Compare(valz[i].Address.Bytes(), valz[j].Address.Bytes()) == -1
}

func (valz ValidatorsByAddress) Swap(i, j int) {
	it := valz[i]
	valz[i] = valz[j]
	valz[j] = it
}


