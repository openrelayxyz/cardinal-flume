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
	"github.com/openrelayxyz/flume/indexer"
	"github.com/openrelayxyz/flume/plugins"
)


var (
	borReceiptRegexp *regexp.Regexp = regexp.MustCompile("c/[0-9a-z]+/b/([0-9a-z]+)/br/([0-9a-z]+)")
	borLogRegexp     *regexp.Regexp = regexp.MustCompile("c/[0-9a-z]+/b/([0-9a-z]+)/bl/([0-9a-z]+)/([0-9a-z]+)")
)

type PolygonIndexer struct {
	Chainid uint64
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
		log.Debug("found bor snapshot on block", "block", pb.Number)
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

	author, err := getBlockAuthor(header)
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
			return err
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
			return err
		}
		if _, err := db.Exec(`CREATE INDEX bor.logsBkHash ON bor_logs(blockHash)`); err != nil {
			log.Error("bor_receiptBlock CREATE INDEX error", "err", err.Error())
			return err
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
				return err
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
				miner, _ = getBlockAuthor(hdr)
			}

			statement := indexer.ApplyParameters("UPDATE blocks.blocks SET coinbase = %v WHERE number = %v", miner, number) 

			
			if _, err := dbtx.Exec(statement); err != nil {
				dbtx.Rollback()
				log.Warn("Failed to insert statement polygong migration v2", "err", err.Error())
				return err
			}
			if number <= terminus {
				if number%500 == 0 { 
					if err := dbtx.Commit(); err != nil {
						log.Error("Failed to commit statements in loop, polygon plugin", "blockNumber", number, "err", err.Error())
						return err
					}
					log.Info("blocks migration in progress", "blockNumber", number)
					dbtx, err = db.BeginTx(context.Background(), nil)
					if err != nil {
						log.Error("Error creating a transaction in loop, polygon plugin", "err", err.Error())
						return err
					}
				} 
			}
			if number == highestBlock {
				if err := dbtx.Commit(); err != nil {
					log.Error("Failed to insert statements at terminus, polygon plugin", "blockNumber", number, "err", err.Error())
					return err
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
			return err
		}
		db.Exec("UPDATE bor.migrations SET version = 3;")
		log.Info("bor migrations done")
	}
	
	log.Info("bor migrations up to date")
	return nil
}