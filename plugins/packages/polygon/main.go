package main

import (
	"context"
	"bytes"
	"database/sql"
	"encoding/binary"
	"regexp"
	"strconv"
	"fmt"
	"io"
	"time"
	// "sync"

	"golang.org/x/crypto/sha3"
	// lru "github.com/hashicorp/golang-lru"

	log "github.com/inconshreveable/log15"
	"github.com/klauspost/compress/zlib"
	"github.com/openrelayxyz/cardinal-evm/common"
	"github.com/openrelayxyz/cardinal-evm/crypto"
	"github.com/openrelayxyz/cardinal-evm/rlp"
	evm "github.com/openrelayxyz/cardinal-evm/types"
	"github.com/openrelayxyz/cardinal-streams/delivery"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/flume/config"
	"github.com/openrelayxyz/flume/indexer"
	"github.com/openrelayxyz/flume/plugins"
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
	borLogRegexp     *regexp.Regexp = regexp.MustCompile("c/[0-9a-z]+/b/([0-9a-z]+)/bl/([0-9a-z]+)/([0-9a-z]+)")
)




func getTopicIndex(topics []types.Hash, idx int) []byte {
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

func bytesToAddress(data []byte) common.Address {
	result := common.Address{}
	copy(result[20-len(data):], data[:])
	return result
}

var compressor *zlib.Writer
var compressionBuffer = bytes.NewBuffer(make([]byte, 0, 5*1024*1024))
var extraSeal = 65

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

func sealHash(header *evm.Header) (hash types.Hash) {
	hasher := sha3.NewLegacyKeccak256()
	encodeSigHeader(hasher, header)
	hasher.Sum(hash[:0])

	return hash
}

func encodeSigHeader(w io.Writer, header *evm.Header) {
	enc := []interface{}{
		header.ParentHash,
		header.UncleHash,
		header.Coinbase,
		header.Root,
		header.TxHash,
		header.ReceiptHash,
		header.Bloom,
		header.Difficulty,
		header.Number,
		header.GasLimit,
		header.GasUsed,
		header.Time,
		header.Extra[:len(header.Extra)-65], // Yes, this will panic if extra is too short
		header.MixDigest,
		header.Nonce,
	}

	// if c.IsJaipur(header.Number.Uint64()) {
	if header.BaseFee != nil {
		enc = append(enc, header.BaseFee)
	}
	// }

	if err := rlp.Encode(w, enc); err != nil {
		panic("can't encode: " + err.Error())
	}
}

func getBlockAuthor(header *evm.Header) (common.Address, error) {
	signature := header.Extra[len(header.Extra)-65:]

	pubkey, err := crypto.Ecrecover(sealHash(header).Bytes(), signature)
	if err != nil {
		log.Info("pubkey error", "err", err.Error())
	}

	var signer common.Address

	copy(signer[:], crypto.Keccak256(pubkey[1:])[12:])

	log.Info("got signer", "signer", signer, "number", header.Number)

	return signer, nil
}

func Indexer(cfg *config.Config) indexer.Indexer {
	return &PolygonIndexer{Chainid: cfg.Chainid}
}

func (pg *PolygonIndexer) Index(pb *delivery.PendingBatch) ([]string, error) {

	test()

	encNum := make([]byte, 8)
	binary.BigEndian.PutUint64(encNum, uint64(pb.Number))
	txHash := crypto.Keccak256(append(append([]byte("matic-bor-receipt-"), encNum...), pb.Hash.Bytes()...))

	receiptData := make(map[int][]byte)
	logData := make(map[int64]*evm.Log)

	statements := []string{indexer.ApplyParameters("DELETE FROM bor_receipts WHERE number >= %v", pb.Number), indexer.ApplyParameters("DELETE FROM bor_logs WHERE block >= %v", pb.Number)}

	headerBytes := pb.Values[fmt.Sprintf("c/%x/b/%x/h", pg.Chainid, pb.Hash.Bytes())]
	header := &evm.Header{}
	if err := rlp.DecodeBytes(headerBytes, &header); err != nil {
		panic(err.Error())
	}

	// write a get block author function -- get example header with known fields, go through bor code and set it up to reproduce the author generation from there
	// https://github.com/maticnetwork/bor/blob/72aa44efe669e5d1f1b10f93fe1b153b63b82952/consensus/bor/bor.go#L238

	getBlockAuthor(header)

	// statements = append(statements, indexer.ApplyParameters("UPDATE blocks.blocks SET coinbase = %v WHERE number = %v", author, pb.Number))
	
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
		if len(logData) == 0 {
			return []string{}, nil
		}
		for txIndex, logsBloom := range receiptData {
			statements = append(statements, indexer.ApplyParameters(
				"INSERT INTO bor_receipts(hash, transactionIndex, number) VALUES (%v, %v, %v)",
				txHash,
				txIndex,
				compress(logsBloom),
				pb.Number,
			))
		}
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
			))
		}
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
		db.Exec(`CREATE TABLE bor.bor_receipts (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			hash varchar(32) UNIQUE,
			transactionIndex MEDIUMINT,
			logsBloom blob,
			block BIGINT
	        );`)

		if _, err := db.Exec(`CREATE INDEX bor.receiptBlock ON bor_receipts(block)`); err != nil {
			log.Error("bor_receiptBlock CREATE INDEX error", "err", err.Error())
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
		if _, err := db.Exec("UPDATE bor.migrations SET version = 1;"); err != nil {
			return err
		}
		log.Info("bor migrations done")
	}
	if schemaVersion < 2 {
		log.Info("Inside bor migration v2", "time", time.Now())
		rows, _ := db.QueryContext(context.Background(), "SELECT number, extra FROM blocks.blocks")
		defer rows.Close()
		for rows.Next() {
			var blockNumber uint64 
			var extra []byte
			rows.Scan(&blockNumber, &extra)
			hdr := &evm.Header{
				Extra: extra,
			}
			miner, _ := getBlockAuthor(hdr)
			statement := indexer.ApplyParameters("INSERT INTO blocks(coinbase) VALUES (%v)", miner) 
				dbtx, err := db.BeginTx(context.Background(), nil)
				if err != nil {
					log.Info("Error creating a transaction polygon plugin", "err", err.Error())
				}
				if _, err := dbtx.Exec(statement); err != nil {
					dbtx.Rollback()
					log.Warn("Failed to insert statement polygong migration v2", "err", err.Error())
					continue
				}
				if err := dbtx.Commit(); err != nil {
					log.Info("Failed to insert statement polygon plugin", "err", err.Error())
					continue
					}
		}
		if _, err := db.Exec("UPDATE bor.migrations SET version = 2;"); err != nil {
			log.Info("polygon migrations v2 error")
		}
		log.Info("bor migrations done")
	}
	return nil
}
