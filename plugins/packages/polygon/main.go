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
	"io/ioutil"
	"math/big"

	"golang.org/x/crypto/sha3"

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

func decompress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}
	r, err := zlib.NewReader(bytes.NewBuffer(data))
	if err != nil {
		return []byte{}, err
	}
	raw, err := ioutil.ReadAll(r)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return raw, nil
	}
	return raw, err
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

func bytesToHash(data []byte) types.Hash {
	result := types.Hash{}
	copy(result[32-len(data):], data[:])
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
	log.Info("Polygon migrate and indexing plugin loaded")
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

	return signer, nil
}

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
		if _, err := db.Exec("UPDATE bor.migrations SET version = 1;"); err != nil {
			log.Error("bor update migration v1 error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`CREATE INDEX bor.logsTxHash ON bor_logs(transactionHash)`); err != nil {
			log.Error("bor_receiptBlock CREATE INDEX error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`CREATE INDEX bor.logsBkHash ON bor_logs(blockHash)`); err != nil {
			log.Error("bor_receiptBlock CREATE INDEX error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec("UPDATE bor.migrations SET version = 1;"); err != nil {
			log.Warn("polygon migrations v2 error", "err", err.Error())
			return nil
		}
	}
	if schemaVersion < 2 {

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
			

			logsBloom, _ := decompress(bloomBytes)
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
				ParentHash: bytesToHash(parentHash),
				UncleHash: bytesToHash(uncleHash),
				Root: bytesToHash(root),
				TxHash: bytesToHash(txRoot),
				ReceiptHash: bytesToHash(receiptRoot),
				Bloom: lb,
				Difficulty: dif,
				Number: num,
				GasLimit: gasLimit,
				GasUsed: gasUsed,
				Time: time,
				Extra: extra,
				MixDigest: bytesToHash(mixDigest),
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
			if _, err := db.Exec("UPDATE bor.migrations SET version = 2;"); err != nil {
				log.Error("polygon migrations v2 error", "err", err.Error())
				return nil
			}
	}
	

	if schemaVersion < 3 {

		if _, err := db.Exec(`CREATE INDEX blocks.bkHash ON blocks(hash);`); err != nil {
			log.Error("polygon migrations CREATE INDEX bkHash On blocks error", "err", err.Error())
			return nil
		}	
		if _, err := db.Exec(`CREATE INDEX transactions.	txHash ON transactions(hash);`); err != nil {
			log.Error("polygon migrations CREATE INDEX txHash On transactions error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec("UPDATE bor.migrations SET version = 3;"); err != nil {
			log.Error("polygon migrations v2 error", "err", err.Error())
			return nil
		}
		log.Info("bor migrations done")
	}

	if schemaVersion >= 3 {
		log.Info("bor migrations up to date")
	}
	return nil
}
