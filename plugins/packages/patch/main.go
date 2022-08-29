package main

import (
	"regexp"
	"bytes"
	"fmt"
	"math/big"
	log "github.com/inconshreveable/log15"
	"github.com/klauspost/compress/zlib"
	"github.com/openrelayxyz/cardinal-streams/delivery"
	"github.com/openrelayxyz/flume/config"
	"github.com/openrelayxyz/flume/indexer"
	"github.com/openrelayxyz/flume/plugins"
	
	// "github.com/openrelayxyz/cardinal-types"
	// evm "github.com/openrelayxyz/cardinal-evm/types"
	// "github.com/openrelayxyz/cardinal-evm/rlp"
	// "github.com/openrelayxyz/cardinal-evm/crypto"
	// "golang.org/x/crypto/sha3"
	// "encoding/binary"
	// "database/sql"
	// "context"
	// "io"
	// "io/ioutil"
	// "github.com/openrelayxyz/cardinal-evm/common"

)


func Initialize(cfg *config.Config, pl *plugins.PluginLoader) {
	log.Info("Polygon One off plugin loaded")
}

type PolygonOneOffIndexer struct {
	chainid uint64
}

func Indexer(cfg *config.Config) indexer.Indexer {
	return &PolygonOneOffIndexer{chainid: cfg.Chainid}
}

var borSnapshotRegexp *regexp.Regexp = regexp.MustCompile("c/[0-9a-z]+/b/[0-9a-z]+/bs")

func (pg *PolygonOneOffIndexer) Index(pb *delivery.PendingBatch) ([]string, error) {

	if pb.Number > 27099999 {
		panic("caught up with terminus")
	}

	statements := []string{indexer.ApplyParameters("DELETE FROM blocks WHERE number >= %v", pb.Number)}

	tdBytes := pb.Values[fmt.Sprintf("c/%x/b/%x/d", pg.chainid, pb.Hash.Bytes())]
	td := new(big.Int).SetBytes(tdBytes)

	statements = append(statements,	indexer.ApplyParameters(
		"INSERT INTO blocks(number, hash, td) VALUES (%v, %v, %v)",
		pb.Number, 
		pb.Hash,
		td.Bytes(),
	))
	
	statements = append(statements, indexer.ApplyParameters("DELETE FROM bor_snapshots WHERE block >= %v", pb.Number))
		
		

	statements = append(statements, indexer.ApplyParameters("DELETE FROM bor_snapshots WHERE block >= %v", pb.Number))

	snapshotBytes := pb.Values[fmt.Sprintf("c/%x/b/%x/bs", pg.chainid, pb.Hash.Bytes())]

	if len(snapshotBytes) > 0 {
		log.Error("found bor snapshot on block", "block", pb.Number)
		statements = append(statements, indexer.ApplyParameters(
			"INSERT INTO bor_snapshots(block, blockHash, snapshot) VALUES (%v, %v, %v)",
			pb.Number, 
			pb.Hash,
			compress(snapshotBytes),
		)) 
	}

	log.Info("statements", "statements", statements)
	return statements, nil
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

// func decompress(data []byte) ([]byte, error) {
// 	if len(data) == 0 {
// 		return data, nil
// 	}
// 	r, err := zlib.NewReader(bytes.NewBuffer(data))
// 	if err != nil {
// 		return []byte{}, err
// 	}
// 	raw, err := ioutil.ReadAll(r)
// 	if err == io.EOF || err == io.ErrUnexpectedEOF {
// 		return raw, nil
// 	}
// 	return raw, err
// }

// func bytesToHash(data []byte) types.Hash {
// 	result := types.Hash{}
// 	copy(result[32-len(data):], data[:])
// 	return result
// }

// func sealHash(header *evm.Header) (hash types.Hash) {
// 	hasher := sha3.NewLegacyKeccak256()
// 	encodeSigHeader(hasher, header)
// 	hasher.Sum(hash[:0])

// 	return hash
// }

// func encodeSigHeader(w io.Writer, header *evm.Header) {
// 	enc := []interface{}{
// 		header.ParentHash,
// 		header.UncleHash,
// 		header.Coinbase,
// 		header.Root,
// 		header.TxHash,
// 		header.ReceiptHash,
// 		header.Bloom,
// 		header.Difficulty,
// 		header.Number,
// 		header.GasLimit,
// 		header.GasUsed,
// 		header.Time,
// 		header.Extra[:len(header.Extra)-65], // Yes, this will panic if extra is too short
// 		header.MixDigest,
// 		header.Nonce,
// 	}

// 	// if c.IsJaipur(header.Number.Uint64()) {
// 	if header.BaseFee != nil {
// 		enc = append(enc, header.BaseFee)
// 	}
// 	// }

// 	if err := rlp.Encode(w, enc); err != nil {
// 		panic("can't encode: " + err.Error())
// 	}
// }

// func getBlockAuthor(header *evm.Header) (common.Address, error) {

// 	signature := header.Extra[len(header.Extra)-65:]

// 	pubkey, err := crypto.Ecrecover(sealHash(header).Bytes(), signature)
// 	if err != nil {
// 		log.Info("pubkey error", "err", err.Error())
// 	}

// 	var signer common.Address

// 	copy(signer[:], crypto.Keccak256(pubkey[1:])[12:])

// 	return signer, nil
// }
// // nil coinbase blocks: 31122501 - 31122623

// func Migrate(db *sql.DB, chainid uint64) error {
// 	var lowRangeBlock uint64
// 	var highRangeBlock uint64
// 	db.QueryRow("SELECT MIN(number), MAX(number) FROM blocks.blocks where coinbase = X'00' AND number != 0;").Scan(&lowRangeBlock, &highRangeBlock)
// 	log.Info("one off coinbase migration patch", "low", lowRangeBlock, "high", highRangeBlock)

// 	if lowRangeBlock != 31122501 || highRangeBlock != 31122623 {
// 		log.Error("cb X'00' block ranges do not match", "expected low", 31122501, "actual low", lowRangeBlock, "expected high", 31122623, "actual high", highRangeBlock)
// 	}

// 	var schemaVersion uint
// 	db.QueryRow("SELECT version FROM bor.migrations;").Scan(&schemaVersion)

// 	if schemaVersion == 2 {

// 		if _, err := db.Exec(`CREATE TABLE bor.bor_snapshots (block BIGINT PRIMARY KEY, blockHash varchar(32) UNIQUE, snapshot blob);`)
// 		err != nil {
// 			log.Error("Migrate bor create table bor_snapshots error", "err", err.Error())
// 			return nil
// 		}
// 		log.Info("bor snapshot table created")

// 		if _, err := db.Exec(`CREATE INDEX bor.bkHash ON bor_snapshots(blockHash);`); err != nil {
// 			log.Error("Migrate bor CREATE INDEX bkHash error", "err", err.Error())
// 			return nil
// 		}


// 		dbtx, err := db.BeginTx(context.Background(), nil)
// 		if err != nil {
// 			log.Warn("Error creating a transaction polygon plugin", "err", err.Error())
// 		}
// 		rows, _ := db.QueryContext(context.Background(), "SELECT parentHash, uncleHash, root, txRoot, receiptRoot, bloom, difficulty, number, gasLimit, gasUsed, `time`, extra, mixDigest, nonce, baseFee FROM blocks.blocks WHERE coinbase = X'00';")
// 		defer rows.Close()

// 		for rows.Next() {
// 			var bloomBytes, parentHash, uncleHash, root, txRoot, receiptRoot, extra, mixDigest, baseFee []byte
// 			var number, gasLimit, gasUsed, time, difficulty uint64
// 			var nonce int64
// 			err := rows.Scan(&parentHash, &uncleHash, &root, &txRoot, &receiptRoot, &bloomBytes, &difficulty, &number, &gasLimit, &gasUsed, &time, &extra, &mixDigest, &nonce, &baseFee)
// 			if err != nil {

// 				log.Error("scan error", "err", err.Error())
// 				return nil
// 			}


// 			logsBloom, _ := decompress(bloomBytes)
// 			if err != nil {
// 				log.Info("Error decompressing data", "err", err.Error())
// 			}
			
// 			var lb [256]byte
// 			copy(lb[:], logsBloom)
// 			var bn [8]byte
// 			binary.BigEndian.PutUint64(bn[:], uint64(nonce))
// 			dif := new(big.Int).SetUint64(difficulty)
// 			num := new(big.Int).SetUint64(number)
// 			hdr := &evm.Header{
// 				ParentHash: bytesToHash(parentHash),
// 				UncleHash: bytesToHash(uncleHash),
// 				Root: bytesToHash(root),
// 				TxHash: bytesToHash(txRoot),
// 				ReceiptHash: bytesToHash(receiptRoot),
// 				Bloom: lb,
// 				Difficulty: dif,
// 				Number: num,
// 				GasLimit: gasLimit,
// 				GasUsed: gasUsed,
// 				Time: time,
// 				Extra: extra,
// 				MixDigest: bytesToHash(mixDigest),
// 				Nonce: bn,
// 			}
// 			if len(baseFee) > 0 {
// 				hdr.BaseFee = new(big.Int).SetBytes(baseFee)
// 			}
// 			var miner common.Address
// 			if len(hdr.Extra) == 0 {
// 				miner = common.Address{}
// 			} else {
// 				miner, _ = getBlockAuthor(hdr)
// 			}

// 			statement := indexer.ApplyParameters("UPDATE blocks.blocks SET coinbase = %v WHERE number = %v", miner, number) 

// 			if _, err := dbtx.Exec(statement); err != nil {
// 				dbtx.Rollback()
// 				log.Error("Failed to execute statement one off polygon migration", "err", err.Error())
// 				continue
// 			}

// 			if number == highRangeBlock{
// 				if err := dbtx.Commit(); err != nil {
// 					log.Error("Failed to commit statement one off polygon plugin", "blockNumber", number, "err", err.Error())
// 					return nil
// 				}
// 				log.Info("blocks migration fixed on blocks ", "blockNumber", number)
// 			}
// 		}
// 	}

// 	return nil
// }
