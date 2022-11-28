package main

import (
	// "os"
	"context"
	"sync"
	"fmt"
	"testing"
	"database/sql"
	"github.com/mattn/go-sqlite3"
	// "reflect"


	// "github.com/openrelayxyz/cardinal-evm/vm"

	// "github.com/openrelayxyz/cardinal-flume/api"
	"github.com/openrelayxyz/cardinal-flume/config"
	"github.com/openrelayxyz/cardinal-types"
	// "github.com/openrelayxyz/cardinal-flume/migrations"
	// "github.com/openrelayxyz/cardinal-flume/indexer"
	// "github.com/openrelayxyz/cardinal-evm/common"
	"github.com/openrelayxyz/cardinal-flume/plugins"
	// "github.com/openrelayxyz/cardinal-types/hexutil"
	log "github.com/inconshreveable/log15"

	"bytes"
	// "strings"

	"compress/gzip"
	"encoding/json"
	"io"
	"io/ioutil"
	_ "net/http/pprof"

	"github.com/openrelayxyz/cardinal-streams/delivery"
	"github.com/openrelayxyz/cardinal-streams/transports"
)

func pendingBatchDecompress() ([]*delivery.PendingBatch, error) {
	file, _ := ioutil.ReadFile("./test-resources2/big_batches.json.gz")
	r, err := gzip.NewReader(bytes.NewReader(file))
	if err != nil {
		return nil, err
	}
	raw, _ := ioutil.ReadAll(r)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, err
	}
	var transportsObjectSlice []*transports.TransportBatch
	json.Unmarshal(raw, &transportsObjectSlice)
	pbSlice := []*delivery.PendingBatch{}
	for _, item := range transportsObjectSlice {
		pb := item.ToPendingBatch()
		pbSlice = append(pbSlice, pb)
	}
	return pbSlice, nil
}

func testDataDecompress() ([]map[string]json.RawMessage, error) {
	file, _ := ioutil.ReadFile("test-resources2/test_data.json.gz")
	r, err := gzip.NewReader(bytes.NewReader(file))
	if err != nil {
		return nil, err
	}
	raw, _ := ioutil.ReadAll(r)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, err
	}
	var blocks []map[string]json.RawMessage
	json.Unmarshal(raw, &blocks)
	return blocks, nil
}

func txReceiptsDecompress() ([][]map[string]json.RawMessage, error) {
	file, _ := ioutil.ReadFile("test-resources2/eth_getTransactionReceiptsByBlock.json.gz")
	r, err := gzip.NewReader(bytes.NewReader(file))
	if err != nil {
		return nil, err
	}
	raw, _ := ioutil.ReadAll(r)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, err
	}
	var blockTxReceipts [][]map[string]json.RawMessage
	json.Unmarshal(raw, &blockTxReceipts)
	return blockTxReceipts, nil
}

var register sync.Once

func testNumbers() []plugins.BlockNumberOrHash {
	var result []plugins.BlockNumberOrHash
	for i := uint64(35779967); i < uint64(35780033); i++ {
		number := plugins.BlockNumber(i)
		num := plugins.BlockNumberOrHash{
			BlockNumber: &number,
		}
		result = append(result, num)
	}
	return result
}

func connectToDatabase(cfg *config.Config) (*sql.DB, error) {

	register.Do(func() {
		sql.Register("sqlite3_hooked",
			&sqlite3.SQLiteDriver{
				ConnectHook: func(conn *sqlite3.SQLiteConn) error {
					for name, path := range cfg.Databases {
						conn.Exec(fmt.Sprintf("ATTACH DATABASE '%v' AS '%v'; PRAGMA %v.journal_mode = WAL ; PRAGMA %v.synchronous = OFF ;", path, name, name, name), nil)
					}
					return nil
				},
			})
	})

	logsdb, err := sql.Open("sqlite3_hooked", (":memory:?_sync=0&_journal_mode=WAL&_foreign_keys=off"))
	if err != nil {
		log.Error(err.Error())
	}

	return logsdb, nil
}

func NewBorAPI(db *sql.DB, cfg *config.Config) *PolygonBorService {
	return &PolygonBorService {
		db: db,
		cfg: cfg,
	}
}

func NewEthAPI(db *sql.DB, cfg *config.Config) *PolygonEthService {
	return &PolygonEthService {
		db: db,
		cfg: cfg,
	}
}



func TestIndexer(t *testing.T) {
	cfg, err := config.LoadConfig("./test-resources2/polygon_test_config.yml")
	if err != nil {
		t.Fatalf(err.Error())
	}
	db, _ := connectToDatabase(cfg)
	defer db.Close()
	// if err := migrations.MigrateBlocks(db, cfg.Chainid); err != nil {
	// 	t.Fatalf(err.Error())
	// }
	// if err := migrations.MigrateTransactions(db, cfg.Chainid); err != nil {
	// 	t.Fatalf(err.Error())
	// }
	// if err := migrations.MigrateLogs(db, cfg.Chainid); err != nil {
	// 	t.Fatalf(err.Error())
	// }
	// if err := migrations.MigrateMempool(db, cfg.Chainid); err != nil {
	// 	t.Fatalf(err.Error())
	// }
	// if err := Migrate(db, cfg.Chainid); err != nil {
	// 	t.Fatalf(err.Error())
	// }
	// batches, err := pendingBatchDecompress()
	// if err != nil {
	// 	log.Error("pending batch decompression error", "err", err.Error())
	// 	t.Fatalf("error decompressing pending batches")
	// }
	// log.Error("lenght of batches", "len", len(batches))
	// indexers := []indexer.Indexer{}
	// indexers = append(indexers, indexer.NewBlockIndexer(cfg.Chainid))
	// indexers = append(indexers, indexer.NewTxIndexer(cfg.Chainid, cfg.Eip155Block, cfg.HomesteadBlock))
	// indexers = append(indexers, indexer.NewLogIndexer(cfg.Chainid))
	// indexers = append(indexers, Indexer(cfg))

	// statements := []string{}
	// for _, idx := range indexers {
	// 	for _, pb := range batches {
	// 		group, err := idx.Index(pb)
	// 		if err != nil {
	// 			t.Fatalf(err.Error())
	// 		}
	// 		statements = append(statements, group...)
	// 	}
	// }
	// megaStatement := strings.Join(statements, ";")
	// log.Error("megastatement length", "len", len(megaStatement))
	// _, err = db.Exec(megaStatement)
	// if err != nil {
	// 	t.Fatalf(err.Error())
	// }

	// pl, err := plugins.NewPluginLoader(cfg)
	// if err != nil {
	// 	log.Error("No PluginLoader initialized", "err", err.Error())
	// }
	// pl.Initialize(cfg)
	blockNumbers := testNumbers()
	blockObject, err := testDataDecompress()
	if err != nil {
		t.Fatalf(err.Error())
	}
	blockObject, err = testDataDecompress()
	if err != nil {
		t.Fatalf(err.Error())
	}

	txReceipts, err := txReceiptsDecompress()
	if err != nil {
		t.Fatalf(err.Error())
	}
	// b := api.NewBlockAPI(db, 137, pl, cfg)
	// for i, block := range blockNumbers {
	// 	log.Info("blocks by number", "number", block)
	// 	t.Run(fmt.Sprintf("GetBlockByNumber %v", i), func(t *testing.T) {
	// 		actual, err := b.GetBlockByNumber(context.Background(), block, true)
	// 		if err != nil {
	// 			t.Fatal(err.Error())
	// 		}
	// 		for k, v := range *actual {
	// 			if k == "transactions" {
	// 				txs := v.([]map[string]interface{})
	// 				var blockTxs []map[string]json.RawMessage
	// 				json.Unmarshal(blockObject[i]["transactions"], &blockTxs)
	// 				// log.Info("txns", "len", len(blockTxs))
	// 				for j, item := range txs {
	// 					for key, value := range item {
	// 						d, err := json.Marshal(value)
	// 						if err != nil {
	// 							t.Fatalf("transaction key marshalling error on block %v  tx index %v", i, j)
	// 						}
	// 						if !bytes.Equal(d, blockTxs[j][key]) {
	// 							t.Fatalf("didnt work")
	// 						}
	// 					}
	// 				}
	// 			} else {
	// 				data, err := json.Marshal(v)
	// 				if err != nil {
	// 					t.Errorf("nope %v", k)
	// 				}
	// 				if !bytes.Equal(data, blockObject[i][k]) {
	// 					var generic interface{}
	// 					json.Unmarshal(blockObject[i][k], &generic)
	// 					log.Info("values", "data", v, "test", generic)
	// 					log.Info("pre marshal type", "type", reflect.TypeOf(v))

	// 					t.Fatalf("not equal %v %v %v %v", i, k, reflect.TypeOf(data), reflect.TypeOf(blockObject[i][k]))
	// 				}
	// 			}
	// 		}
	// 	})
	// }

	bor := NewBorAPI(db, cfg)
	eth := NewEthAPI(db, cfg)
	for i, block := range blockNumbers {
		log.Warn("blockNumber", "number", block)
		actualTxReceiptBlock, err := eth.GetTransactionReceiptsByBlock(context.Background(), block)
		if err != nil {
			t.Fatal(err.Error())
		}
		for j, receipt := range actualTxReceiptBlock {
			for key, value := range receipt {
				if key == "logs" {
					logs := value.(plugins.SortLogs)
					var controlLogs [][]map[string]json.RawMessage
					json.Unmarshal(txReceipts[i][j]["logs"], &controlLogs)
					// log.Info("lengths", "actual", len(logs), "control", len(blockTxs))
					for k, log := range logs {
						for k, v := range log {
							d, err := json.Marshal(v)
							if err != nil {
								t.Fatalf("transaction key marshalling error on block %v  tx index %v", i, j)
							}
							var controlSingleLog plugins.SortLogs
							json.Unmarshal(controlLogs[k], &controlSingleLog)
							if !bytes.Equal(d, controlSingleLog[k]) {
								t.Fatalf("nope didnt work")
							}
						}
					}
					} else {
					d, err := json.Marshal(value)
					if err != nil {
						t.Fatalf("transaction key marshalling error on block %v  tx index %v", i, j)
					}
					if !bytes.Equal(d, txReceipts[i][j][key]) {
					// var logs plugins.SortLogs
					// if err := json.Unmarshal(txReceipts[i][j][key], &logs); err != nil {log.Warn(err.Error())}
						t.Fatalf("didnt work %v", key)
					}
				}
			}
		}
		// log.Info("get snapshot", "number", block)
		// t.Run(fmt.Sprintf("GetSanpshot %v", i), func(t *testing.T) {
		actual, err := bor.GetSnapshot(context.Background(), block)
		if err != nil {
			t.Fatal(err.Error())
		}
		var number uint64
		json.Unmarshal(blockObject[i]["number"], &number)
		if actual.Number != number {
			t.Fatalf("not equal, number")
		}
		var hash types.Hash
		json.Unmarshal(blockObject[i]["hash"], &hash)
		if actual.Hash != hash {
			t.Fatalf("not equal, hash")
		}
		// var proposer Validator
		// json.Unmarshal(blockObject[i]["validatorSet"]["proposer"], &proposer)
		// if *actual.ValidatorSet.Proposer != proposer {
		// 	t.Fatalf("not equal, propser")
		// }
		log.Info("number", "number", block)
	}

}

//eth: getBlockByNumber getBlockByHash getTransactionByHash GetTransactionReceipt GetBorBlockReceipt GetTransactionReceiptsByBlock
//bor: getAuthor GetRootHash GetSignersAtHash GetCurrentValidators GetCurrentProposer
