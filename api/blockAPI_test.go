package api

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/mattn/go-sqlite3"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"sync"
	"testing"

	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-evm/vm"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-types/hexutil"
	"github.com/openrelayxyz/cardinal-flume/config"
	"github.com/openrelayxyz/cardinal-flume/migrations"
	"github.com/openrelayxyz/cardinal-flume/plugins"
	_ "net/http/pprof"
)

var register sync.Once

func connectToDatabase() (*sql.DB, error) {

	cfg, err := config.LoadConfig("../testing-resources/api_test_config.yml")
	if err != nil {
		log.Error("Error parsing config", "err", err.Error())
		os.Exit(1)
	}

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

	_, hasLogs := cfg.Databases["logs"]
	if hasLogs {
		log.Info("has logs", "logs", cfg.Databases["logs"])
	}
	_, hasBlocks := cfg.Databases["blocks"]
	if hasBlocks {
		log.Info("has blocks", "blocks", cfg.Databases["blocks"])
	}
	_, hasTx := cfg.Databases["transactions"]
	if hasTx {
		log.Info("has transactions", "transactions", cfg.Databases["transactions"])
	}
	_, hasMempool := cfg.Databases["mempool"]
	if hasMempool {
		log.Info("has mempool", "mempool", cfg.Databases["mempool"])
	}

	if hasBlocks {
		if err := migrations.MigrateBlocks(logsdb, cfg.Chainid); err != nil {
			log.Error(err.Error())
		}
	}
	if hasTx {
		if err := migrations.MigrateTransactions(logsdb, cfg.Chainid); err != nil {
			log.Error(err.Error())
		}
	}
	if hasLogs {
		if err := migrations.MigrateLogs(logsdb, cfg.Chainid); err != nil {
			log.Error(err.Error())
		}
	}
	if hasMempool {
		if err := migrations.MigrateMempool(logsdb, cfg.Chainid); err != nil {
			log.Error(err.Error())
		}
	}

	return logsdb, nil
}

func blocksDecompress() ([]map[string]json.RawMessage, error) {
	file, _ := ioutil.ReadFile("../testing-resources/block_test_data.json.gz")
	r, err := gzip.NewReader(bytes.NewReader(file))
	if err != nil {
		return nil, err
	}
	raw, _ := ioutil.ReadAll(r)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, err
	}
	var blocksObject []map[string]json.RawMessage
	json.Unmarshal(raw, &blocksObject)
	return blocksObject, nil
}

func receiptsDecompress() ([]map[string]json.RawMessage, error) {
	file, _ := ioutil.ReadFile("../testing-resources/receipt_test_data.json.gz")
	r, err := gzip.NewReader(bytes.NewReader(file))
	if err != nil {
		return nil, err
	}
	raw, _ := ioutil.ReadAll(r)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, err
	}
	var receiptsObject []map[string]json.RawMessage
	json.Unmarshal(raw, &receiptsObject)
	return receiptsObject, nil
}

func getBlockNumbers(jsonBlockObject []map[string]json.RawMessage) []vm.BlockNumber {
	result := []vm.BlockNumber{}
	for _, block := range jsonBlockObject {
		var x vm.BlockNumber
		json.Unmarshal(block["number"], &x)
		result = append(result, x)
	}
	return result
}

func getBlockHashes(jsonBlockObject []map[string]json.RawMessage) []types.Hash {
	result := []types.Hash{}
	for _, block := range jsonBlockObject {
		var x types.Hash
		json.Unmarshal(block["hash"], &x)
		result = append(result, x)
	}
	return result
}

func TestBlockNumber(t *testing.T) {
	db, err := connectToDatabase()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer db.Close()
	cfg, err := config.LoadConfig("../testing-resources/api_test_config.yml")
	if err != nil {
		t.Fatal("Error parsing config", "err", err.Error())
	}
	pl, _ := plugins.NewPluginLoader(cfg)
	b := NewBlockAPI(db, 1, pl, cfg)
	expectedResult, _ := hexutil.DecodeUint64("0xd59f95")
	test, err := b.BlockNumber(context.Background())
	if err != nil {
		t.Fatalf(err.Error())
	}
	if test != hexutil.Uint64(expectedResult) {
		t.Fatalf("BlockNumber() result not accurate")
	}
}

func TestBlockAPI(t *testing.T) {
	db, err := connectToDatabase()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer db.Close()
	cfg, err := config.LoadConfig("../testing-resources/api_test_config.yml")
	if err != nil {
		t.Fatal("Error parsing config", "err", err.Error())
	}
	pl, _ := plugins.NewPluginLoader(cfg)
	b := NewBlockAPI(db, 1, pl, cfg)
	blockObject, _ := blocksDecompress()
	blockNumbers := getBlockNumbers(blockObject)
	for i, block := range blockNumbers {
		t.Run(fmt.Sprintf("GetBlockByNumber %v", i), func(t *testing.T) {
			actual, err := b.GetBlockByNumber(context.Background(), block, true)
			if err != nil {
				t.Fatal(err.Error())
			}
			for k, v := range actual {
				if k == "transactions" {
					txs := v.([]map[string]interface{})
					var blockTxs []map[string]json.RawMessage
					json.Unmarshal(blockObject[i]["transactions"], &blockTxs)
					for j, item := range txs {
						for key, value := range item {
							d, err := json.Marshal(value)
							if err != nil {
								t.Fatalf("transaction key marshalling error on block %v  tx index %v", i, j)
							}
							if !bytes.Equal(d, blockTxs[j][key]) {
								t.Fatalf("didnt work")
							}

						}
					}
				} else {
					data, err := json.Marshal(v)
					if err != nil {
						t.Errorf("nope %v", k)
					}
					if !bytes.Equal(data, blockObject[i][k]) {
						var generic interface{}
						json.Unmarshal(blockObject[i][k], &generic)
						log.Info("values", "data", v, "test", generic)
						log.Info("pre marshal type", "type", reflect.TypeOf(v))

						t.Fatalf("not equal %v %v %v %v", i, k, reflect.TypeOf(data), reflect.TypeOf(blockObject[i][k]))
					}
				}
			}
		})

		t.Run("GetBlockTransactionCountByNumber", func(t *testing.T) {
			actual, err := b.GetBlockTransactionCountByNumber(context.Background(), block)
			if err != nil {
				t.Fatal(err.Error())
			}
			var txSlice []map[string]interface{}
			json.Unmarshal(blockObject[i]["transactions"], &txSlice)
			if actual != hexutil.Uint64(len(txSlice)) {
				t.Fatalf("transaction count by block %v %v", actual, hexutil.Uint64(len(txSlice)))
			}
		})

		t.Run("GetUncleCountByBlockNumber", func(t *testing.T) {
			actual, err := b.GetUncleCountByBlockNumber(context.Background(), block)
			if err != nil {
				t.Fatal(err.Error())
			}
			var uncleSlice []types.Hash
			json.Unmarshal(blockObject[i]["uncles"], &uncleSlice)
			if actual != hexutil.Uint64(len(uncleSlice)) {
				t.Fatalf("uncle count by block %v %v", actual, hexutil.Uint64(len(uncleSlice)))
			}
		})
		blockHashes := getBlockHashes(blockObject)
		for i, hash := range blockHashes {
			t.Run(fmt.Sprintf("GetBlockByHash %v", i), func(t *testing.T) {
				actual, err := b.GetBlockByHash(context.Background(), hash, true)
				if err != nil {
					t.Fatal(err.Error())
				}
				for k, v := range actual {
					if k == "transactions" {
						txs := v.([]map[string]interface{})
						var blockTxs []map[string]json.RawMessage
						json.Unmarshal(blockObject[i]["transactions"], &blockTxs)
						for j, item := range txs {
							for key, value := range item {
								d, err := json.Marshal(value)
								if err != nil {
									t.Fatalf("transaction key marshalling error on block %v  tx index %v", i, j)
								}
								if !bytes.Equal(d, blockTxs[j][key]) {
									t.Fatalf("didnt work")
								}

							}
						}
					} else {
						data, err := json.Marshal(v)
						if err != nil {
							t.Errorf("nope %v", k)
						}
						if !bytes.Equal(data, blockObject[i][k]) {
							log.Info("pre marshal type", "type", reflect.TypeOf(v))
							t.Fatalf("not equal %v %v %v %v", i, k, reflect.TypeOf(data), reflect.TypeOf(blockObject[i][k]))
						}
					}
				}
			})
			t.Run("GetBlockTransactionCountByHash", func(t *testing.T) {
				actual, err := b.GetBlockTransactionCountByHash(context.Background(), hash)
				if err != nil {
					t.Fatal(err.Error())
				}
				var txSlice []map[string]interface{}
				json.Unmarshal(blockObject[i]["transactions"], &txSlice)
				if actual != hexutil.Uint64(len(txSlice)) {
					t.Fatalf("transaction count by hash %v %v", actual, hexutil.Uint64(len(txSlice)))
				}
			})

			t.Run("GetUncleCountByBlockHash", func(t *testing.T) {
				actual, err := b.GetUncleCountByBlockHash(context.Background(), hash)
				if err != nil {
					t.Fatal(err.Error())
				}
				var uncleSlice []types.Hash
				json.Unmarshal(blockObject[i]["uncles"], &uncleSlice)
				if actual != hexutil.Uint64(len(uncleSlice)) {
					t.Fatalf("uncle count by hash %v %v", actual, hexutil.Uint64(len(uncleSlice)))
				}
			})
		}
	}
}
