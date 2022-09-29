package main

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"testing"
	"os"
	// "net/http"
	// "net/url"
	// "flag"

	"github.com/openrelayxyz/cardinal-evm/vm"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-types/hexutil"
	"github.com/openrelayxyz/flume/migrations"
	"github.com/openrelayxyz/flume/plugins"
	"github.com/openrelayxyz/flume/api"
	"github.com/openrelayxyz/flume/config"
	rpcTransports "github.com/openrelayxyz/cardinal-rpc/transports"

	"compress/gzip"
	"database/sql"
	"encoding/json"
	log "github.com/inconshreveable/log15"
	"github.com/mattn/go-sqlite3"
	"io"
	"io/ioutil"
	_ "net/http/pprof"
	"path/filepath"
	"sync"
)

// 27005120, 27006145

var register sync.Once

func connectToDatabase() (*sql.DB, error) {
	sqlitePath := "testdata/testdata.sqlite"

	mempoolDb := filepath.Join(filepath.Dir(sqlitePath), "mempool.sqlite")
	blocksDb := filepath.Join(filepath.Dir(sqlitePath), "blocks.sqlite")
	txDb := filepath.Join(filepath.Dir(sqlitePath), "transactions.sqlite")
	logsDb := filepath.Join(filepath.Dir(sqlitePath), "logs.sqlite")
	borDb := filepath.Join(filepath.Dir(sqlitePath), "bor.sqlite")

	register.Do(func() {
		sql.Register("sqlite3_hooked",
			&sqlite3.SQLiteDriver{
				ConnectHook: func(conn *sqlite3.SQLiteConn) error {
					conn.Exec(fmt.Sprintf("ATTACH DATABASE '%v' AS 'mempool'; PRAGMA mempool.journal_mode = WAL ; PRAGMA mempool.synchronous = OFF ;", mempoolDb), nil)
					conn.Exec(fmt.Sprintf("ATTACH DATABASE '%v' AS 'blocks'; PRAGMA block.journal_mode = WAL ; PRAGMA block.synchronous = OFF ;", blocksDb), nil)
					conn.Exec(fmt.Sprintf("ATTACH DATABASE '%v' AS 'transactions'; PRAGMA transactions.journal_mode = WAL ; PRAGMA transactions.synchronous = OFF ;", txDb), nil)
					conn.Exec(fmt.Sprintf("ATTACH DATABASE '%v' AS 'logs'; PRAGMA logs.journal_mode = WAL ; PRAGMA logs.synchronous = OFF ;", logsDb), nil)
					conn.Exec(fmt.Sprintf("ATTACH DATABASE '%v' AS 'bor'; PRAGMA bor.journal_mode = WAL ; PRAGMA bor.synchronous = OFF ;", borDb), nil)

					return nil
				},
			})
	})

	logsdb, err := sql.Open("sqlite3_hooked", fmt.Sprintf("file:%v?_sync=0&_journal_mode=WAL&_foreign_keys=off", sqlitePath))

	chainid := uint64(1)

	if err := migrations.MigrateBlocks(logsdb, chainid); err != nil {
		return nil, err
	}
	if err := migrations.MigrateTransactions(logsdb, chainid); err != nil {
		return nil, err
	}
	if err := migrations.MigrateLogs(logsdb, chainid); err != nil {
		return nil, err
	}
	if err := migrations.MigrateMempool(logsdb, chainid); err != nil {
		return nil, err
	}

	if err != nil {
		return nil, err
	}
	return logsdb, nil
}

func snapshotsDecompress() ([]*Snapshot, error) {
	file, _ := ioutil.ReadFile("testdata/snapshots.json.gz")
	r, err := gzip.NewReader(bytes.NewReader(file))
	if err != nil {
		return nil, err
	}
	raw, _ := ioutil.ReadAll(r)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, err
	}
	var Snapshots []*Snapshot
	json.Unmarshal(raw, &Snapshots)
	return Snapshots, nil
}

func snapshotsDecompress2() ([]*Snapshot, error) {
	file, _ := ioutil.ReadFile("testdata/snapshots2.json.gz")
	r, err := gzip.NewReader(bytes.NewReader(file))
	if err != nil {
		return nil, err
	}
	raw, _ := ioutil.ReadAll(r)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, err
	}
	var Snapshots []*Snapshot
	json.Unmarshal(raw, &Snapshots)
	return Snapshots, nil
}

func roothashDecompress() ([]string, error) {
	file, _ := ioutil.ReadFile("testdata/roothash.json.gz")
	r, err := gzip.NewReader(bytes.NewReader(file))
	if err != nil {
		return nil, err
	}
	raw, _ := ioutil.ReadAll(r)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, err
	}
	var hashes []string
	json.Unmarshal(raw, &hashes)
	return hashes, nil
}

func txReceiptsDecompress() ([]map[string]json.RawMessage, error) {
	file, _ := ioutil.ReadFile("testdata/transaction_receipts.json.gz")
	r, err := gzip.NewReader(bytes.NewReader(file))
	if err != nil {
		return nil, err
	}
	raw, _ := ioutil.ReadAll(r)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, err
	}
	var receipts []map[string]json.RawMessage
	json.Unmarshal(raw, &receipts)
	return receipts, nil
}

func blocksDecompress() ([]map[string]json.RawMessage, error) {
	file, _ := ioutil.ReadFile("testdata/blocks.json.gz")
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

func getBlockSubset(jsonBlockObject []map[string]json.RawMessage) ([]map[string]json.RawMessage, error) {
	result := []map[string]json.RawMessage{}
	for _, block := range jsonBlockObject {
		var x vm.BlockNumber
		json.Unmarshal(block["number"], &x)
		if x.Int64() > 27005100 && x.Int64() <= 27005184 {
			result = append(result, block)
		}
	}
	log.Info("subset", "len", len(result))
	return result, nil
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

func getTransactionsForTesting(blockObject []map[string]json.RawMessage) []map[string]json.RawMessage {
	result := []map[string]json.RawMessage{}
	for _, block := range blockObject {
		txns := []map[string]json.RawMessage{}
		json.Unmarshal(block["transactions"], &txns)
		result = append(result, txns...)
	}
	log.Info("result", "len", len(result))
	return result
}

func getTransactionHashes(blockObject []map[string]json.RawMessage) []types.Hash {
	result := []types.Hash{}
	for _, block := range blockObject {
		txnLevel := []map[string]interface{}{}
		json.Unmarshal(block["transactions"], &txnLevel)
		if len(txnLevel) > 0 {
			for _, tx := range txnLevel {
				result = append(result, types.HexToHash(tx["hash"].(string)))
			}
		}
	}
	return result
}

func TestEthAPI(t *testing.T) {
	db, err := connectToDatabase()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer db.Close()
	cfg, err := config.LoadConfig("testdata/config.yml")
	if err != nil {
		log.Error("Error parsing config", "err", err)
		os.Exit(1)
	}
	eth := NewEthAPI(db, cfg)
	tm := rpcTransports.NewTransportManager(cfg.Concurrency)
	tm.AddHTTPServer(cfg.Port)
	pluginsPath := cfg.PluginDir
	pl, err := plugins.NewPluginLoader(pluginsPath)
	if err != nil {
		log.Error("No PluginLoader initialized", "err", err.Error())
	}
	pl.Initialize(cfg)
	pluginAPIs := pl.Lookup("RegisterAPI", func(v interface{}) bool {
		_, ok := v.(func(*rpcTransports.TransportManager, *sql.DB, *config.Config) error)
		return ok
	})

	for _, api := range pluginAPIs {
		fn := api.(func(*rpcTransports.TransportManager, *sql.DB, *config.Config) error)
		if err := fn(tm, db, cfg); err != nil {
			log.Error("Unable to load api plugins", "fn", fn)
		}
	}
	// pl, _ := plugins.NewPluginLoader("")
	t.Run(fmt.Sprintf("ChainId"), func(t *testing.T) {
		actual := eth.ChainId(context.Background())
		log.Info("chain id", "id", actual)
		if actual != hexutil.Uint64(137) {
			t.Fatal(err.Error())
		}
	})
	b := api.NewBlockAPI(db, 137, pl)
	bObject, _ := blocksDecompress()
	blockObject, _ := getBlockSubset(bObject)
	blockNumbers := getBlockNumbers(blockObject)
	log.Info("numbers", "len", len(blockNumbers))
	for i, block := range blockNumbers {
		log.Info("blocks by number", "number", block)
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
					// log.Info("txns", "len", len(blockTxs))
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
	}

		blockHashes := getBlockHashes(blockObject)
		log.Info("hashes", "len", len(blockHashes))
		for i, hash := range blockHashes {
			log.Info("blocks by hash")
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
		}
		tx := api.NewTransactionAPI(db, 137, pl)
		transactions := getTransactionsForTesting([]map[string]json.RawMessage{blockObject[0]})
		log.Info("tx list", "len", len(transactions))
		receipts, _ := txReceiptsDecompress()
		log.Info("receipts list", "len", len(receipts))
		txHashes := getTransactionHashes([]map[string]json.RawMessage{blockObject[0]})
		log.Info("hash list", "len", len(txHashes))
		for i, hash := range txHashes {
			t.Run(fmt.Sprintf("GetTransactionByHash %v", i), func(t *testing.T) {
				actual, err := tx.GetTransactionByHash(context.Background(), hash)
				if err != nil {
					t.Fatal(err.Error())
				}
				for k, v := range actual {
					data, err := json.Marshal(v)
					if err != nil {
						t.Errorf("marshalling error gtbh on key: %v", k)
					}
					if !bytes.Equal(data, transactions[i][k]) {
						t.Fatalf("error on transaction %v, key%v", hash, k)
					}
				}
			})
			// t.Run(fmt.Sprintf("GetTransactionReceipt %v", i), func(t *testing.T) {
			// 	actual, err := tx.GetTransactionReceipt(context.Background(), hash)
			// 	log.Info("thing", "receipts i", receipts[i], "index", i)
			// 	if err != nil {
			// 		t.Fatal(err.Error())
			// 	}
			// 	log.Info("hashes", "index", i, "hash", hash, "test", actual["transactionHash"], "control", plugins.BytesToHash(receipts[i]["transactionHash"]))
			// 	for k, v := range actual {
			// 		data, err := json.Marshal(v)
			// 		if err != nil {
			// 			t.Errorf("marshalling error gtr on key: %v", k)
			// 		}
			// 		if !bytes.Equal(data, receipts[i][k]) {
			// 			t.Fatalf("error on receipts %v, key%v", hash, k)
			// 		}
			// 	}
			// })
		}
	}

func TestBorAPI(t *testing.T) {
	db, err := connectToDatabase()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer db.Close()
	cfg, err := config.LoadConfig("testdata/config.yml")
	if err != nil {
		log.Error("Error parsing config", "err", err)
		os.Exit(1)
	}
	bor := NewBorAPI(db, cfg)
	tm := rpcTransports.NewTransportManager(cfg.Concurrency)
	tm.AddHTTPServer(cfg.Port)
	pluginsPath := cfg.PluginDir
	pl, err := plugins.NewPluginLoader(pluginsPath)
	if err != nil {
		log.Error("No PluginLoader initialized", "err", err.Error())
	}
	pl.Initialize(cfg)
	pluginAPIs := pl.Lookup("RegisterAPI", func(v interface{}) bool {
		_, ok := v.(func(*rpcTransports.TransportManager, *sql.DB, *config.Config) error)
		return ok
	})

	for _, api := range pluginAPIs {
		fn := api.(func(*rpcTransports.TransportManager, *sql.DB, *config.Config) error)
		if err := fn(tm, db, cfg); err != nil {
			log.Error("Unable to load api plugins", "fn", fn)
		}
	}
	snaps, _ := snapshotsDecompress2()
	blockNumbers := []plugins.BlockNumberOrHash{}
	for _, snap := range snaps {
		x := plugins.BlockNumberOrHashWithNumber(plugins.BlockNumber(snap.Number))
		
		blockNumbers = append(blockNumbers, x)
	}
	for i, number := range blockNumbers {
		t.Run(fmt.Sprintf("GetSnapshot %v", i), func(t *testing.T) {
			actual, err := bor.GetSnapshot(context.Background(), number)
			if err != nil {
				t.Fatal(err.Error())
			}
			if actual.Number != snaps[i].Number {
				t.Fatalf("innacurate snapshot number on block %v", number)
			}
			if actual.Hash != snaps[i].Hash {
				t.Fatalf("innacurate snapshot hash on block %v", number)
			}
			for k, v := range actual.Recents {
				if v != snaps[i].Recents[k] {
					t.Fatalf("innacurate recents map on block %v", number)
				}
			}
			for j, val := range actual.ValidatorSet.Validators {
				if *val != *snaps[i].ValidatorSet.Validators[j] {
					t.Fatalf("innacurate validator on index %v, block %v, val %v, control %v", j, number, val, snaps[i].ValidatorSet.Validators[j])
				}
			}
			if *actual.ValidatorSet.Proposer != *snaps[i].ValidatorSet.Proposer {
				t.Fatalf("innacurate proposer on block %v", number)
			}
		})
	}
	rootHashes, _ := roothashDecompress()
	start := uint64(0)
	end := uint64(0)
	for i := 0; i >= len(blockNumbers); i++ {
		end += 25
		t.Run(fmt.Sprintf("GetRootHash %v", i), func(t *testing.T) {
			log.Info("s and e", "start", start, "end", end)
			actual, err := bor.GetRootHash(context.Background(), start, end)
			if err != nil {
				t.Fatal(err.Error())
			}
			if actual != rootHashes[i] {
				t.Fatal(err.Error())
			}
		})	
	} 
}



// type Snapshot struct {
// 	Number       uint64                    `json:"number"`       // Block number where the snapshot was created
// 	Hash         types.Hash               `json:"hash"`         // Block hash where the snapshot was created
// 	ValidatorSet *ValidatorSet      `json:"validatorSet"` // Validator set at this moment
// 	Recents      map[uint64]common.Address `json:"recents"`      // Set of recent signers for spam protections
// }

// type ValidatorSet struct {
// 	Validators []*Validator `json:"validators"`
// 	Proposer   *Validator   `json:"proposer"`
// 	totalVotingPower int64
// 	validatorsMap    map[common.Address]int // address -> index
// }

// type Validator struct {
// 	ID               uint64         `json:"ID"`
// 	Address          common.Address `json:"signer"`
// 	VotingPower      int64          `json:"power"`
// 	ProposerPriority int64          `json:"accum"`
// }

