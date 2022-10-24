package main 

import (
	"bytes"
	"fmt"
	"context"
	"strings"
	"testing"
	"sync"
	"os"
	"compress/gzip"
	"database/sql"
	"encoding/json"
	"github.com/mattn/go-sqlite3"
	"io"
	"io/ioutil"
	_ "net/http/pprof"
	// "path/filepath"

	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-streams/delivery"
	"github.com/openrelayxyz/cardinal-streams/transports"

	"github.com/openrelayxyz/flume/config"
	// "github.com/openrelayxyz/flume/indexer"
	"github.com/openrelayxyz/flume/migrations"
)

var register sync.Once

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

	if err := Migrate(logsdb, cfg.Chainid); err != nil {
		log.Error("Polygon migration error", "err", err.Error())
	}

	return logsdb, nil
}

func pendingBatchDecompress() ([]*delivery.PendingBatch, error) {
	file, _ := ioutil.ReadFile("test-resources/pending_batches.json.gz")
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

func blocksDecompress() ([]map[string]json.RawMessage, error) {
	file, _ := ioutil.ReadFile("testdata/poly_blocks.json.gz")
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

func TestPolygon(t *testing.T) {
	cfg, err := config.LoadConfig("test-resources/test_config.yml")
	if err != nil {
		t.Fatal("Error parsing config polygon test", "err", err.Error())
	}
	db, err := connectToDatabase(cfg)
	if err != nil {
		t.Fatal("Error connecting to databases polygon test", "err", err.Error())
	}
	defer db.Close()

	batches, err := pendingBatchDecompress()
	if err != nil {
		t.Fatal("Error decompressing pending batches polygon test", "err", err.Error())
	}

	megaStatement := []string{}

	// indexers := []indexer.Indexer{Indexer(cfg), indexer.NewBlockIndexer(cfg.Chainid), indexer.NewTxIndexer(cfg.Chainid, cfg.Eip155Block, cfg.HomesteadBlock), indexer.NewLogIndexer(cfg.Chainid)}

	polyIndexer := Indexer(cfg)

	for _, pendingBatch := range batches {           
		s, err := polyIndexer.Index(pendingBatch)
			if err != nil {
				t.Fatal("Error creating statements polygon test", "err", err.Error())
			}
		megaStatement = append(megaStatement, s...)
	}

	log.Info("Megastatement created successfully")

	// mut := &sync.RWMutex{}

	// mut.Lock()
	dbtx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal("Error beginning database transaction polygon test", "err", err.Error())
	}
	if _, err := dbtx.Exec(strings.Join(megaStatement, " ; ")); err != nil {
		t.Fatal("Error executing megastatement polygon test", "err", err.Error())
	}
	if err := dbtx.Commit(); err != nil {
		t.Fatal("Error commiting megastatement polygon test", "err", err.Error())
		// mut.Unlock()
	}

	// if err := os.Remove("test-resources/br.sqlite"); err != nil {
    //     t.Fatal("Error removing bor database polygon test")
    // }
	// if err := os.Remove("test-resources/*.sqlite-wal"); err != nil {
    //     t.Fatal("Error removing bor database polygon test")
    // }
	// if err := os.Remove("test-resources/*.sqlite-shm"); err != nil {
    //     t.Fatal("Error removing bor database polygon test")
    // }
}

func TestEthAPI(t *testing.T) {
	cfg, err := config.LoadConfig("test-resources/test_config.yml")
	if err != nil {
		t.Fatal("Error parsing config polygon test", "err", err.Error())
	}
	db, err := connectToDatabase(cfg)
	if err != nil {
		t.Fatal("Error connecting to databases polygon test", "err", err.Error())
	}
	defer db.Close()
	pl, _ := plugins.NewPluginLoader(cfg)
	
	eth := NewEthAPI(db, cfg)
	tm := rpcTransports.NewTransportManager(cfg.Concurrency)
	tm.AddHTTPServer(cfg.Port)
	// pluginsPath := cfg.PluginDir
	// pl, err := plugins.NewPluginLoader(pluginsPath)
	// if err != nil {
	// 	log.Error("No PluginLoader initialized", "err", err.Error())
	// }
	// pl.Initialize(cfg)
	// pluginAPIs := pl.Lookup("RegisterAPI", func(v interface{}) bool {
	// 	_, ok := v.(func(*rpcTransports.TransportManager, *sql.DB, *config.Config) error)
	// 	return ok
	// })

	// for _, api := range pluginAPIs {
	// 	fn := api.(func(*rpcTransports.TransportManager, *sql.DB, *config.Config) error)
	// 	if err := fn(tm, db, cfg); err != nil {
	// 		log.Error("Unable to load api plugins", "fn", fn)
	// 	}
	// }
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
}