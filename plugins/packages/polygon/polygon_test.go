package main 

import (
	"bytes"
	"fmt"
	"context"
	"strings"
	"testing"
	"sync"

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
		// for _, indexer := range indexers {
			s, err := polyIndexer.Index(pendingBatch)
				if err != nil {
					t.Fatal("Error creating statements polygon test", "err", err.Error())
				}
				megaStatement = append(megaStatement, s...)
		// }
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

	


	// pl, _ := plugins.NewPluginLoader(cfg)
	// b := NewBlockAPI(db, 1, pl)
	// expectedResult, _ := hexutil.DecodeUint64("0xd59f95")
	// test, err := b.BlockNumber(context.Background())
	// if err != nil {
	// 	t.Fatalf(err.Error())
	// }
	// if test != hexutil.Uint64(expectedResult) {
	// 	t.Fatalf("BlockNumber() result not accurate")
	// }
}