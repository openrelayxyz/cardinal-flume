package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	log "github.com/inconshreveable/log15"
	"github.com/mattn/go-sqlite3"
	rpcTransports "github.com/openrelayxyz/cardinal-rpc/transports"
	"github.com/openrelayxyz/cardinal-types/metrics"
	"github.com/openrelayxyz/cardinal-types/metrics/publishers"
	"github.com/openrelayxyz/flume/api"
	"github.com/openrelayxyz/flume/config"
	"github.com/openrelayxyz/flume/indexer"
	"github.com/openrelayxyz/flume/migrations"
	"github.com/openrelayxyz/flume/plugins"
	"github.com/openrelayxyz/flume/txfeed"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sync"
	"time"
)

func main() {
	exitWhenSynced := flag.Bool("shutdownSync", false, "Shutdown server once sync is completed")
	resumptionTimestampMs := flag.Int64("resumption.ts", -1, "Timestamp (in ms) to resume from instead of database timestamp (requires Cardinal source)")

	flag.CommandLine.Parse(os.Args[1:])

	cfg, err := config.LoadConfig(flag.CommandLine.Args()[0])
	if err != nil {
		log.Error("Error parsing config", "err", err)
		os.Exit(1)
	}

	pl, err := plugins.NewPluginLoader(cfg)
	if err != nil {
		log.Error("No PluginLoader initialized", "err", err.Error())
 	}

	pl.Initialize(cfg)

	sql.Register("sqlite3_hooked",
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				for name, path := range cfg.Databases {
					conn.Exec(fmt.Sprintf("ATTACH DATABASE '%v' AS '%v'; PRAGMA %v.journal_mode = WAL ; PRAGMA %v.synchronous = OFF ;", path, name, name, name), nil)
				}
				return nil
			},
		})

	logsdb, err := sql.Open("sqlite3_hooked", (":memory:?_sync=0&_journal_mode=WAL&_foreign_keys=off"))
	if err != nil {
		log.Error(err.Error())
	}

	logsdb.SetConnMaxLifetime(0)
	logsdb.SetMaxIdleConns(32)
	go func() {
		connectionsGauge := metrics.NewMajorGauge("/flume/connections")
		inUseGauge := metrics.NewMajorGauge("/flume/inUse")
		idleGauge := metrics.NewMajorGauge("/flume/idle")
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			stats := logsdb.Stats()
			connectionsGauge.Update(int64(stats.OpenConnections))
			inUseGauge.Update(int64(stats.InUse))
			idleGauge.Update(int64(stats.Idle))
		}
	}()
	if cfg.PprofPort > 0 {
		p := &http.Server{
			Addr:              fmt.Sprintf(":%v", cfg.PprofPort),
			Handler:           http.DefaultServeMux,
			ReadHeaderTimeout: 5 * time.Second,
			IdleTimeout:       120 * time.Second,
			MaxHeaderBytes:    1 << 20,
		}
		go p.ListenAndServe()
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

	pluginMigrations := pl.Lookup("Migrate", func(v interface{}) bool {
		_, ok := v.(func(*sql.DB, uint64) error)
		return ok
	})

	for _, mgt := range pluginMigrations {
		fn := mgt.(func(*sql.DB, uint64) error)
		if err := fn(logsdb, cfg.Chainid); err != nil {
			log.Error("Unable to migrate from plugin", "err", err.Error())
		}
	}

	txFeed, err := txfeed.ResolveTransactionFeed(cfg.BrokerParams[0].URL, cfg.TxTopic)
	if err != nil {
		log.Error(err.Error())
	}
	quit := make(chan struct{})
	mut := &sync.RWMutex{}

	// func AquireConsumer(db *sql.DB, cfg *config.Config, resumptionTime int64)
	consumer, _ := AquireConsumer(logsdb, cfg, *resumptionTimestampMs) 
	indexes := []indexer.Indexer{}

	if hasBlocks {
		indexes = append(indexes, indexer.NewBlockIndexer(cfg.Chainid))
	}
	if hasTx {
		indexes = append(indexes, indexer.NewTxIndexer(cfg.Chainid, cfg.Eip155Block, cfg.HomesteadBlock))
	}
	if hasLogs {
		indexes = append(indexes, indexer.NewLogIndexer(cfg.Chainid))
	}

	pluginIndexers := pl.Lookup("Indexer", func(v interface{}) bool {
		_, ok := v.(func(*config.Config) indexer.Indexer)
		return ok
	})

	for _, fni := range pluginIndexers {
		fn := fni.(func(*config.Config) indexer.Indexer)
		idx := fn(cfg)
		if idx != nil {
			indexes = append(indexes, fn(cfg))
		}
	}

	pluginReIndexers := pl.Lookup("ReIndexer", func(v interface{}) bool {
		_, ok := v.(func(*config.Config, *sql.DB, []indexer.Indexer) error)
		return ok
	})

	var reIndexed bool = false

	for _, fni := range pluginReIndexers {
		fn := fni.(func(*config.Config, *sql.DB, []indexer.Indexer) error)
		reIndexed = true
		if err := fn(cfg, logsdb, indexes); err != nil {
			log.Error("Unable to load reindexer plugins", "fn", fn)
		}
	}

	if reIndexed == true {
		return 
	}

	go indexer.ProcessDataFeed(consumer, txFeed, logsdb, quit, cfg.Eip155Block, cfg.HomesteadBlock, mut, cfg.MempoolSlots, indexes) //[]indexer

	tm := rpcTransports.NewTransportManager(cfg.Concurrency)
	tm.AddHTTPServer(cfg.Port)

	pluginAPIs := pl.Lookup("RegisterAPI", func(v interface{}) bool {
		_, ok := v.(func(*rpcTransports.TransportManager, *sql.DB, *config.Config) error)
		return ok
	})

	for _, api := range pluginAPIs {
		fn := api.(func(*rpcTransports.TransportManager, *sql.DB, *config.Config) error)
		if err := fn(tm, logsdb, cfg); err != nil {
			log.Error("Unable to load api plugins", "fn", fn)
		}
	}

	if hasLogs {
		tm.Register("eth", api.NewLogsAPI(logsdb, cfg.Chainid, pl, cfg))
		tm.Register("flume", api.NewFlumeTokensAPI(logsdb, cfg.Chainid, pl, cfg))
	}
	if hasTx && hasBlocks {
		tm.Register("eth", api.NewBlockAPI(logsdb, cfg.Chainid, pl, cfg))
		tm.Register("eth", api.NewGasAPI(logsdb, cfg.Chainid, pl, cfg))
	}
	if hasTx && hasBlocks && hasLogs && hasMempool {
		tm.Register("eth", api.NewTransactionAPI(logsdb, cfg.Chainid, pl, cfg))
		tm.Register("flume", api.NewFlumeAPI(logsdb, cfg.Chainid, pl, cfg))
	}
	tm.Register("debug", &metrics.MetricsAPI{})

	<-consumer.Ready()
	var minBlock int
	//if this > 0 then this is a light server
	logsdb.QueryRowContext(context.Background(), "SELECT min(block) FROM event_logs;").Scan(&minBlock)
	cfg.EarliestBlock = uint64(minBlock)
	log.Debug("earliest block config", "number", cfg.EarliestBlock)
	if len(cfg.HeavyServer) == 0 && minBlock > cfg.MinSafeBlock {
		log.Error("Minimum block error", "Earliest log found on block:", minBlock, "Should be less than or equal to:", cfg.MinSafeBlock)
		os.Exit(1) 
	}
	if !*exitWhenSynced {
		if err := tm.Run(cfg.HealthcheckPort); err != nil {
			log.Error(err.Error())
			quit <- struct{}{}
			logsdb.Close()
			time.Sleep(time.Second)
			os.Exit(1)
		}
	}
	if cfg.Statsd != nil {
		publishers.StatsD(cfg.Statsd.Port, cfg.Statsd.Address, time.Duration(cfg.Statsd.Interval), cfg.Statsd.Prefix, cfg.Statsd.Minor)
	}
	if cfg.CloudWatch != nil {
		publishers.CloudWatch(cfg.CloudWatch.Namespace, cfg.CloudWatch.Dimensions, int64(cfg.Chainid), time.Duration(cfg.CloudWatch.Interval), cfg.CloudWatch.Percentiles, cfg.CloudWatch.Minor)
	}
	quit <- struct{}{}
	logsdb.Close()
	metrics.Clear()
	time.Sleep(time.Second)
}
