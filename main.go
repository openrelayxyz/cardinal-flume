package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"
	"net/http"
	_ "net/http/pprof"
	
	"github.com/mattn/go-sqlite3"
	log "github.com/inconshreveable/log15"
	
	rpcTransports "github.com/openrelayxyz/cardinal-rpc/transports"
	"github.com/openrelayxyz/cardinal-types/metrics"
	"github.com/openrelayxyz/cardinal-types/metrics/publishers"
	"github.com/openrelayxyz/cardinal-flume/api"
	"github.com/openrelayxyz/cardinal-flume/config"
	"github.com/openrelayxyz/cardinal-flume/indexer"
	"github.com/openrelayxyz/cardinal-flume/migrations"
	"github.com/openrelayxyz/cardinal-flume/plugins"
	"github.com/openrelayxyz/cardinal-flume/txfeed"
)

func main() {
	exitWhenSynced := flag.Bool("shutdownSync", false, "Shutdown server once sync is completed")
	ignoreBlockTime := flag.Bool("ignore.block.time", false, "Use the Cardinal offsets table instead of block times for resumption")
	resumptionTimestampMs := flag.Int64("resumption.ts", -1, "Timestamp (in ms) to resume from instead of database timestamp (requires Cardinal source)")
	genesisIndex := flag.Bool("genesisIndex", false, "index from zero")
	blockRollback := flag.Int64("block.rollback", 0, "Rollback to block N before syncing. If N < 0, rolls back from head before starting or syncing.")

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
					conn.Exec(fmt.Sprintf("ATTACH DATABASE '%v' AS '%v'; PRAGMA %v.page_size = 65536 ; PRAGMA %v.journal_mode = WAL ; PRAGMA %v.synchronous = OFF ; pragma %v.max_page_count = 4294967294;", path, name, name, name, name, name), nil)
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
		if err := api.LoadIndexHints(logsdb); err != nil {
			log.Warn("Failed to load index hints", "err", err.Error())
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

	
	var maxBlock int
	if err := logsdb.QueryRowContext(context.Background(), "SELECT max(number) FROM blocks;").Scan(&maxBlock); err != nil {
		log.Warn("sql max block query error", "err", err.Error())
		// If starting with empty databases the above will return an error which can be ignored
	}
	cfg.LatestBlock = uint64(maxBlock)

	if *blockRollback != 0 {
		rollback := *blockRollback 
		if *blockRollback < 0 {
			rollback = int64(maxBlock) + *blockRollback
		}
		if _, err := logsdb.Exec(fmt.Sprintf("DELETE FROM blocks.blocks WHERE number >= %v;", rollback)); err != nil {
			log.Error("blockRollBack error", "err", err.Error())
		}
		cfg.LatestBlock = uint64(rollback)
	}

	log.Debug("latest block config", "number", cfg.LatestBlock)

	txFeed, err := txfeed.ResolveTransactionFeed(cfg.BrokerParams[0].URL, cfg.TxTopic)
	if err != nil {
		log.Error(err.Error())
	}
	quit := make(chan struct{})
	mut := &sync.RWMutex{}

	indexes := []indexer.Indexer{}

	if hasBlocks {
		indexes = append(indexes, indexer.NewBlockIndexer(cfg.Chainid))
	}
	if hasTx {
		indexes = append(indexes, indexer.NewTxIndexer(cfg.Chainid, cfg.Eip155Block, cfg.HomesteadBlock, hasMempool))
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

	if *genesisIndex {
		err := indexer.IndexGenesis(cfg, logsdb, indexes, mut)
		if err != nil {
			log.Error("Failed to index genesis block", "err", err.Error())
			panic(err)
		} else {
			log.Info("genesis block indexed")
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

	consumer, err := AcquireConsumer(logsdb, cfg, *resumptionTimestampMs, !*ignoreBlockTime, pl)
	if err != nil {
		log.Error("error establishing consumer", "err", err.Error())
	}

	hc := &indexer.HealthCheck{}
	rhf := make(chan int64, 1024)
	go indexer.ProcessDataFeed(consumer, txFeed, logsdb, quit, cfg.Eip155Block, cfg.HomesteadBlock, mut, cfg.MempoolSlots, indexes, hc, cfg.MemTxTimeThreshold, rhf)

	tm := rpcTransports.NewTransportManager(cfg.Concurrency)
	tm.SetBlockWaitDuration(time.Duration(cfg.BlockWaitDuration) * time.Millisecond)
	tm.RegisterHeightFeed(rhf)
	tm.RegisterHealthCheck(hc)
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

	startFns := pl.Lookup("Start", func(v interface{}) bool {
		_, ok := v.(func(*sql.DB, *config.Config) func())
		return ok
	})

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
	if cfg.Brokers[0].URL != "null://" {
		for {
			if err := logsdb.QueryRowContext(context.Background(), "SELECT min(number) FROM blocks.blocks;").Scan(&minBlock); err == nil {
				log.Debug("Earliest block set", "block", minBlock)
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
	//if this > 0 then this is a light server
	cfg.EarliestBlock = uint64(minBlock)
	log.Debug("earliest block config", "number", cfg.EarliestBlock)
	if len(cfg.HeavyServer) == 0 && minBlock > cfg.MinSafeBlock {
		log.Error("Minimum block error", "Earliest log found on block:", minBlock, "Should be less than or equal to:", cfg.MinSafeBlock)
		os.Exit(1)
	}
	if !*exitWhenSynced {
		if cfg.Statsd != nil {
			publishers.StatsD(cfg.Statsd.Port, cfg.Statsd.Address, time.Duration(cfg.Statsd.Interval), cfg.Statsd.Prefix, cfg.Statsd.Minor)
		}
		if cfg.CloudWatch != nil {
			publishers.CloudWatch(cfg.CloudWatch.Namespace, cfg.CloudWatch.Dimensions, int64(cfg.Chainid), time.Duration(cfg.CloudWatch.Interval), cfg.CloudWatch.Percentiles, cfg.CloudWatch.Minor)
		}
		stopFns := make([]func(), 0, len(startFns))
		for _, v := range startFns {
			if fn, ok := v.(func(*sql.DB, *config.Config) func()); ok {
				stopFns = append(stopFns, fn(logsdb, cfg))
			}
		}
		stop := func() {
			for _, fn := range stopFns {
				fn()
			}
		}
		if err := tm.Run(cfg.HealthcheckPort); err != nil {
			log.Error(err.Error())
			stop()
			quit <- struct{}{}
			logsdb.Close()
			time.Sleep(time.Second)
			os.Exit(1)
		}
		stop()
	}
	quit <- struct{}{}
	logsdb.Close()
	metrics.Clear()
	time.Sleep(time.Second)
}
