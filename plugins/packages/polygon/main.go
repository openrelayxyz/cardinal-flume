package main

import (
	"database/sql"

	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/flume/config"
	"github.com/openrelayxyz/flume/indexer"
	"github.com/openrelayxyz/flume/plugins"
	rpcTransports "github.com/openrelayxyz/cardinal-rpc/transports"
)

func Initialize(cfg *config.Config, pl *plugins.PluginLoader) {
	log.Info("Polygon plugin loaded")
}

func Indexer(cfg *config.Config) indexer.Indexer {
	return &PolygonIndexer{Chainid: cfg.Chainid}
}

func RegisterAPI(tm *rpcTransports.TransportManager, db *sql.DB, cfg *config.Config) error {
	tm.Register("eth", &PolygonEthService{
			db: db,
			cfg: cfg,
	})
	log.Info("PolygonService registered")
	tm.Register("bor", &PolygonBorService{
		db: db,
		cfg: cfg,
	})
	log.Info("PolygonBorService registered")
	return nil	
} 
