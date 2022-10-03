package main

import (
	"context"
	"database/sql"
	// "encoding/binary"
	// "regexp"
	// "strconv"
	// "io"
	"os"
	// "fmt"
	"flag"
	// "math/big"


	log "github.com/inconshreveable/log15"
	// "github.com/mattn/go-sqlite3"
	// "github.com/openrelayxyz/cardinal-evm/common"
	// "github.com/openrelayxyz/cardinal-evm/crypto"
	// "github.com/openrelayxyz/cardinal-evm/rlp"
	// evm "github.com/openrelayxyz/cardinal-evm/types"
	// "github.com/openrelayxyz/cardinal-streams/delivery"
	// "github.com/openrelayxyz/cardinal-types"
	// "github.com/openrelayxyz/flume/indexer"
	"github.com/openrelayxyz/flume/config"
	"github.com/openrelayxyz/flume/plugins"
)

//query attached db for missing blocks. 

// make list of missing blocks from sql Query

// pass list as arguement to function

// recruit indexers to generate insert statments to sql

// (optional) query sql to confirm that all blocks from list are present

func Initialize(cfg *config.Config, pl *plugins.PluginLoader) {
	log.Info("Re-indexer loaded")
}


func ReIndexer(cfg *config.Config, db *sql.DB) error {

	cfg, err := config.LoadConfig(flag.CommandLine.Args()[0])
	if err != nil {
		log.Error("Error parsing config", "err", err)
		os.Exit(1)
	}

	output, err := os.Create("output.txt")
    if err != nil {
        log.Error("Error opening output file, reindexer", "err", err)
    }

	defer func() {
        if err := output.Close(); err != nil {
            log.Error("Error closing output file, reindexer", "err", err)
        }
    }()

	rows, _ := db.QueryContext(context.Background(), "SELECT number + 1 FROM blocks WHERE number + 1 NOT IN (SELECT number FROM blocks);")
	defer rows.Close()

	for rows.Next() {
		var number []byte
		rows.Scan(&number)
		if _, err := output.Write(number); err != nil {
            log.Error("Error writing to output file, reindexer", "err", err)
        }
	}

	return nil

}

