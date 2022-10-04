package main

import (
	"context"
	"database/sql"
	// "websocket" ???
	"os"
	"strings"

	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-streams/transports"
	"github.com/openrelayxyz/flume/indexer"
	"github.com/openrelayxyz/flume/config"
	"github.com/openrelayxyz/flume/plugins"
)

func Initialize(cfg *config.Config, pl *plugins.PluginLoader) {
	log.Info("Re-indexer loaded")
}

func ReIndexer(cfg *config.Config, db *sql.DB, indexers *[]indexer.Indexer) error {

	pluginsPath := cfg.PluginDir
	pl, err := plugins.NewPluginLoader(pluginsPath)
	if err != nil {
		log.Error("No PluginLoader initialized", "err", err.Error())
	}

	for _, broker := range cfg.BrokerParams {
		if strings.HasPrefix(broker.URL, "ws://") || strings.HasPrefix(broker.URL, "wss://") {
			websocket := broker.URL
			log.Info("found websocket broker, reindexer", "broker", websocket) 
		}
	}

	pluginIndexers := pl.Lookup("Indexer", func(v interface{}) bool {
		_, ok := v.(func(*config.Config) indexer.Indexer)
		return ok
	})
	
	for _, fni := range pluginIndexers {
		fn := fni.(func(*config.Config) indexer.Indexer)
		idx := fn(cfg)
		if idx != nil {
			*indexers = append(*indexers, fn(cfg))
		}
	}

	// conn, err := websocket.Dial(websocket)
	// if err != nil {
	// 	log.Error("websocket connection error", "err", err)
	// }
	// defer conn.Close()
	
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
		var tb transports.TransportBatch
		rows.Scan(&number)

		// message, _ := `{"id":0, "method":"cardinal_streamsBlock", "params": [%v]}`, number
		// websocket.SendJSON(message)
		// websocket.JSONResponse()(&tb)

		for _, indexer := range *indexers {
			statements, err := indexer.Index(pb)
			if err != nil {
				log.Error("Error generating statement reindexer, on indexer", indexer, "block", number, "err", err.Error())
			}
			for _, statement := range statements {
				if statement[:6] == "DELETE" {
					continue
				} else {
					if _, err := output.Write([]byte(statement)); err != nil {
						log.Error("Error writing to output file, reindexer", "err", err)
					}
				}
			}
		}

		if _, err := output.Write(number); err != nil {
			log.Error("Error writing to output file, reindexer", "err", err)
		}
	}
		
	return nil

}

