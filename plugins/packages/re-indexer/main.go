package main

import (
	"fmt"
	"context"
	"database/sql"
	"os"
	"strings"
	"net/http"
	"time"
	"encoding/json"
	"reflect"

	"github.com/gorilla/websocket"
	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-streams/transports"
	"github.com/openrelayxyz/cardinal-types/hexutil"
	"github.com/openrelayxyz/flume/indexer"
	"github.com/openrelayxyz/flume/config"
	"github.com/openrelayxyz/flume/plugins"
)

func Initialize(cfg *config.Config, pl *plugins.PluginLoader) {
	log.Info("Re-indexer loaded")
}

type message struct {
	Id int          `json:"id"`
	Method string   `json:"method"`
	Params []string `json:"params"`
}

type resultMessage struct {
	Type string `json:"type"`
	Batch *transports.TransportBatch `json:"batch,omitempty"`
}

type outerResult struct {
	Result  *resultMessage `json:"result"`
	JsonRPC string         `json:"jsonrpc"`
	Id		int			   `json:"id"`
}

func ReIndexer(cfg *config.Config, db *sql.DB, indexers []indexer.Indexer) error {

	var wsURL string

	for _, broker := range cfg.BrokerParams {
		if strings.HasPrefix(broker.URL, "ws://") || strings.HasPrefix(broker.URL, "wss://") {
			wsURL = broker.URL
			break
			log.Info("found websocket broker, reindexer", "broker", wsURL) 
		}
	}
	
	dialer := &websocket.Dialer{
		EnableCompression: true,
		Proxy: http.ProxyFromEnvironment,
		HandshakeTimeout: 45 * time.Second,
	  }
	
	conn, _, err := dialer.Dial(wsURL, nil)
      if err != nil {
		log.Error("Websocket dial error, reindexer", "err", err.Error())
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

	var firstBlock uint64

	var lastBlock uint64

	idx := 0
		
	rows, _ := db.QueryContext(context.Background(), "SELECT number + 1 FROM blocks WHERE number + 1 NOT IN (SELECT number FROM blocks);")
	defer rows.Close()

	log.Info("rows type", "type", reflect.TypeOf(rows))
	
	for rows.Next() {
		var number uint64
		rows.Scan(&number)
		idx += 1

		lastBlock = number
		
		if idx == 1 {
			firstBlock = number
		}

		nbr := hexutil.EncodeUint64(number)
		
		num := []string{}

		num = append(num, nbr)

		message := message{
			Id: 1,
			Method: "cardinal_streamsBlock",
			Params: num,
		}

		msg, err := json.Marshal(message)
		if err != nil {
			log.Error("cannot json marshal message, reindexer, block", number, "err", err.Error())
		}

		conn.WriteMessage(websocket.TextMessage, msg)

		_, resultBytes, err := conn.ReadMessage()
		if err != nil {
			log.Error("Error reading transport batch, reindexer, on block", number, "err", err.Error())
		}

		var or *outerResult

		if err := json.Unmarshal(resultBytes, &or); err != nil {
			log.Error("cannot unmarshal transportBytes, reindexer, on block", number, "err", err.Error())
		}

		tb := or.Result.Batch

		for _, indexer := range indexers {
			statements, err := indexer.Index(tb.ToPendingBatch())
			if err != nil {
				log.Error("Error generating statement reindexer, on indexer", indexer, "block", number, "err", err.Error())
			}
			for _, statement := range statements {
				if _, err := output.Write([]byte(statement + ";" + "\n")); err != nil {
					log.Error("Error writing to output file, reindexer", "err", err)
				}
			}
		}
	}

	log.Info(fmt.Sprintf("reindexing complete on blocks %v - %v", firstBlock, lastBlock))
		
	return nil

}

