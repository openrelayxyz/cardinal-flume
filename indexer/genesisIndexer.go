package indexer

import (
	"fmt"
	"sync"
	"context"
	"database/sql"
	"strings"
	"net/http"
	"time"
	"encoding/json"

	"github.com/gorilla/websocket"
	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-streams/transports"
	"github.com/openrelayxyz/cardinal-types/hexutil"
	"github.com/openrelayxyz/cardinal-flume/config"
)

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

func IndexSingleBlock(cfg *config.Config, db *sql.DB, indexers []Indexer, mut *sync.RWMutex, block int64) error {

	// if cfg.LatestBlock > 0 {
	// 	log.Info("Indexing continuing from block", "number", cfg.LatestBlock)
	// 	return nil
	// }

	job := "single block"
	if block == 0 {
		job = "genesis"
	}


	var wsURL string

	for _, broker := range cfg.BrokerParams {
		if strings.HasPrefix(broker.URL, "ws://") || strings.HasPrefix(broker.URL, "wss://") {
			wsURL = broker.URL
			log.Info(fmt.Sprintf("found websocket broker, %v indexer", job), "broker", wsURL) 
			break
		}
	}
	
	dialer := &websocket.Dialer{
		EnableCompression: true,
		Proxy: http.ProxyFromEnvironment,
		HandshakeTimeout: 45 * time.Second,
	}
	
	conn, _, err := dialer.Dial(wsURL, nil)
    if err != nil {
		log.Error(fmt.Sprintf("Websocket dial error, %v indexer", job), "err", err)
		return err
	}

	blk := uint64(block)

	params := []string{hexutil.EncodeUint64(blk)}

	message := message{
		Id: 1,
		Method: "cardinal_streamsBlock",
		Params: params,
	}

	msg, err := json.Marshal(message)
	if err != nil {
		log.Error(fmt.Sprintf("cannot json marshal message, %v indexer", job), "block", block, "err", err)
	}

	if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
		log.Error(fmt.Sprintf("failed to send message, %v indexer", job), "err", err)
	}

	_, resultBytes, err := conn.ReadMessage()
	if err != nil {
		log.Error(fmt.Sprintf("Error reading transport batch, %v indexer", job), "err", err)
		return err
	}

	var or *outerResult

	if err := json.Unmarshal(resultBytes, &or); err != nil {
		log.Error(fmt.Sprintf("cannot unmarshal transportBytes, %v indexer", job), "err", err)
		return err
	}

	pb := or.Result.Batch

	genesisStatements := []string{}

	for _, indexer := range indexers {
		statements, err := indexer.Index(pb.ToPendingBatch())
		if err != nil {
			log.Error(fmt.Sprintf("Error generating statement %v indexer, on indexer", job), indexer, "err", err)
			return err
		}
		genesisStatements = append(genesisStatements, statements...)
	}

	mut.Lock()
	dbtx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		log.Error(fmt.Sprintf("Error creating database transaction %v indexer", job), "err", err)
		return err
	}
	if _, err := dbtx.Exec(strings.Join(genesisStatements, " ; ")); err != nil {
		log.Error(fmt.Sprintf("Failed to execute statement %v indexer", job), "err", err)
		return err
	}
	if err := dbtx.Commit(); err != nil {
		log.Error(fmt.Sprintf("Failed to commit block %v indexer", job), "err", err)
		return err
	}

	mut.Unlock()
	return nil

}