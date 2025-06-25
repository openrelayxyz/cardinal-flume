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

func replaceStatements(number uint64, statements []string) []string {

	for i, stmnt := range statements {
		if strings.Contains(stmnt, "number >=") || strings.Contains(stmnt, "block >=") {
			dFrom := stmnt
			words := strings.Fields(dFrom)
			n := len(words)
			mod := words[:n-2]
			prefix := strings.Join(mod, " ")
			suffix := " " + "=" + " " + fmt.Sprintf("%d", number)
			replacement := prefix + suffix
			statements[i] = replacement
		}
	}

	return statements
}

func InsertSingle(cfg *config.Config, number uint64, db *sql.DB, indexers []Indexer, mut *sync.RWMutex) error {

	if number > uint64(cfg.LatestBlock) {
		log.Error("skip ahead indexing not allowed", "latest block", cfg.LatestBlock)
		return nil
	}

	var wsURL string

	for _, broker := range cfg.BrokerParams {
		if strings.HasPrefix(broker.URL, "ws://") || strings.HasPrefix(broker.URL, "wss://") {
			wsURL = broker.URL
			log.Info("found websocket broker, reindexer", "broker", wsURL) 
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
		return err
	}

	params := []string{hexutil.EncodeUint64(number)}

	message := message{
		Id: 1,
		Method: "cardinal_streamsBlock",
		Params: params,
	}

	msg, err := json.Marshal(message)
	if err != nil {
		return err
	}

	if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
		return err
	}

	_, resultBytes, err := conn.ReadMessage()
	if err != nil {
		return err
	}

	var or *outerResult

	if err := json.Unmarshal(resultBytes, &or); err != nil {
		return err
	}

	pb := or.Result.Batch

	insertStatements := []string{}

	for _, indexer := range indexers {
		statements, err := indexer.Index(pb.ToPendingBatch())
		if err != nil {
			return err
		}
		modStatements := replaceStatements(number, statements)
		
		insertStatements = append(insertStatements, modStatements...)
	}

	mut.Lock()
	dbtx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	if _, err := dbtx.Exec(strings.Join(insertStatements, " ; ")); err != nil {
		return err
	}
	if err := dbtx.Commit(); err != nil {
		return err
	}

	mut.Unlock()
	return nil

}