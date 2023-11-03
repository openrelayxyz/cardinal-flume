package indexer

import (
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

func genesisAppend(stmnts []string) []string {
	filteredStatements := make([]string, 0, len(stmnts))
	for _, statement := range stmnts {
		if !strings.HasPrefix(statement, "DELETE") {
			filteredStatements = append(filteredStatements, statement)
		}
	}
	return filteredStatements
}

func IndexGenesis(cfg *config.Config, db *sql.DB, indexers []Indexer, mut *sync.RWMutex, genAppend bool) error {

	if !genAppend && cfg.LatestBlock > 0 {
		log.Info("Indexing continuing from block", "number", cfg.LatestBlock)
		return nil
	}

	var wsURL string

	for _, broker := range cfg.BrokerParams {
		if strings.HasPrefix(broker.URL, "ws://") || strings.HasPrefix(broker.URL, "wss://") {
			wsURL = broker.URL
			log.Info("found websocket broker, genesis indexer", "broker", wsURL) 
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
		log.Error("Websocket dial error, genesis indexer", "err", err.Error())
		return err
	}

	genesis := uint64(0)

	params := []string{hexutil.EncodeUint64(genesis)}

	message := message{
		Id: 1,
		Method: "cardinal_streamsBlock",
		Params: params,
	}

	msg, err := json.Marshal(message)
	if err != nil {
		log.Error("cannot json marshal message, reindexer, block", genesis, "err", err.Error())
	}

	if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
		log.Error("failed to send message, genesis indexer", "err", err.Error())
	}

	_, resultBytes, err := conn.ReadMessage()
	if err != nil {
		log.Error("Error reading transport batch, reindexer, on block", genesis, "err", err.Error())
		return err
	}

	var or *outerResult

	if err := json.Unmarshal(resultBytes, &or); err != nil {
		log.Error("cannot unmarshal transportBytes, reindexer, on block", genesis, "err", err.Error())
		return err
	}

	pb := or.Result.Batch

	genesisStatements := []string{}

	for _, indexer := range indexers {
			statements, err := indexer.Index(pb.ToPendingBatch())
			if genAppend {
				statements = genesisAppend(statements)
			}
			if err != nil {
				log.Error("Error generating statement genesis indexer, on indexer", indexer, "err", err.Error())
				return err
			}
			genesisStatements = append(genesisStatements, statements...)
	}

	mut.Lock()
	dbtx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		log.Error("Error creating database transaction genesis indexer", "err", err.Error())
		return err
	}
	if _, err := dbtx.Exec(strings.Join(genesisStatements, " ; ")); err != nil {
		log.Error("Failed to execute statement genesis indexer", "err", err.Error())
		return err
	}
	if err := dbtx.Commit(); err != nil {
		log.Error("Failed to commit genesis block genesis indexer", "err", err.Error())
		return err
	}

	mut.Unlock()
	return nil

}