package main

import (
	// "fmt"
	// "context"
	// "database/sql"
	// "os"
	// "strings"
	// "net/http"
	// "time"
	// "encoding/json"
	// "reflect"

	// "github.com/gorilla/websocket"
	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-streams/transports"
	"github.com/openrelayxyz/cardinal-types/hexutil"
	// "github.com/openrelayxyz/cardinal-flume/indexer"
	// "github.com/openrelayxyz/cardinal-flume/config"
	// "github.com/openrelayxyz/cardinal-flume/plugins"
)


func present() {
	log.Error("we out chere")
}

func absent() {
	log.Error("$$$$$$$$$$")
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

func indexGenesis() {

	// var wsURL string

	// for _, broker := range cfg.BrokerParams {
	// 	if strings.HasPrefix(broker.URL, "ws://") || strings.HasPrefix(broker.URL, "wss://") {
	// 		wsURL = broker.URL
	// 		log.Info("found websocket broker, reindexer", "broker", wsURL) 
	// 		break
	// 	}
	// }
	
	// dialer := &websocket.Dialer{
	// 	EnableCompression: true,
	// 	Proxy: http.ProxyFromEnvironment,
	// 	HandshakeTimeout: 45 * time.Second,
	// }
	
	// conn, _, err := dialer.Dial(wsURL, nil)
    // if err != nil {
	// 	log.Error("Websocket dial error, genesis indexer", "err", err.Error())
	// }

	params := []string{hexutil.EncodeUint64(uint64(0))}

	message := message{
		Id: 1,
		Method: "cardinal_streamsBlock",
		Params: params,
	}

	log.Error("this is the message", "msg", message)

}