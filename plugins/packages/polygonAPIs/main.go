package main

import (
	"database/sql"

	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-types/hexutil"
	"github.com/openrelayxyz/flume/config"
	"github.com/openrelayxyz/flume/plugins"
	rpcTransports "github.com/openrelayxyz/cardinal-rpc/transports"
)

type PolygonService struct {
}

func Initialize(cfg *config.Config, pl *plugins.PluginLoader) {
	log.Info("Polygon API plugin loaded")
}

func RegisterAPI(tm *rpcTransports.TransportManager) error {
	tm.Register("eth", &PolygonService{})
	log.Info("PolygonService registered")
	return nil	
}

func (service *PolygonService) PolygonTest() string {
	return "Goodbye Horses"
}

func bytesToHash(data []byte) types.Hash {
	result := types.Hash{}
	copy(result[32-len(data):], data[:])
	return result
}

func GetBlockByNumber(blockVal map[string]interface{}, db *sql.DB) (map[string]interface{}, error) {
	var txHash  []byte
	var txIndex  uint64

	row, err := db.Query("SELECT hash, transactionIndex FROM bor.bor_receipts WHERE block = '%v';", blockVal["number"])
	if err != nil {return nil, err}
	
	if err := row.Scan(&txHash, &txIndex); err != nil {return nil, err}
	

	if txHash != nil {

		switch blockVal["transactions"].(type) {

		case []types.Hash:
			txns := blockVal["transactions"].([]types.Hash)
			txns = append(txns, bytesToHash(txHash))
			return blockVal, nil

		case []map[string]interface{}:
			txns := blockVal["transactions"].([]map[string]interface{})
			borTx := map[string]interface{}{
				"blockHash": blockVal["hash"],
				"blockNumber": blockVal["number"],
				"from": "0x0000000000000000000000000000000000000000",
				"gas": "0x0",
				"gasPrice": "0x0",
				"hash": bytesToHash(txHash),
				"input": "0x",
				"nonce": "0x0",
				"r": "0x0",
				"s": "0x0",
				"to": "0x0000000000000000000000000000000000000000",
				"transactionIndex": hexutil.Uint64(txIndex),
				"type": "0x0",
				"v": "0x0",
				"value": "0x0",
			}
			txns = append(txns, borTx)
			return blockVal, nil

		}
	}

	return nil, nil
}

func GetBlockByHash(blockVal map[string]interface{}, db *sql.DB) (map[string]interface{}, error) {
	var txHash  []byte
	var txIndex  uint64

	row, err := db.Query("SELECT transactionHash, transactionIndex FROM bor.bor_receipts WHERE blockHash = '%v';", blockVal["hash"])
	if err != nil {return nil, err}
	
	if err := row.Scan(&txHash, &txIndex); err != nil {return nil, err}
	

	if txHash != nil {

		switch blockVal["transactions"].(type) {

		case []types.Hash:
			txns := blockVal["transactions"].([]types.Hash)
			txns = append(txns, bytesToHash(txHash))
			return blockVal, nil

		case []map[string]interface{}:
			txns := blockVal["transactions"].([]map[string]interface{})
			borTx := map[string]interface{}{
				"blockHash": blockVal["hash"],
				"blockNumber": blockVal["number"],
				"from": "0x0000000000000000000000000000000000000000",
				"gas": "0x0",
				"gasPrice": "0x0",
				"hash": bytesToHash(txHash),
				"input": "0x",
				"nonce": "0x0",
				"r": "0x0",
				"s": "0x0",
				"to": "0x0000000000000000000000000000000000000000",
				"transactionIndex": hexutil.Uint64(txIndex),
				"type": "0x0",
				"v": "0x0",
				"value": "0x0",
			}
			txns = append(txns, borTx)
			return blockVal, nil

		}
	}

	return nil, nil
}

