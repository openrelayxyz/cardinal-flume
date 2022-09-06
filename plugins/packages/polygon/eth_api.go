package main

import (
	"context"
	"database/sql"

	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-types/hexutil"
	"github.com/openrelayxyz/flume/plugins"
	"github.com/openrelayxyz/flume/config"
)

type PolygonEthService struct {
	db *sql.DB
	cfg *config.Config
}

func (service *PolygonEthService) GetRootHash(ctx context.Context, starBlockNr uint64, endBlockNr uint64) (string, error) {
	return "goodbye horsETHs", nil
}


func GetBlockByNumber(blockVal map[string]interface{}, db *sql.DB) (map[string]interface{}, error) {
	var txHash  []byte
	var txIndex  uint64

	if err := db.QueryRowContext(context.Background(), "SELECT hash, transactionIndex FROM bor.bor_receipts WHERE block = ?;", uint64(blockVal["number"].(hexutil.Uint64))).Scan(&txHash, &txIndex);
	err != nil {
		log.Info("sql response", "err", err)
	}

	if txHash != nil {
		switch blockVal["transactions"].(type) {
		case []types.Hash:
			txns := blockVal["transactions"].([]types.Hash)
			blockVal["transactions"] = append(txns, plugins.BytesToHash(txHash))
			return blockVal, nil
			
		case []map[string]interface{}:
			txns := blockVal["transactions"].([]map[string]interface{})
			borTx := map[string]interface{}{
				"blockHash": blockVal["hash"],
				"blockNumber": blockVal["number"],
				"from": "0x0000000000000000000000000000000000000000",
				"gas": "0x0",
				"gasPrice": "0x0",
				"hash": plugins.BytesToHash(txHash),
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
			blockVal["transactions"] = append(txns, borTx)
			return blockVal, nil
		}
	}
	return nil, nil
}

func GetBlockByHash(blockVal map[string]interface{}, db *sql.DB) (map[string]interface{}, error) {
	var txHash  []byte
	var txIndex  uint64
	
	if err := db.QueryRowContext(context.Background(), "SELECT transactionHash, transactionIndex FROM bor.bor_logs WHERE blockHash = ?;", blockVal["hash"]).Scan(&txHash, &txIndex)
	err != nil {
		log.Info("sql response", "err", err)
	}
	
	if txHash != nil {

		switch blockVal["transactions"].(type) {
			
		case []types.Hash:
			txns := blockVal["transactions"].([]types.Hash)
			blockVal["transactions"] = append(txns, plugins.BytesToHash(txHash))
			return blockVal, nil
			
		case []map[string]interface{}:
			txns := blockVal["transactions"].([]map[string]interface{})
			borTx := map[string]interface{}{
				"blockHash": blockVal["hash"],
				"blockNumber": blockVal["number"],
				"from": "0x0000000000000000000000000000000000000000",
				"gas": "0x0",
				"gasPrice": "0x0",
				"hash": plugins.BytesToHash(txHash),
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
			blockVal["transactions"] = append(txns, borTx)
			return blockVal, nil

		}
	}
	
	return nil, nil
}

func GetTransactionByHash(txHash types.Hash, db *sql.DB) (map[string]interface{}, error) {  // method is very slow to respond 10+ secs
	var blockHash []byte
	var blockNumber, txIndex  uint64

	if err := db.QueryRowContext(context.Background(), "SELECT DISTINCT block, blockHash, transactionIndex FROM bor.bor_logs INDEXED BY logsTxHash WHERE transactionHash = ?;", txHash).Scan(&blockNumber, &blockHash, &txIndex);
	err != nil {
		log.Info("sql response", "err", err)
		return nil, nil
		} 

	if blockHash != nil {

		borTxObj :=  map[string]interface{}{
			"blockHash": plugins.BytesToHash(blockHash),
				"blockNumber": hexutil.Uint64(blockNumber),
				"from": "0x0000000000000000000000000000000000000000",
				"gas": "0x0",
				"gasPrice": "0x0",
				"hash": txHash,
				"input": "0x",
				"nonce": "0x0",
				"to": "0x0000000000000000000000000000000000000000",
				"transactionIndex": hexutil.Uint64(txIndex),
				"value": "0x0",
				"type": "0x0",
				"v": "0x0",
				"r": "0x0",
				"s": "0x0",
		}
		return borTxObj, nil
	}
	return nil, nil
}

func GetTransactionReceipt(db *sql.DB, txHash types.Hash) (map[string]interface{}, error) { 
	var blockHash []byte
	var blockNumber, txIndex uint64
	
	if err := db.QueryRowContext(context.Background(), "SELECT DISTINCT block, blockHash, transactionIndex FROM bor.bor_logs INDEXED BY logsTxHash WHERE transactionHash = ?;", txHash).Scan(&blockNumber, &blockHash, &txIndex);
	err != nil {
		log.Info("GTR sql response", "err", err)
		return nil, nil
	} 

	if blockHash != nil {
		
		bkHash := plugins.BytesToHash(blockHash)

		logsBloom, err := plugins.GetLogsBloom(db, blockNumber)
		if err != nil {
			log.Error("Error fetching logsBloom", "err", err.Error())
			return nil, err
		}
		
		txLogs, err := plugins.GetLogs(db, blockNumber, bkHash, txIndex) 
		if err != nil {
			log.Error("Error fetching logs", "err", err.Error())
			return nil, err
		}
		
		txObj := map[string]interface{}{
			"blockHash": plugins.BytesToHash(blockHash),
    		"blockNumber": hexutil.Uint64(blockNumber),
    		"contractAddress": nil,
    		"cumulativeGasUsed": "0x0",
    		"effectiveGasPrice": "0x0",
    		"from": "0x0000000000000000000000000000000000000000",
    		"gasUsed": "0x0",
    		"logs": txLogs,
			"logsBloom": hexutil.Bytes(logsBloom),
    		"status": "0x1",
    		"to": "0x0000000000000000000000000000000000000000",
    		"transactionHash": txHash,
    		"transactionIndex": hexutil.Uint64(txIndex),
    		"type": "0x0",
		}
		
		return txObj, nil
	}
	return nil, nil
}

func (service *PolygonEthService) ChainId(ctx context.Context) hexutil.Uint64 {
	return hexutil.Uint64(service.cfg.Chainid)
}

func (service *PolygonEthService) GetBorBlockReceipt(ctx context.Context, bkHash types.Hash) (map[string]interface{}, error) {
	var transactionHash []byte
	var blockNumber, txIndex uint64

	if err := service.db.QueryRowContext(context.Background(), "SELECT DISTINCT block, transactionHash, transactionIndex FROM bor.bor_logs WHERE blockHash = ?;", bkHash).Scan(&blockNumber, &transactionHash, &txIndex);
	err != nil {
		log.Info("sql response", "err", err)
		return nil, nil
	}

	if transactionHash != nil {

		txHash := plugins.BytesToHash(transactionHash)

		logsBloom, err := plugins.GetLogsBloom(service.db, blockNumber)
		if err != nil {
			log.Error("Error fetching logsBloom", "err", err.Error())
			return nil, err
		}

		txLogs, err := plugins.GetLogs(service.db, blockNumber, bkHash, txIndex) 
		if err != nil {
			log.Error("Error fetching logs", "err", err.Error())
			return nil, err
		}

		borBlockObj := map[string]interface{}{
			"root": "0x",
			"status": "0x1",
			"cumulativeGasUsed": "0x0",
			"logsBloom": hexutil.Bytes(logsBloom),
			"logs": txLogs,
			"transactionHash": txHash,
			"contractAddress": "0x0000000000000000000000000000000000000000",
			"gasUsed": "0x0",
			"blockHash": bkHash,
			"blockNumber": hexutil.Uint64(blockNumber),
			"transactionIndex": hexutil.Uint(txIndex),
			}
	
		return borBlockObj, nil
	}
	
	return nil, nil
}

func (service *PolygonEthService) GetTransactionReceiptsByBlock(ctx context.Context, blockNrOrHash plugins.BlockNumberOrHash) ([]map[string]interface{}, error) {
	errResponse := make(map[string]interface{})
	receipts := []map[string]interface{}{}

	number, numOk := blockNrOrHash.Number()
	hash, hshOk := blockNrOrHash.Hash()

	var column interface{}
	var whereClause string
	var borTxQuery string

	switch true {
		case numOk: 
			column = number
			whereClause = "block = ?"
			borTxQuery = "SELECT transactionHash FROM bor.bor_logs WHERE block = ?;"
		case hshOk:
			column = plugins.TrimPrefix(hash.Bytes())
			whereClause = "blocks.hash = ?"
			borTxQuery = "SELECT transactionHash FROM bor.bor_logs WHERE blockHash = ?;"
		default:
			errResponse["message"] = "invalid argument 0: json: cannot unmarshal number into Go value of type string"
			receipts = append(receipts, errResponse)
			return receipts, nil
	}

	var err error
	receipts, err = plugins.GetTransactionReceiptsBlock(context.Background(), service.db, 0, 100000, service.cfg.Chainid, whereClause, column)
	if err != nil {
		return nil, err
	}

	var borTxHashBytes  []byte
	
	if err := service.db.QueryRowContext(context.Background(), borTxQuery, column).Scan(&borTxHashBytes)
	err != nil {
		log.Error("bor tx query", "err", err)
		return nil, err
	}
	
	borReceipt, err := GetTransactionReceipt(service.db, plugins.BytesToHash(borTxHashBytes))
	
	
	receipts = append(receipts, borReceipt)

	// This is left here for exploring / debugging purposes
	// if len(receipts) > 0 {
	// 	for _, receipt := range receipts {
	// 		delete(receipt, "effectiveGasPrice")
	// 		delete(receipt, "type")
	// 		receipt["transactionHash"] = plugins.BytesToHash(borTxHashBytes)
	// 	}
	// }  

	return receipts, nil
}


