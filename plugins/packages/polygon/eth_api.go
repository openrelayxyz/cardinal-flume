package main

import (
	"context"
	"database/sql"
	"errors"

	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-types/metrics"
	"github.com/openrelayxyz/cardinal-types/hexutil"
	"github.com/openrelayxyz/cardinal-flume/plugins"
	"github.com/openrelayxyz/cardinal-flume/config"
	"github.com/openrelayxyz/cardinal-flume/heavy"
	"github.com/openrelayxyz/cardinal-rpc"
)

type PolygonEthService struct {
	db *sql.DB
	cfg *config.Config
}


func GetBlockByNumber(blockVal map[string]interface{}, db *sql.DB) (map[string]interface{}, error) {
	var txHash  []byte
	var txIndex  uint64

	db.QueryRowContext(context.Background(), "SELECT hash, transactionIndex FROM bor.bor_receipts WHERE block = ?;", uint64(blockVal["number"].(hexutil.Uint64))).Scan(&txHash, &txIndex)

	if txHash != nil {
		switch blockVal["transactions"].(type) {
		case []types.Hash:
			txns := blockVal["transactions"].([]types.Hash)
			blockVal["transactions"] = append(txns, plugins.BytesToHash(txHash))
			blockVal["miner"] = "0x0000000000000000000000000000000000000000"
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
			blockVal["miner"] = "0x0000000000000000000000000000000000000000"
			return blockVal, nil
		}
	}

	blockVal["miner"] = "0x0000000000000000000000000000000000000000"

	return blockVal, nil
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
			blockVal["miner"] = "0x0000000000000000000000000000000000000000"
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
			blockVal["miner"] = "0x0000000000000000000000000000000000000000"
			return blockVal, nil

		}
	}

	blockVal["miner"] = "0x0000000000000000000000000000000000000000"

	return blockVal, nil
}


func GetTransactionByHash(txObj map[string]interface{}, txHash types.Hash, db *sql.DB) (map[string]interface{}, error) {

	if txObj == nil {

		var blockHash []byte
		var blockNumber, txIndex  uint64

		if err := db.QueryRowContext(context.Background(), "SELECT DISTINCT block, blockHash, transactionIndex FROM bor.bor_logs INDEXED BY logsTxHash WHERE transactionHash = ?;", txHash).Scan(&blockNumber, &blockHash, &txIndex);
		err != nil {
			log.Info("sql response", "err", err)
			return nil, nil
		}

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

	return txObj, nil
}

func GetTransactionReceipt(receiptObj map[string]interface{}, txHash types.Hash, db *sql.DB) (map[string]interface{}, error) {

	if receiptObj == nil {

		var blockHash []byte
		var blockNumber, txIndex uint64

		if err := db.QueryRowContext(context.Background(), "SELECT DISTINCT block, blockHash, transactionIndex FROM bor.bor_logs INDEXED BY logsTxHash WHERE transactionHash = ?;", txHash).Scan(&blockNumber, &blockHash, &txIndex);
		err != nil {
			log.Info("GTR sql response", "err", err)
			return nil, nil
		}


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

		borReceiptObj := map[string]interface{}{
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

		receiptObj =  borReceiptObj
	}

	return receiptObj, nil
}

var (
	gborbrHitMeter  = metrics.NewMinorMeter("/flume/polygon/gborbr/hit")
	gborbrMissMeter = metrics.NewMinorMeter("/flume/polygon/gborbr/miss")
)

func (service *PolygonEthService) GetBorBlockReceipt(ctx context.Context, bkHash types.Hash) (*map[string]interface{}, error) {
	
	
	var transactionHash []byte
	var blockNumber, txIndex uint64

	if err := service.db.QueryRowContext(context.Background(), "SELECT DISTINCT block, transactionHash, transactionIndex FROM bor.bor_logs WHERE blockHash = ?;", bkHash).Scan(&blockNumber, &transactionHash, &txIndex);
	err != nil {
		err := rpc.NewRPCError(-32000, "not found")
		log.Error("sql response", "err", err)
		return nil, err
	}
	
	if transactionHash != nil {
		
		if len(service.cfg.HeavyServer) > 0 && !borBlockDataPresent(bkHash, service.cfg, service.db) {
			log.Debug("eth_getBorBlockReceipt sent to flume heavy")
			polygonMissMeter.Mark(1)
			gborbrMissMeter.Mark(1)
			responseShell, err := heavy.CallHeavy[map[string]interface{}](ctx, service.cfg.HeavyServer, "eth_getBorBlockReceipt", bkHash)
			if err != nil {
				return nil, err
			}
			return responseShell, nil
		}	
		
		if len(service.cfg.HeavyServer) > 0 {
			log.Debug("eth_getBorBlockReceipt served from flume light")
			polygonHitMeter.Mark(1)
			gborbrHitMeter.Mark(1)
		}

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

		return &borBlockObj, nil
	}

	err := rpc.NewRPCError(-32000, "not found")
	return nil, err
}

var (
	gtrbbHitMeter  = metrics.NewMinorMeter("/flume/polygon/gtrbb/hit")
	gtrbbMissMeter = metrics.NewMinorMeter("/flume/polygon/gtrbb/miss")
)

func (service *PolygonEthService) GetTransactionReceiptsByBlock(ctx context.Context, blockNrOrHash plugins.BlockNumberOrHash) ([]map[string]interface{}, error) {
	
	errResponse := make(map[string]interface{})
	receipts := []map[string]interface{}{}

	number, numOk := blockNrOrHash.Number()
	hash, hshOk := blockNrOrHash.Hash()

	var column interface{}
	var whereClause string
	var borTxQuery string
	var borTxHashBytes []byte

	switch {
		case numOk:

			if len(service.cfg.HeavyServer) > 0 && !borBlockDataPresent(number, service.cfg, service.db) {
				log.Debug("eth_getTransactionReceiptByBlock sent to flume heavy")
				polygonMissMeter.Mark(1)
				gtrbbMissMeter.Mark(1)
				responseShell, err := heavy.CallHeavy[[]map[string]interface{}](ctx, service.cfg.HeavyServer, "eth_getBorBlockReceipt", number)
				if err != nil {
					return nil, err
				}
				return *responseShell, nil
			}

			if len(service.cfg.HeavyServer) > 0 {
				log.Debug("eth_getTransactionReceiptByBlock served from flume light")
				polygonHitMeter.Mark(1)
				gtrbbHitMeter.Mark(1)
			}

			column = number
			whereClause = "block = ?"
			borTxQuery = "SELECT transactionHash FROM bor.bor_logs WHERE block = ?;"
			service.db.QueryRowContext(context.Background(), borTxQuery, column).Scan(&borTxHashBytes)
		case hshOk:

			if len(service.cfg.HeavyServer) > 0 && !borBlockDataPresent(hash, service.cfg, service.db) {
				log.Debug("eth_getTransactionReceiptByBlock sent to flume heavy")
				polygonMissMeter.Mark(1)
				gtrbbMissMeter.Mark(1)
				responseShell, err := heavy.CallHeavy[[]map[string]interface{}](ctx, service.cfg.HeavyServer, "eth_getBorBlockReceipt", hash)
				if err != nil {
					return nil, err
				}
				return *responseShell, nil
			}

			if len(service.cfg.HeavyServer) > 0 {
				log.Debug("eth_getBorBlockReceipt served from flume light")
				polygonHitMeter.Mark(1)
				gtrbbHitMeter.Mark(1)
			}

			column = plugins.TrimPrefix(hash.Bytes())
			whereClause = "hash = ?"
			borTxQuery = "SELECT transactionHash FROM bor.bor_logs WHERE blockHash = ?;"
			service.db.QueryRowContext(context.Background(), borTxQuery, column).Scan(&borTxHashBytes)
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

	if borTxHashBytes != nil {
		var nilMap map[string]interface{}
		borReceipt, _ := GetTransactionReceipt(nilMap, plugins.BytesToHash(borTxHashBytes), service.db)
		receipts = append(receipts, borReceipt)
		for _, receipt := range receipts {
			receipt["transactionHash"] = plugins.BytesToHash(borTxHashBytes)
		}
	}

	for _, receipt := range receipts {
		if receipt["type"] == hexutil.Uint(2) || receipt["type"] == hexutil.Uint(1) {
			receipt["from"] = "0x0000000000000000000000000000000000000000"
		}
		delete(receipt, "effectiveGasPrice")
		delete(receipt, "type")
	}

	return receipts, nil
}