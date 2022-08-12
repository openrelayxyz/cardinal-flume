package main

import (
	"context"
	"database/sql"
	"io"
	"io/ioutil"
	"bytes"

	log "github.com/inconshreveable/log15"
	"github.com/klauspost/compress/zlib"
	"github.com/openrelayxyz/cardinal-evm/common"
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

func (service *PolygonService) GetTransactionReceiptsByBlock() string {
	// GetTransactionReceiptsByBlock(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) ([]map[string]interface{}, error)
	return "Under construction"
}

func (service *PolygonService) GetBlockBorReceipt() string {
	// GetBorBlockReceipt(ctx context.Context, hash common.Hash) (*types.Receipt, error)
	return "Under construction"
}

func bytesToHash(data []byte) types.Hash {
	result := types.Hash{}
	copy(result[32-len(data):], data[:])
	return result
}

func decompress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}
	r, err := zlib.NewReader(bytes.NewBuffer(data))
	if err != nil {
		return []byte{}, err
	}
	raw, err := ioutil.ReadAll(r)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return raw, nil
	}
	return raw, err
}

func bytesToAddress(data []byte) common.Address {
	result := common.Address{}
	copy(result[20-len(data):], data[:])
	return result
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
			blockVal["transactions"] = append(txns, bytesToHash(txHash))
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
			blockVal["transactions"] = append(txns, bytesToHash(txHash))
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
			blockVal["transactions"] = append(txns, borTx)
			return blockVal, nil

		}
	}

	return nil, nil
}

func GetTransactionByHash(txHash types.Hash, db *sql.DB) (map[string]interface{}, error) {
	var blockHash []byte
	var blockNumber, txIndex  uint64

	if err := db.QueryRowContext(context.Background(), "SELECT DISTINCT block, blockHash, transactionIndex FROM bor.bor_logs WHERE block = ?;", txHash).Scan(&blockNumber, &blockHash, &txIndex);
	err != nil {
		log.Info("sql response", "err", err)
		return nil, nil
	} 

	//above is the original approach, its is pretty slow, the one below is slow as well. 

	// if err := db.QueryRowContext(context.Background(), "SELECT block FROM bor.bor_receipts WHERE hash = ?;", txHash).Scan(&blockNumber);
	// err != nil {
	// 	log.Info("sql response", "err", err)
	// }
	// if err := db.QueryRowContext(context.Background(), "SELECT DISTINCT blockHash, transactionIndex FROM bor.bor_logs WHERE block = ?;", blockNumber).Scan(&blockHash, &txIndex);
	// err != nil {
	// 	log.Info("sql response", "err", err)
	// 	return nil, nil
	// } 

	if blockHash != nil {

		borTxObj :=  map[string]interface{}{
				"blockHash": bytesToHash(blockHash),
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

type logType struct {
	Address common.Address `json:"address" gencodec:"required"`
	Topics []types.Hash `json:"topics" gencodec:"required"`
	Data hexutil.Bytes `json:"data" gencodec: "required"`
	BlockNumber string `json:"blockNumber"`
	TxHash types.Hash `json:"transactionHash" gencodec:"required"`
	TxIndex hexutil.Uint `json:"transactionIndex"`
	BlockHash types.Hash `json:"blockHash"`
	Index hexutil.Uint `json:"logIndex"`
	Removed bool `json:"removed"`
}

type sortLogs []*logType

func GetTransactionReceipt(txHash types.Hash, db *sql.DB) (map[string]interface{}, error) {
	var blockHash, bloomBytes []byte
	var blockNumber, txIndex uint64

	if err := db.QueryRowContext(context.Background(), "SELECT DISTINCT block, blockHash, transactionIndex FROM bor.bor_logs WHERE transactionHash = ?;", txHash).Scan(&blockNumber, &blockHash, &txIndex);
	err != nil {
		log.Info("sql response", "err", err)
		return nil, nil
	} 

	if err := db.QueryRowContext(context.Background(), "SELECT logsBloom FROM bor.bor_receipts WHERE block = ?;", blockNumber).Scan(&bloomBytes);
	err != nil {
		log.Info("sql response", "err", err)
		return nil, nil
	} 

	logRows, err := db.QueryContext(context.Background(), "SELECT DISTINCT transactionHash, address, topic0, topic1, topic2, topic3, data, logIndex from bor.bor_logs WHERE transactionHash = ?;", txHash)
	if err != nil {
		log.Info("sql response error", "err", err)
		return nil, err
	} 

	if blockHash != nil { 

		logsBloom, err := decompress(bloomBytes)
		if err != nil {
			log.Error("Error decompressing logsBloom", "err", err.Error())
			return nil, err
		}

		txLogs := make(map[types.Hash]sortLogs)
		for logRows.Next() {
			var txHashBytes, address, topic0, topic1, topic2, topic3, data []byte
			var logIndex uint
			err := logRows.Scan(&txHashBytes, &address, &topic0, &topic1, &topic2, &topic3, &data, &logIndex)
			if err != nil {
				logRows.Close()
				return nil, err
			}
			txHash := bytesToHash(txHashBytes)
			if _, ok := txLogs[txHash]; !ok {
				txLogs[txHash] = sortLogs{}
			}
			topics := []types.Hash{}
			if len(topic0) > 0 {
				topics = append(topics, bytesToHash(topic0))
			}
			if len(topic1) > 0 {
				topics = append(topics, bytesToHash(topic1))
			}
			if len(topic2) > 0 {
				topics = append(topics, bytesToHash(topic2))
			}
			if len(topic3) > 0 {
				topics = append(topics, bytesToHash(topic3))
			}
			input, err := decompress(data)
			if err != nil {
				return nil, err
			}
			txLogs[txHash] = append(txLogs[txHash], &logType{
				Address:     bytesToAddress(address),
				Topics:      topics,
				Data:        hexutil.Bytes(input),
				BlockNumber: hexutil.EncodeUint64(blockNumber),
				TxHash:      txHash,
				Index:       hexutil.Uint(logIndex),
			})
		}
		logRows.Close()
		if err := logRows.Err(); err != nil {
			return nil, err
			}

		txObj := map[string]interface{}{
			"blockHash": bytesToHash(blockHash),
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






