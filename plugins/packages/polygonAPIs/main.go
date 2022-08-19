package main

import (
	"context"
	"database/sql"
	"io"
	"io/ioutil"
	"bytes"
	"sort"

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
	db *sql.DB
}

func Initialize(cfg *config.Config, pl *plugins.PluginLoader) {
	log.Info("Polygon API plugin loaded")
}

func RegisterAPI(tm *rpcTransports.TransportManager, db *sql.DB) error {
	tm.Register("eth", &PolygonService{
			db: db,
	})
	log.Info("PolygonService registered")
	return nil	
}

func (service *PolygonService) GetTransactionReceiptsByBlock() string {
	// GetTransactionReceiptsByBlock(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) ([]map[string]interface{}, error)
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

func (ms sortLogs) Len() int {
	return len(ms)
}

func (ms sortLogs) Less(i, j int) bool {
	if ms[i].BlockNumber != ms[j].BlockNumber {
		return ms[i].BlockNumber < ms[j].BlockNumber
	}
	return ms[i].Index < ms[j].Index
}

func (ms sortLogs) Swap(i, j int) {
	ms[i], ms[j] = ms[j], ms[i]
}

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

		txLogs := sortLogs{}
		for logRows.Next() {
			var txHashBytes, address, topic0, topic1, topic2, topic3, data []byte
			var logIndex uint
			err := logRows.Scan(&txHashBytes, &address, &topic0, &topic1, &topic2, &topic3, &data, &logIndex)
			if err != nil {
				logRows.Close()
				return nil, err
			}
			txHash := bytesToHash(txHashBytes)
			// if _, ok := txLogs[txHash]; !ok {
			// 	txLogs[txHash] = sortLogs{}
			// }
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
			txLogs = append(txLogs, &logType{
				Address:     bytesToAddress(address),
				Topics:      topics,
				Data:        hexutil.Bytes(input),
				BlockNumber: hexutil.EncodeUint64(blockNumber),
				BlockHash: 	 bytesToHash(blockHash),
				TxIndex: hexutil.Uint(txIndex), 
				TxHash:      txHash,
				Index:       hexutil.Uint(logIndex),
			})
		}
		logRows.Close()
		if err := logRows.Err(); err != nil {
			log.Warn("Rows close() error", "err", err.Error())
		}

		sort.Sort(txLogs)

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

func (service *PolygonService) GetBorBlockReceipt(ctx context.Context, bkHash types.Hash) (map[string]interface{}, error) {
	var bloomBytes, txHash []byte
	var blockNumber, txIndex uint64

	if err := service.db.QueryRowContext(context.Background(), "SELECT DISTINCT block, transactionHash, transactionIndex FROM bor.bor_logs WHERE blockHash = ?;", bkHash).Scan(&blockNumber, &txHash, &txIndex);
	err != nil {
		log.Info("sql response", "err", err)
		return nil, nil
	}

	if err := service.db.QueryRowContext(context.Background(), "SELECT logsBloom FROM bor.bor_receipts WHERE block = ?;", blockNumber).Scan(&bloomBytes);
	err != nil {
		log.Info("sql response", "err", err)
		return nil, nil
	} 

	logRows, err := service.db.QueryContext(context.Background(), "SELECT DISTINCT transactionHash, address, topic0, topic1, topic2, topic3, data, logIndex from bor.bor_logs WHERE block = ?;", blockNumber)
	if err != nil {
		log.Info("sql response error", "err", err)
		return nil, err
	} 

		if bloomBytes != nil {

			logsBloom, err := decompress(bloomBytes)
		if err != nil {
			log.Error("Error decompressing logsBloom", "err", err.Error())
			return nil, err
		}

		txLogs := sortLogs{}
		for logRows.Next() {
			var txHashBytes, address, topic0, topic1, topic2, topic3, data []byte
			var logIndex uint
			err := logRows.Scan(&txHashBytes, &address, &topic0, &topic1, &topic2, &topic3, &data, &logIndex)
			if err != nil {
				logRows.Close()
				return nil, err
			}
			txHash := bytesToHash(txHashBytes)
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
			txLogs = append(txLogs, &logType{
				Address:     bytesToAddress(address),
				Topics:      topics,
				Data:        hexutil.Bytes(input),
				BlockNumber: hexutil.EncodeUint64(blockNumber),
				TxIndex: hexutil.Uint(txIndex),
				BlockHash: bkHash,
				TxHash:      txHash,
				Index:       hexutil.Uint(logIndex),
			})
		}
		logRows.Close()
		if err := logRows.Err(); err != nil {
			log.Warn("Rows close() error", "err", err.Error())
		}

		log.Info("object inspect", "bkHash", bkHash, "txIndex", txIndex)

		borBlockObj := map[string]interface{}{
			"root": "0x",
			"status": "0x1",
			"cumulativeGasUsed": "0x0",
			"logsBloom": hexutil.Bytes(logsBloom),
			"logs": txLogs,
			"transactionHash": bytesToHash(txHash),
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


// {
//     "root": "0x",
//     "status": "0x1",
//     "cumulativeGasUsed": "0x0",
//     "logsBloom": "0x00000000400000000000000080002000020000000000000000000000040000000400000000000000000820140010000000008000000000800000090800200000000000000000000910008008000800800000000000000000000100000000000000000000020000000000000000000800010000000020000100000010018000000001000000000000040024800000000000010000440004000110000000000000020004000000000000000000000000000200000400000000000400000000004000000102400002000000800000000000004801000000800001508008000120000010008000400010000000000000000080000001000000080010000000100403",
//     "logs": [


// {
// 	"address": "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",
// 	"topics": [
// 	  "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
// 	  "0x00000000000000000000000076b22b8c1079a44f1211d867d68b1eda76a635a7",
// 	  "0x0000000000000000000000006a11bb3014df9d228037f62510940633215ca1b5"
// 	],
// 	"data": "0x000000000000000000000000000000000000000000000000000000003b97dbbb",
// 	"blockNumber": "0x19d8340",
// 	"transactionHash": "0xdc98c1082ea71326c6b895b4839a3559518483747aea199c0288bf3f4421aade",
// 	"transactionIndex": "0xd1",
// 	"blockHash": "0x031871dde8d9b8fab7386ad0cf20629a01c8187bdf68deec30b0ae3013ba3bf1",
// 	"logIndex": "0x317",
// 	"removed": false
//   }

//   ],
//   "transactionHash": "0xdc98c1082ea71326c6b895b4839a3559518483747aea199c0288bf3f4421aade",
//   "contractAddress": "0x0000000000000000000000000000000000000000",
//   "gasUsed": "0x0",
//   "blockHash": "0x031871dde8d9b8fab7386ad0cf20629a01c8187bdf68deec30b0ae3013ba3bf1",
//   "blockNumber": "0x19d8340",
//   "transactionIndex": "0xd1"
// }






