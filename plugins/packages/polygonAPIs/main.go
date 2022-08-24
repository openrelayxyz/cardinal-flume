package main

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"io/ioutil"
	// "reflect"
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
	cfg *config.Config
}

func Initialize(cfg *config.Config, pl *plugins.PluginLoader) {
	log.Info("Polygon API plugin loaded")
}

func RegisterAPI(tm *rpcTransports.TransportManager, db *sql.DB, cfg *config.Config) error {
	tm.Register("eth", &PolygonService{
			db: db,
			cfg: cfg,
	})
	log.Info("PolygonService registered")
	return nil	
}

func (service *PolygonService) ChainId(ctx context.Context) hexutil.Uint64 {
	return hexutil.Uint64(service.cfg.Chainid)
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

func getLogs(db *sql.DB, blockNumber uint64, bkHash types.Hash, txIndex uint64) ([]*logType, error) {

	logRows, err := db.QueryContext(context.Background(), "SELECT DISTINCT transactionHash, address, topic0, topic1, topic2, topic3, data, logIndex from bor.bor_logs WHERE block = ?;", blockNumber)
	if err != nil {
		log.Info("sql response error", "err", err)
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
				TxIndex:     hexutil.Uint(txIndex),
				BlockHash:   bkHash,
				TxHash:      txHash,
				Index:       hexutil.Uint(logIndex),
			})
		}
		logRows.Close()
		if err := logRows.Err(); err != nil {
			log.Warn("Rows close() error", "err", err.Error())
		}
	sort.Sort(txLogs)

	return txLogs, nil
}

func getLogsBloom(db *sql.DB, blockNumber uint64) ([]byte, error) {
	var bloomBytes []byte

	if err := db.QueryRowContext(context.Background(), "SELECT logsBloom FROM bor.bor_receipts WHERE block = ?;", blockNumber).Scan(&bloomBytes);
	err != nil {
		log.Info("getLogs error", "err", err) // I suppose we should be checking to see what errors the polygon node actually returns?
		return nil, err
	}

	logsBloom, err := decompress(bloomBytes)
	if err != nil {
		log.Error("Error decompressing logsBloom", "err", err.Error())
		return nil, err
	}

	return logsBloom, nil
} 


func GetTransactionReceipt(txHash types.Hash, db *sql.DB) (map[string]interface{}, error) {  // method is very slow to respond, 10+ secs
	var blockHash []byte
	var blockNumber, txIndex uint64

	if err := db.QueryRowContext(context.Background(), "SELECT DISTINCT block, blockHash, transactionIndex FROM bor.bor_logs INDEXED BY logsTxHash WHERE transactionHash = ?;", txHash).Scan(&blockNumber, &blockHash, &txIndex);
	err != nil {
		log.Info("sql response", "err", err)
		return nil, nil
	} 

	if blockHash != nil {
		
		bkHash := bytesToHash(blockHash)

		logsBloom, err := getLogsBloom(db, blockNumber)
		if err != nil {
			log.Error("Error fetching logsBloom", "err", err.Error())
			return nil, err
		}

		txLogs, err := getLogs(db, blockNumber, bkHash, txIndex) 
		if err != nil {
			log.Error("Error fetching logs", "err", err.Error())
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


func (service *PolygonService) GetBorBlockReceipt(ctx context.Context, bkHash types.Hash) (map[string]interface{}, error) {
	var transactionHash []byte
	var blockNumber, txIndex uint64

	if err := service.db.QueryRowContext(context.Background(), "SELECT DISTINCT block, transactionHash, transactionIndex FROM bor.bor_logs WHERE blockHash = ?;", bkHash).Scan(&blockNumber, &transactionHash, &txIndex);
	err != nil {
		log.Info("sql response", "err", err)
		return nil, nil
	}

	if transactionHash != nil {

		txHash := bytesToHash(transactionHash)

		logsBloom, err := getLogsBloom(service.db, blockNumber)
		if err != nil {
			log.Error("Error fetching logsBloom", "err", err.Error())
			return nil, err
		}

		txLogs, err := getLogs(service.db, blockNumber, bkHash, txIndex) 
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

func (service *PolygonService) GetTransactionReceiptsByBlock(ctx context.Context, bkNumber uint64) ([]map[string]interface{}, error) {
	results := []map[string]interface{}{}
	hashes := []types.Hash{}

	// bkNumber, _ := hexutil.DecodeUint64()
	
	txHashRows, err := service.db.QueryContext(context.Background(), "SELECT hash from transactions.transactions WHERE block = ?;", bkNumber)
	if err != nil {
		log.Error("sql response error getTxReceiptsByBlock", "err", err)
		return nil, err
	} 
	for txHashRows.Next() {
		var txHashBytes []byte
		err := txHashRows.Scan(&txHashBytes)
		if err != nil {
			txHashRows.Close()
			return nil, err
		}
		hashes = append(hashes, bytesToHash(txHashBytes))
	}
	txHashRows.Close()
	if err := txHashRows.Err(); err != nil {
		log.Error("Rows close() error", "err", err.Error())
	}
	
	var borTxHash  []byte
	
	if err := service.db.QueryRowContext(context.Background(), "SELECT transactionHash FROM bor.bor_logs WHERE block = ?;", bkNumber).Scan(&borTxHash)
	err != nil {
		log.Error("bor tx query", "err", err)
		return nil, err
	}

	hashes = append(hashes, bytesToHash(borTxHash))

	for i, hash := range hashes {
		txMap, err := GetTransactionReceipt(hash, service.db)
		if err != nil {
			log.Error("GTR error GTRBB", "on index", i, "err", err)
			return nil, err
		}
		results = append(results, txMap)
	}


	return results, nil
}


	




	// 	txObj := map[string]interface{}{
	// 		"blockHash": bytesToHash(blockHash),
    // 		"blockNumber": hexutil.Uint64(blockNumber),
    // 		"contractAddress": nil,
    // 		"cumulativeGasUsed": "0x0",
    // 		"from": "0x0000000000000000000000000000000000000000",
    // 		"gasUsed": "0x0",
    // 		"logs": txLogs,
	// 		"logsBloom": hexutil.Bytes(logsBloom),
    // 		"status": "0x1",
    // 		"to": "0x0000000000000000000000000000000000000000",
    // 		"transactionHash": txHash,
    // 		"transactionIndex": hexutil.Uint64(txIndex),
	// 	}

	// 	results = append(results, txObj)
	// }
// 	return results, nil
// }

// func (b *EthAPIBackend) GetRootHash(ctx context.Context, starBlockNr uint64, endBlockNr uint64) (string, error) {

	// https://github.com/maticnetwork/bor/blob/develop/eth/bor_api_backend.go#L17





