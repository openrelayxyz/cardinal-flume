package plugins

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math/big"

	"github.com/openrelayxyz/cardinal-evm/common"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-types/hexutil"

	log "github.com/inconshreveable/log15"
	"github.com/klauspost/compress/zlib"
	"io"
	"io/ioutil"
	"sort"
)

func BytesToHash(data []byte) types.Hash {
	result := types.Hash{}
	copy(result[32-len(data):], data[:])
	return result
}

func BytesToAddress(data []byte) common.Address {
	result := common.Address{}
	copy(result[20-len(data):], data[:])
	return result
}

func bytesToAddressPtr(data []byte) *common.Address {
	if len(data) == 0 {
		return nil
	}
	result := BytesToAddress(data)
	return &result
}

func Decompress(data []byte) ([]byte, error) {
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

func TrimPrefix(data []byte) []byte {
	v := bytes.TrimLeft(data, string([]byte{0}))
	if len(v) == 0 {
		return []byte{0}
	}
	return v
}

func GetTopicIndex(topics []types.Hash, idx int) []byte {
	if len(topics) > idx {
		return TrimPrefix(topics[idx].Bytes())
	}
	return []byte{}
}

func UintToHexBig(a uint64) *hexutil.Big {
	x := hexutil.Big(*new(big.Int).SetUint64(a))
	return &x
}

var compressor *zlib.Writer
var compressionBuffer = bytes.NewBuffer(make([]byte, 0, 5*1024*1024))
// var extraSeal = 65

func Compress(data []byte) []byte {
	if len(data) == 0 {
		return data
	}
	compressionBuffer.Reset()
	if compressor == nil {
		compressor = zlib.NewWriter(compressionBuffer)
	} else {
		compressor.Reset(compressionBuffer)
	}
	compressor.Write(data)
	compressor.Close()
	return compressionBuffer.Bytes()
}

func GetLogs(db *sql.DB, blockNumber uint64, bkHash types.Hash, txIndex uint64) (SortLogs, error) {

	logRows, err := db.QueryContext(context.Background(), "SELECT DISTINCT transactionHash, address, topic0, topic1, topic2, topic3, data, logIndex from bor.bor_logs WHERE block = ?;", blockNumber)
	if err != nil {
		log.Info("sql response error", "err", err)
		return nil, err
	} 

		txLogs := SortLogs{}
		for logRows.Next() {
			var txHashBytes, address, topic0, topic1, topic2, topic3, data []byte
			var logIndex uint
			err := logRows.Scan(&txHashBytes, &address, &topic0, &topic1, &topic2, &topic3, &data, &logIndex)
			if err != nil {
				logRows.Close()
				return nil, err
			}
			txHash := BytesToHash(txHashBytes)
			topics := []types.Hash{}
			if len(topic0) > 0 {
				topics = append(topics, BytesToHash(topic0))
			}
			if len(topic1) > 0 {
				topics = append(topics, BytesToHash(topic1))
			}
			if len(topic2) > 0 {
				topics = append(topics, BytesToHash(topic2))
			}
			if len(topic3) > 0 {
				topics = append(topics, BytesToHash(topic3))
			}
			input, err := Decompress(data)
			if err != nil {
				return nil, err
			}
			txLogs = append(txLogs, &LogType{
				Address:     BytesToAddress(address),
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

func GetLogsBloom(db *sql.DB, blockNumber uint64) ([]byte, error) {
	var bloomBytes []byte

	if err := db.QueryRowContext(context.Background(), "SELECT logsBloom FROM bor.bor_receipts WHERE block = ?;", blockNumber).Scan(&bloomBytes);
	err != nil {
		log.Info("getLogs error", "err", err) // I suppose we should be checking to see what errors the polygon node actually returns?
		return nil, err
	}

	logsBloom, err := Decompress(bloomBytes)
	if err != nil {
		log.Error("Error decompressing logsBloom", "err", err.Error())
		return nil, err
	}

	return logsBloom, nil
} 

func GetTransactionReceiptsBlock(ctx context.Context, db *sql.DB, offset, limit int, chainid uint64, whereClause string, params ...interface{}) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT blocks.hash, transactions.block, transactions.gasUsed, transactions.cumulativeGasUsed, transactions.hash, transactions.recipient, transactions.transactionIndex, transactions.sender, transactions.contractAddress, transactions.logsBloom, transactions.status, transactions.type, transactions.gasPrice FROM transactions.transactions INNER JOIN blocks.blocks ON blocks.number = transactions.block WHERE %v ORDER BY transactions.rowid LIMIT ? OFFSET ?;", whereClause)
	logsQuery := fmt.Sprintf(`
		SELECT event_logs.transactionHash, event_logs.block, event_logs.address, event_logs.topic0, event_logs.topic1, event_logs.topic2, event_logs.topic3, event_logs.data, event_logs.logIndex
		FROM event_logs
		WHERE (transactionHash, block) IN (
			SELECT transactions.hash, block
			FROM transactions.transactions INNER JOIN blocks.blocks ON transactions.block = blocks.number
			WHERE %v
		);`, whereClause)
	return getTransactionReceiptsQuery(ctx, db, offset, limit, chainid, query, logsQuery, params...)
}


func getTransactionReceiptsQuery(ctx context.Context, db *sql.DB, offset, limit int, chainid uint64, query, logsQuery string, params ...interface{}) ([]map[string]interface{}, error) {
	logRows, err := db.QueryContext(ctx, logsQuery, params...)
	if err != nil {
		log.Error("Error selecting logs", "query", query, "err", err.Error())
		return nil, err
	}
	txLogs := make(map[types.Hash]SortLogs)
	for logRows.Next() {
		var txHashBytes, address, topic0, topic1, topic2, topic3, data []byte
		var logIndex uint
		var blockNumber uint64
		err := logRows.Scan(&txHashBytes, &blockNumber, &address, &topic0, &topic1, &topic2, &topic3, &data, &logIndex)
		if err != nil {
			logRows.Close()
			return nil, err
		}
		txHash := BytesToHash(txHashBytes)
		if _, ok := txLogs[txHash]; !ok {
			txLogs[txHash] = SortLogs{}
		}
		topics := []types.Hash{}
		if len(topic0) > 0 {
			topics = append(topics, BytesToHash(topic0))
		}
		if len(topic1) > 0 {
			topics = append(topics, BytesToHash(topic1))
		}
		if len(topic2) > 0 {
			topics = append(topics, BytesToHash(topic2))
		}
		if len(topic3) > 0 {
			topics = append(topics, BytesToHash(topic3))
		}
		input, err := Decompress(data)
		if err != nil {
			return nil, err
		}
		txLogs[txHash] = append(txLogs[txHash], &LogType{
			Address:     BytesToAddress(address),
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

	rows, err := db.QueryContext(ctx, query, append(params, limit, offset)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	results := []map[string]interface{}{}
	for rows.Next() {
		var to, from, blockHash, txHash, contractAddress, bloomBytes []byte
		var blockNumber, txIndex, gasUsed, cumulativeGasUsed, status, gasPrice uint64
		var txTypeRaw sql.NullInt32
		err := rows.Scan(
			&blockHash,
			&blockNumber,
			&gasUsed,
			&cumulativeGasUsed,
			&txHash,
			&to,
			&txIndex,
			&from,
			&contractAddress,
			&bloomBytes,
			&status,
			&txTypeRaw,
			&gasPrice,
		)
		if err != nil {
			return nil, err
		}
		txType := uint8(txTypeRaw.Int32)
		logsBloom, err := Decompress(bloomBytes)
		if err != nil {
			return nil, err
		}
		fields := map[string]interface{}{
			"blockHash":         BytesToHash(blockHash),
			"blockNumber":       hexutil.Uint64(blockNumber),
			"transactionHash":   BytesToHash(txHash),
			"transactionIndex":  hexutil.Uint64(txIndex),
			"from":              BytesToAddress(from),
			"to":                bytesToAddressPtr(to),
			"gasUsed":           hexutil.Uint64(gasUsed),
			"cumulativeGasUsed": hexutil.Uint64(cumulativeGasUsed),
			"effectiveGasPrice": hexutil.Uint64(gasPrice),
			"contractAddress":   nil,
			"logsBloom":         hexutil.Bytes(logsBloom),
			"status":            hexutil.Uint(status),
			"type":              hexutil.Uint(txType),
		}
		// If the ContractAddress is 20 0x0 bytes, assume it is not a contract creation
		if address := BytesToAddress(contractAddress); address != (common.Address{}) {
			fields["contractAddress"] = address
		}
		txh := BytesToHash(txHash)
		for i := range txLogs[txh] {
			txLogs[txh][i].TxIndex = hexutil.Uint(txIndex)
			txLogs[txh][i].BlockHash = BytesToHash(blockHash)
		}
		logs, ok := txLogs[txh]
		if !ok {
			logs = SortLogs{}
		}
		sort.Sort(logs)
		fields["logs"] = logs
		results = append(results, fields)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func DeriveBlockNumberOrHash(db *sql.DB, blockNrOrHash BlockNumberOrHash) (*uint64, *types.Hash, error) {
	var hashBytes []byte
	var blockNumber uint64
	var blockHash types.Hash
	
	bkNumber, numOk := blockNrOrHash.Number()
	bkHash, hashOk := blockNrOrHash.Hash()

	switch {

		case numOk:
			if err := db.QueryRowContext(context.Background(), "SELECT hash FROM blocks.blocks WHERE number = ?;", uint64(bkNumber)).Scan(&hashBytes);
			err != nil {
				log.Error("DeriveBlockNumberOrHash fetch hash error", "err", err)
				return nil, nil, err
			}
			blockHash = BytesToHash(hashBytes)
			blockNumber = uint64(int64(bkNumber))

		case hashOk:
			if err := db.QueryRowContext(context.Background(), "SELECT number FROM blocks.blocks WHERE hash = ?;", bkHash).Scan(&blockNumber);
			err != nil {
				log.Error("GetTestSnapshot fetch hash error", "err", err)
				return nil, nil, err
			}
			blockHash = bkHash

	}

	return &blockNumber, &blockHash, nil
}