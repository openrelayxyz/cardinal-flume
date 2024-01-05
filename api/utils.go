package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/openrelayxyz/cardinal-evm/common"
	"github.com/openrelayxyz/cardinal-evm/rlp"
	evm "github.com/openrelayxyz/cardinal-evm/types"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-rpc"
	"github.com/openrelayxyz/cardinal-types/hexutil"
	"github.com/openrelayxyz/cardinal-flume/config"

	log "github.com/inconshreveable/log15"
	"github.com/klauspost/compress/zlib"
	"io"
	"io/ioutil"
	"math/big"
	"os"
	"sort"
)

var (
	zeroInputError = errors.New("Input must contain non zero characters")
)

func blockDataPresent(input interface{}, cfg *config.Config, db *sql.DB) bool {
	present := true
	switch input.(type) {
	case rpc.BlockNumber:
		if uint64(input.(rpc.BlockNumber)) < cfg.EarliestBlock {
			present = false
			return present
		}
	case types.Hash:
		blockHash := input.(types.Hash)
		var response int
		statement := "SELECT 1 FROM blocks.blocks WHERE hash = ?;"
		db.QueryRow(statement, trimPrefix(blockHash.Bytes())).Scan(&response)
		if response == 0 {
			present = false
			return present
		}
	}
	return present
}

func txDataPresent(txHash types.Hash, cfg *config.Config, db *sql.DB, mempool bool) bool {
	var present bool
	var response int
	txStatement := "SELECT 1 FROM transactions.transactions WHERE hash = ?;"
	db.QueryRow(txStatement, trimPrefix(txHash.Bytes())).Scan(&response)
	if response != 0 {
		present = true
		return present
	}
	if mempool {
		mpStatement := "SELECT 1 FROM mempool.transactions WHERE hash = ?;"
		db.QueryRow(mpStatement, trimPrefix(txHash.Bytes())).Scan(&response)
		if response != 0 {
			present = true
			return present
		}
	}
	return present
}

func getLatestBlock(ctx context.Context, db *sql.DB) (int64, error) {
	var result int64
	var hash []byte
	err := db.QueryRowContext(ctx, "SELECT max(number), hash FROM blocks.blocks;").Scan(&result, &hash)
	return result, err
}

func testingJson(fileString string) ([]byte, error) {
	jsonFile, err := os.Open(fileString)
	defer jsonFile.Close()
	if err != nil {
		return nil, err
	}
	byteValue, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		return nil, err
	}
	return byteValue, nil
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

func trimPrefix(data []byte) []byte {
	v := bytes.TrimLeft(data, string([]byte{0}))
	if len(v) == 0 {
		return []byte{0}
	}
	return v
}

func bytesToAddress(data []byte) common.Address {
	result := common.Address{}
	copy(result[20-len(data):], data[:])
	return result
}
func bytesToAddressPtr(data []byte) *common.Address {
	if len(data) == 0 {
		return nil
	}
	result := bytesToAddress(data)
	return &result
}
func bytesToHash(data []byte) types.Hash {
	result := types.Hash{}
	copy(result[32-len(data):], data[:])
	return result
}

func uintToHexBig(a uint64) *hexutil.Big {
	x := hexutil.Big(*new(big.Int).SetUint64(a))
	return &x
}

func bytesToHexBig(a []byte) *hexutil.Big {
	x := hexutil.Big(*new(big.Int).SetBytes(a))
	return &x
}

func incrementLastByte(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	prefixCopy := make([]byte, len(prefix))
	copy(prefixCopy, prefix)

	lastByteIndex := len(prefixCopy) - 1
	
	if prefixCopy[lastByteIndex] == 0xFF {
		return nil
	}
	prefixCopy[lastByteIndex]++

	return prefixCopy
}

func countLeadingZeros(byteSlice []byte) (int, error) {

	leadingZeros := 0
	for ; leadingZeros < len(byteSlice); leadingZeros++ {
		if byteSlice[leadingZeros] != 0 {
			return leadingZeros, nil
		}
	}
	return 0, zeroInputError
}

func getTransactionsQuery(ctx context.Context, db *sql.DB, offset, limit int, chainid uint64, query string, params ...interface{}) ([]map[string]interface{}, error) {
	rows, err := db.QueryContext(ctx, query, append(params, limit, offset)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	results := []map[string]interface{}{}
	for rows.Next() {
		var amount, to, from, data, blockHashBytes, txHash, r, s, cAccessListRLP, baseFeeBytes, gasFeeCapBytes, gasTipCapBytes, bVHashesRLP []byte
		var nonce, gasLimit, blockNumber, gasPrice, txIndex, v uint64
		var intermediateBFC interface{}
		var txTypeRaw sql.NullInt32
		err := rows.Scan(
			&blockHashBytes,
			&blockNumber,
			&gasLimit,
			&gasPrice,
			&txHash,
			&data,
			&nonce,
			&to,
			&txIndex,
			&amount,
			&v,
			&r,
			&s,
			&from,
			&txTypeRaw,
			&cAccessListRLP,
			&baseFeeBytes,
			&gasFeeCapBytes,
			&gasTipCapBytes,
			&intermediateBFC,
			&bVHashesRLP,
		)
		if err != nil {
			return nil, err
		}
		txType := uint8(txTypeRaw.Int32)
		blockHash := bytesToHash(blockHashBytes)
		txIndexHex := hexutil.Uint64(txIndex)
		inputBytes, err := decompress(data)
		if err != nil {
			return nil, err
		}
		accessListRLP, err := decompress(cAccessListRLP)
		if err != nil {
			return nil, err
		}
		var accessList *evm.AccessList

		item := map[string]interface{}{
			"blockHash":            &blockHash,
			"blockNumber":          uintToHexBig(blockNumber),
			"from":                 bytesToAddress(from),
			"gas":                  hexutil.Uint64(gasLimit),
			"gasPrice":             uintToHexBig(gasPrice),
			"hash":                 bytesToHash(txHash),
			"input":                hexutil.Bytes(inputBytes),
			"nonce":                hexutil.Uint64(nonce),
			"to":                   bytesToAddressPtr(to),
			"transactionIndex":     &txIndexHex,
			"value":                bytesToHexBig(amount),
			"v":                    uintToHexBig(v),
			"r":                    bytesToHexBig(r),
			"s":                    bytesToHexBig(s),
			"type":                 hexutil.Uint64(txType),
		}

		switch txType {
		case evm.AccessListTxType:
			accessList = &evm.AccessList{}
			rlp.DecodeBytes(accessListRLP, accessList)
			item["accessList"] = accessList
			item["chainId"] = uintToHexBig(chainid)
		case evm.DynamicFeeTxType:
			accessList = &evm.AccessList{}
			rlp.DecodeBytes(accessListRLP, accessList)
			item["accessList"] = accessList
			item["chainId"] = uintToHexBig(chainid)
			item["maxPriorityFeePerGas"] = bytesToHexBig(gasTipCapBytes)
			item["maxFeePerGas"] = bytesToHexBig(gasFeeCapBytes)
		case evm.BlobTxType:
			accessList = &evm.AccessList{}
			rlp.DecodeBytes(accessListRLP, accessList)
			item["accessList"] = accessList
			item["chainId"] = uintToHexBig(chainid)
			item["maxPriorityFeePerGas"] = bytesToHexBig(gasTipCapBytes)
			item["maxFeePerGas"] = bytesToHexBig(gasFeeCapBytes)
			if intermediateBFC != nil {
				blobFeeCap, ok := intermediateBFC.(uint64)
				if !ok {
					log.Error("Failed to convert blobFeeCap to uint64, getTransactionsQuery")
				}
				item["maxFeePerBlobGas"] = blobFeeCap
			}
			if len(bVHashesRLP) > 0 {
				bVHashes := types.Hash{}
				rlp.DecodeBytes(bVHashesRLP, bVHashes)
				item["blobVersionedHashes"] = bVHashes
			}
		}

		results = append(results, item)

	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

func getTransactionsBlock(ctx context.Context, db *sql.DB, offset, limit int, chainid uint64, whereClause string, params ...interface{}) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT blocks.hash, transactions.block, transactions.gas, transactions.gasPrice, transactions.hash, transactions.input, transactions.nonce, transactions.recipient, transactions.transactionIndex, transactions.value, transactions.v, transactions.r, transactions.s, transactions.sender, transactions.type, transactions.access_list, blocks.baseFee, transactions.gasFeeCap, transactions.gasTipCap, transactions.maxFeePerBlobGas, transactions.blobVersionedHashes FROM transactions.transactions INNER JOIN blocks.blocks ON blocks.number = transactions.block WHERE %v ORDER BY transactions.transactionIndex LIMIT ? OFFSET ?;", whereClause)
	return getTransactionsQuery(ctx, db, offset, limit, chainid, query, params...)
}

var emptyStateTrieHash types.Hash = types.HexToHash("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")


func getBlocks(ctx context.Context, db *sql.DB, includeTxs bool, chainid uint64, whereClause string, params ...interface{}) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT hash, parentHash, uncleHash, coinbase, root, txRoot, receiptRoot, bloom, difficulty, extra, mixDigest, uncles, td, number, gasLimit, gasUsed, time, nonce, size, baseFee, withdrawalHash, blobGasUsed, excessBlobGas, parentBeaconRoot FROM blocks.blocks WHERE %v;", whereClause)
	rows, err := db.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	results := []map[string]interface{}{}
	for rows.Next() {
		var hash, parentHash, uncleHash, coinbase, root, txRoot, receiptRoot, bloomBytes, extra, mixDigest, uncles, td, baseFee, withdrawalHashBytes, parentBeaconBlockRootBytes []byte
		var number, gasLimit, gasUsed, time, size, difficulty uint64
		var nonce int64
		var intermediateBGU, intermediateEBG interface{}
		err := rows.Scan(&hash, &parentHash, &uncleHash, &coinbase, &root, &txRoot, &receiptRoot, &bloomBytes, &difficulty, &extra, &mixDigest, &uncles, &td, &number, &gasLimit, &gasUsed, &time, &nonce, &size, &baseFee, &withdrawalHashBytes, &intermediateBGU, &intermediateEBG, &parentBeaconBlockRootBytes)
		if err != nil {
			return nil, err
		}
		logsBloom, err := decompress(bloomBytes)
		if err != nil {
			log.Error("Error decompressing data", "err", err.Error())
			return nil, err
		}

		var withdrawals []map[string]interface{}
		switch {
		case len(withdrawalHashBytes) == 0:
			// This empty case is used to account for blocks before withdrawals were included
		case len(withdrawalHashBytes) > 0 && bytesToHash(withdrawalHashBytes) == emptyStateTrieHash:
			withdrawals = make([]map[string]interface{}, 0)
		default:
			withdrawals, err = getWithdrawals(ctx, db, "withdrawals.block = ?", number)
			if err != nil {
				log.Error("Error fetching withdrawals", "err", err.Error())
				return nil, err
			}
		}

		unclesList := []types.Hash{}
		rlp.DecodeBytes(uncles, &unclesList)
		var bn [8]byte
		binary.BigEndian.PutUint64(bn[:], uint64(nonce))
		fields := map[string]interface{}{
			"difficulty":       hexutil.Uint64(difficulty),
			"extraData":        hexutil.Bytes(extra),
			"gasLimit":         hexutil.Uint64(gasLimit),
			"gasUsed":          hexutil.Uint64(gasUsed),
			"hash":             bytesToHash(hash),
			"logsBloom":        hexutil.Bytes(logsBloom),
			"miner":            bytesToAddress(coinbase),
			"mixHash":          bytesToHash(mixDigest),
			"nonce":            hexutil.Bytes(bn[:]),
			"number":           hexutil.Uint64(number),
			"parentHash":       bytesToHash(parentHash),
			"receiptsRoot":     bytesToHash(receiptRoot),
			"sha3Uncles":       bytesToHash(uncleHash),
			"size":             hexutil.Uint64(size),
			"stateRoot":        bytesToHash(root),
			"timestamp":        hexutil.Uint64(time),
			"totalDifficulty":  bytesToHexBig(td),
			"transactionsRoot": bytesToHash(txRoot),
			"uncles":           unclesList,
		}
		if intermediateBGU != nil {
			blobGasUsed, ok := intermediateBGU.(uint64)
			if !ok {
				log.Error("Failed to convert blobGasUsed to uint64, getBlocks")
			}
			fields["blobGasUsed"] = blobGasUsed
		}
		if intermediateEBG != nil {
			excessBlobGas, ok := intermediateEBG.(uint64)
			if !ok {
				log.Error("Failed to convert excessBlobGas to uint64, getBlocks")
			}
			fields["excessBlobGas"] = excessBlobGas
		}
		if len(parentBeaconBlockRootBytes) > 0 {
			fields["parentBeaconBlockRoot"] = bytesToHash(parentBeaconBlockRootBytes)
		}
		if len(withdrawalHashBytes) > 0 {
			fields["withdrawalsRoot"] = bytesToHash(withdrawalHashBytes)
		}
		if withdrawals != nil {
			fields["withdrawals"] = withdrawals
		}
		if includeTxs {
			fields["transactions"], err = getTransactionsBlock(ctx, db, 0, 100000, chainid, "transactions.block = ?", number)
			if err != nil {
				return nil, err
			}
		} else {
			txs := []types.Hash{}
			txRows, err := db.QueryContext(ctx, "SELECT hash FROM transactions.transactions WHERE block = ? ORDER BY transactionIndex ASC", number)
			if err != nil {
				return nil, err
			}
			for txRows.Next() {
				var txHash []byte
				if err := txRows.Scan(&txHash); err != nil {
					return nil, err
				}
				txs = append(txs, bytesToHash(txHash))
			}
			if err := txRows.Err(); err != nil {
				return nil, err
			}
			fields["transactions"] = txs
		}
		if len(baseFee) > 0 {
			fields["baseFeePerGas"] = bytesToHexBig(baseFee)
		}
		results = append(results, fields)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func getPendingTransactions(ctx context.Context, db *sql.DB, mempool bool, offset, limit int, chainid uint64, whereClause string, params ...interface{}) ([]map[string]interface{}, error) {
	results := []map[string]interface{}{}
	if !mempool {
		return results, nil
	} 
	query := fmt.Sprintf("SELECT transactions.gas, transactions.gasPrice, transactions.hash, transactions.input, transactions.nonce, transactions.recipient, transactions.value, transactions.v, transactions.r, transactions.s, transactions.sender, transactions.type, transactions.access_list, transactions.gasFeeCap, transactions.gasTipCap FROM mempool.transactions WHERE %v LIMIT ? OFFSET ?;", whereClause)
	rows, err := db.QueryContext(ctx, query, append(params, limit, offset)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var amount, to, from, data, txHash, r, s, cAccessListRLP, gasFeeCapBytes, gasTipCapBytes []byte
		var nonce, gasLimit, gasPrice, v uint64
		var txTypeRaw sql.NullInt32
		err := rows.Scan(
			&gasLimit,
			&gasPrice,
			&txHash,
			&data,
			&nonce,
			&to,
			&amount,
			&v,
			&r,
			&s,
			&from,
			&txTypeRaw,
			&cAccessListRLP,
			&gasFeeCapBytes,
			&gasTipCapBytes,
		)
		if err != nil {
			return nil, err
		}
		txType := uint8(txTypeRaw.Int32)
		inputBytes, err := decompress(data)
		if err != nil {
			return nil, err
		}
		accessListRLP, err := decompress(cAccessListRLP)
		if err != nil {
			return nil, err
		}
		var accessList *evm.AccessList
		var chainID, gasFeeCap, gasTipCap *hexutil.Big
		//move below and assign to mao conditionally
		switch txType {
		case evm.AccessListTxType:
			accessList = &evm.AccessList{}
			rlp.DecodeBytes(accessListRLP, accessList)
			chainID = uintToHexBig(chainid)
		case evm.DynamicFeeTxType:
			accessList = &evm.AccessList{}
			rlp.DecodeBytes(accessListRLP, accessList)
			chainID = uintToHexBig(chainid)
			gasFeeCap = bytesToHexBig(gasFeeCapBytes)
			gasTipCap = bytesToHexBig(gasTipCapBytes)
		case evm.LegacyTxType:
			chainID = nil
		}
		results = append(results, map[string]interface{}{
			"from":       bytesToAddress(from),
			"gas":        hexutil.Uint64(gasLimit),
			"gasPrice":   uintToHexBig(gasPrice),
			"gasFeeCap":  gasFeeCap,
			"gasTipCap":  gasTipCap,
			"hash":       bytesToHash(txHash),
			"input":      hexutil.Bytes(inputBytes),
			"nonce":      hexutil.Uint64(nonce),
			"to":         bytesToAddressPtr(to),
			"value":      bytesToHexBig(amount),
			"v":          uintToHexBig(v),
			"r":          bytesToHexBig(r),
			"s":          bytesToHexBig(s),
			"type":       hexutil.Uint64(txType),
			"chainID":    chainID,
			"accessList": accessList,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	keys := []string{"chainID", "accessList", "maxFeePerGas", "maxPriorityFeePerGas"}
	for _, key := range keys {
		for _, item := range results {
			for k, v := range item {
				if k == key || v == nil {
					delete(item, k)
				}
			}
		}
	}
	return results, nil
}

func getTransactions(ctx context.Context, db *sql.DB, offset, limit int, chainid uint64, whereClause string, params ...interface{}) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT blocks.hash, transactions.block, transactions.gas, transactions.gasPrice, transactions.hash, transactions.input, transactions.nonce, transactions.recipient, transactions.transactionIndex, transactions.value, transactions.v, transactions.r, transactions.s, transactions.sender, transactions.type, transactions.access_list, blocks.baseFee, transactions.gasFeeCap, transactions.gasTipCap, transactions.blobVersionedHashes FROM transactions.transactions INNER JOIN blocks.blocks ON blocks.number = transactions.block WHERE transactions.rowid IN (SELECT transactions.rowid FROM transactions.transactions INNER JOIN blocks.blocks ON transactions.block = blocks.number WHERE %v) LIMIT ? OFFSET ?;", whereClause)
	return getTransactionsQuery(ctx, db, offset, limit, chainid, query, params...)
}

func getTransactionReceiptsQuery(ctx context.Context, db *sql.DB, offset, limit int, chainid uint64, query, logsQuery string, params ...interface{}) ([]map[string]interface{}, error) {
	logRows, err := db.QueryContext(ctx, logsQuery, params...)
	if err != nil {
		log.Error("Error selecting logs", "query", query, "err", err.Error())
		return nil, err
	}
	txLogs := make(map[types.Hash]sortLogs)
	for logRows.Next() {
		var txHashBytes, address, topic0, topic1, topic2, topic3, data []byte
		var logIndex uint
		var blockNumber uint64
		err := logRows.Scan(&txHashBytes, &blockNumber, &address, &topic0, &topic1, &topic2, &topic3, &data, &logIndex)
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
		logsBloom, err := decompress(bloomBytes)
		if err != nil {
			return nil, err
		}
		fields := map[string]interface{}{
			"blockHash":         bytesToHash(blockHash),
			"blockNumber":       hexutil.Uint64(blockNumber),
			"transactionHash":   bytesToHash(txHash),
			"transactionIndex":  hexutil.Uint64(txIndex),
			"from":              bytesToAddress(from),
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
		if address := bytesToAddress(contractAddress); address != (common.Address{}) {
			fields["contractAddress"] = address
		}
		txh := bytesToHash(txHash)
		for i := range txLogs[txh] {
			txLogs[txh][i].TxIndex = hexutil.Uint(txIndex)
			txLogs[txh][i].BlockHash = bytesToHash(blockHash)
		}
		logs, ok := txLogs[txh]
		if !ok {
			logs = sortLogs{}
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

func getTransactionReceipts(ctx context.Context, db *sql.DB, offset, limit int, chainid uint64, whereClause string, params ...interface{}) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT blocks.hash, transactions.block, transactions.gasUsed, transactions.cumulativeGasUsed, transactions.hash, transactions.recipient, transactions.transactionIndex, transactions.sender, transactions.contractAddress, transactions.logsBloom, transactions.status, transactions.type, transactions.gasPrice FROM transactions.transactions INNER JOIN blocks.blocks ON blocks.number = transactions.block WHERE transactions.rowid IN (SELECT transactions.rowid FROM transactions.transactions INNER JOIN blocks.blocks ON transactions.block = blocks.number WHERE %v) LIMIT ? OFFSET ?;", whereClause)
	logsQuery := fmt.Sprintf(`
		SELECT transactionHash, block, address, topic0, topic1, topic2, topic3, data, logIndex
		FROM event_logs
		WHERE (transactionHash, block) IN (
			SELECT transactions.hash, transactions.block
			FROM transactions.transactions INNER JOIN blocks.blocks ON transactions.block = blocks.number
			WHERE %v
		);`, whereClause)
	return getTransactionReceiptsQuery(ctx, db, offset, limit, chainid, query, logsQuery, params...)
}

func getTransactionReceiptsBlock(ctx context.Context, db *sql.DB, offset, limit int, chainid uint64, whereClause string, params ...interface{}) ([]map[string]interface{}, error) {
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

func getSenderNonce(ctx context.Context, db *sql.DB, sender common.Address, blockNumber rpc.BlockNumber, pending, mempool bool) (hexutil.Uint64, error) {
	
	var count sql.NullInt64
	if err := db.QueryRowContext(ctx, "SELECT max(nonce) FROM transactions.transactions WHERE sender = ? AND block <= ?", trimPrefix(sender.Bytes()), int64(blockNumber)).Scan(&count); err != nil {
		return 0, err
	}

	var nonce sql.NullInt64
	if pending && mempool{
		if err := db.QueryRowContext(ctx, "SELECT max(nonce) FROM mempool.transactions WHERE sender = ?", trimPrefix(sender.Bytes())).Scan(&nonce); err != nil {
			return 0, err
		}
	}
	if !nonce.Valid {
		if !count.Valid {
			return hexutil.Uint64(0), nil
		}
		return hexutil.Uint64(count.Int64 + 1), nil
	}
	if nonce.Int64 >= count.Int64 {
		return hexutil.Uint64(nonce.Int64 + 1), nil
	}
	// It shouldn't happen that the mempool has a lower nonce than confirmed
	// blocks, but just in case:
	return hexutil.Uint64(count.Int64 + 1), nil
}

func returnSingleTransaction(txs []map[string]interface{}) map[string]interface{} {
	var result map[string]interface{}
	if len(txs) > 0 {
		result = txs[0]
	} else {
		result = nil
	}
	return result
}

func txCount(ctx context.Context, db *sql.DB, whereClause string, params ...interface{}) (hexutil.Uint64, error) {
	var count uint64
	err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM transactions.transactions WHERE %v", whereClause), params...).Scan(&count)
	return hexutil.Uint64(count), err
}

func returnSingleReceipt(txs []map[string]interface{}) map[string]interface{} {
	var result map[string]interface{}
	if len(txs) > 0 {
		result = txs[0]
	} else {
		result = nil
	}
	return result
}

func getFlumeTransactions(ctx context.Context, db *sql.DB, offset, limit int, chainid uint64, whereClause string, params ...interface{}) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT blocks.hash, transactions.block, blocks.time, transactions.gas, transactions.gasPrice, transactions.hash, transactions.input, transactions.nonce, transactions.recipient, transactions.transactionIndex, transactions.value, transactions.v, transactions.r, transactions.s, transactions.sender, transactions.type, transactions.access_list, blocks.baseFee, transactions.gasFeeCap, transactions.gasTipCap FROM transactions.transactions INNER JOIN blocks.blocks ON blocks.number = transactions.block WHERE %v LIMIT ? OFFSET ?;", whereClause)
	return getFlumeTransactionsQuery(ctx, db, offset, limit, chainid, query, params...)
}

func getFlumeTransactionsQuery(ctx context.Context, db *sql.DB, offset, limit int, chainid uint64, query string, params ...interface{}) ([]map[string]interface{}, error) {
	rows, err := db.QueryContext(ctx, query, append(params, limit, offset)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var results sortTxMap
	for rows.Next() {
		var amount, to, from, data, blockHashBytes, txHash, r, s, cAccessListRLP, baseFeeBytes, gasFeeCapBytes, gasTipCapBytes []byte
		var nonce, gasLimit, blockNumber, gasPrice, time, txIndex, v uint64
		var txTypeRaw sql.NullInt32
		err := rows.Scan(
			&blockHashBytes,
			&blockNumber,
			&time,
			&gasLimit,
			&gasPrice,
			&txHash,
			&data,
			&nonce,
			&to,
			&txIndex,
			&amount,
			&v,
			&r,
			&s,
			&from,
			&txTypeRaw,
			&cAccessListRLP,
			&baseFeeBytes,
			&gasFeeCapBytes,
			&gasTipCapBytes,
		)
		if err != nil {
			return nil, err
		}
		txType := uint8(txTypeRaw.Int32)
		blockHash := bytesToHash(blockHashBytes)
		txIndexHex := hexutil.Uint64(txIndex)
		inputBytes, err := decompress(data)
		if err != nil {
			return nil, err
		}
		accessListRLP, err := decompress(cAccessListRLP)
		if err != nil {
			return nil, err
		}
		var accessList *evm.AccessList
	item := map[string]interface{}{
		"blockHash":            &blockHash,
		"blockNumber":          uintToHexBig(blockNumber),
		"from":                 bytesToAddress(from),
		"timestamp":         uintToHexBig(time),
		"gas":                  hexutil.Uint64(gasLimit),
		"gasPrice":             uintToHexBig(gasPrice),
		"hash":                 bytesToHash(txHash),
		"input":                hexutil.Bytes(inputBytes),
		"nonce":                hexutil.Uint64(nonce),
		"to":                   bytesToAddressPtr(to),
		"transactionIndex":     &txIndexHex,
		"value":                bytesToHexBig(amount),
		"v":                    uintToHexBig(v),
		"r":                    bytesToHexBig(r),
		"s":                    bytesToHexBig(s),
		"type":                 hexutil.Uint64(txType),
	}

	switch txType {
	case evm.AccessListTxType:
		accessList = &evm.AccessList{}
		rlp.DecodeBytes(accessListRLP, accessList)
		item["accessList"] = accessList
		item["chainId"] = uintToHexBig(chainid)
	case evm.DynamicFeeTxType:
		accessList = &evm.AccessList{}
		rlp.DecodeBytes(accessListRLP, accessList)
		item["accessList"] = accessList
		item["chainId"] = uintToHexBig(chainid)
		item["maxPriorityFeePerGas"] = bytesToHexBig(gasTipCapBytes)
		item["maxFeePerGas"] = bytesToHexBig(gasFeeCapBytes)
	}

	results = append(results, item)
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Sort(results)
	}
return results, nil
}

func getFlumeTransactionReceipts(ctx context.Context, db *sql.DB, offset, limit int, chainid uint64, whereClause string, params ...interface{}) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT blocks.hash, transactions.block, blocks.time, transactions.gasUsed, transactions.cumulativeGasUsed, transactions.hash, transactions.recipient, transactions.transactionIndex, transactions.sender, transactions.contractAddress, transactions.logsBloom, transactions.status, transactions.type, transactions.gasPrice FROM transactions.transactions INNER JOIN blocks.blocks ON blocks.number = transactions.block WHERE %v LIMIT ? OFFSET ?;", whereClause)
	logsQuery := fmt.Sprintf(`
		SELECT transactionHash, block, address, topic0, topic1, topic2, topic3, data, logIndex
		FROM event_logs
		WHERE (transactionHash, block) IN (
			SELECT transactions.hash, transactions.block
			FROM transactions.transactions INNER JOIN blocks.blocks ON transactions.block = blocks.number
			WHERE %v LIMIT ? OFFSET ?
		);`, whereClause)
	return getFlumeTransactionReceiptsQuery(ctx, db, offset, limit, chainid, query, logsQuery, params...)
}

func getFlumeTransactionReceiptsQuery(ctx context.Context, db *sql.DB, offset, limit int, chainid uint64, query, logsQuery string, params ...interface{}) ([]map[string]interface{}, error) {
	logRows, err := db.QueryContext(ctx, logsQuery, append(params, limit, offset)...)
	if err != nil {
		log.Error("Error selecting logs", "query", query, "err", err.Error())
		return nil, err
	}
	txLogs := make(map[types.Hash]sortLogs)
	for logRows.Next() {
		var txHashBytes, address, topic0, topic1, topic2, topic3, data []byte
		var logIndex uint
		var blockNumber uint64
		err := logRows.Scan(&txHashBytes, &blockNumber, &address, &topic0, &topic1, &topic2, &topic3, &data, &logIndex)
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
			Data:        input,
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
	results := sortTxMap{}
	for rows.Next() {
		var to, from, blockHash, txHash, contractAddress, bloomBytes []byte
		var blockNumber, txIndex, time, gasUsed, cumulativeGasUsed, status, gasPrice uint64
		var txTypeRaw sql.NullInt32
		err := rows.Scan(
			&blockHash,
			&blockNumber,
			&time,
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
		logsBloom, err := decompress(bloomBytes)
		if err != nil {
			return nil, err
		}
		fields := map[string]interface{}{
			"blockHash":         bytesToHash(blockHash),
			"blockNumber":       hexutil.Uint64(blockNumber),
			"timestamp":         uintToHexBig(time),
			"transactionHash":   bytesToHash(txHash),
			"transactionIndex":  hexutil.Uint64(txIndex),
			"from":              bytesToAddress(from),
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
		if address := bytesToAddress(contractAddress); address != (common.Address{}) {
			fields["contractAddress"] = address
		}
		txh := bytesToHash(txHash)
		for i := range txLogs[txh] {
			txLogs[txh][i].TxIndex = hexutil.Uint(txIndex)
			txLogs[txh][i].BlockHash = bytesToHash(blockHash)
		}
		logs, ok := txLogs[txh]
		if !ok {
			logs = sortLogs{}
		}
		sort.Sort(logs)
		fields["logs"] = logs
		results = append(results, fields)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Sort(results)
	return results, nil
}

func getFlumeTransactionReceiptsBlock(ctx context.Context, db *sql.DB, offset, limit int, chainid uint64, whereClause string, params ...interface{}) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT blocks.hash, transactions.block, blocks.time, transactions.gasUsed, transactions.cumulativeGasUsed, transactions.hash, transactions.recipient, transactions.transactionIndex, transactions.sender, transactions.contractAddress, transactions.logsBloom, transactions.status, transactions.type, transactions.gasPrice FROM transactions.transactions INNER JOIN blocks.blocks ON blocks.number = transactions.block WHERE %v ORDER BY transactions.rowid LIMIT ? OFFSET ?;", whereClause)
	logsQuery := fmt.Sprintf(`
		SELECT event_logs.transactionHash, event_logs.block, event_logs.address, event_logs.topic0, event_logs.topic1, event_logs.topic2, event_logs.topic3, event_logs.data, event_logs.logIndex
		FROM event_logs
		WHERE (transactionHash, block) IN (
			SELECT transactions.hash, block
			FROM transactions.transactions INNER JOIN blocks.blocks ON transactions.block = blocks.number
			WHERE %v LIMIT ? OFFSET ?
		);`, whereClause)
	return getFlumeTransactionReceiptsQuery(ctx, db, offset, limit, chainid, query, logsQuery, params...)

}

func getWithdrawals(ctx context.Context, db *sql.DB, whereClause string, params ...interface{}) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT withdrawals.wtdrlIndex, withdrawals.vldtrIndex, withdrawals.address, withdrawals.amount FROM withdrawals WHERE %v;", whereClause)
	rows, err := db.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var results []map[string]interface{}
	for rows.Next() {
		var addressBytes []byte
		var wtdrlIdx, vldtrIdx, amount uint64
		err := rows.Scan(
			&wtdrlIdx,
			&vldtrIdx,
			&addressBytes,
			&amount,
		)
		if err != nil {
			log.Error("Error retrieving withdrawal data", "err", err.Error())
			return nil, err
		}
		item := map[string]interface{}{
			"index":            hexutil.Uint64(wtdrlIdx),
			"validatorIndex":   hexutil.Uint64(vldtrIdx),
			"address":        bytesToAddress(addressBytes),
			"amount":           hexutil.Uint64(amount),
		}

		results = append(results, item)

		if err := rows.Err(); err != nil {
			log.Error("Error loading withdrawal data", "err", err.Error())
			return nil, err
		}
	}
	return results, nil
}
