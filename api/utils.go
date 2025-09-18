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

func dedup[T comparable](sliceA, sliceB []T) []T {
	set := make(map[T]bool)

	for _, slice := range [][]T{sliceA, sliceB} {
		for _, item := range slice {
			set[item] = true
		}
	}

	unique := make([]T, 0, len(set))
	for item := range set {
		unique = append(unique, item)
	}

	return unique
}

func exhaustChannels[T any](ch chan T, errChan chan error) {
	go func() {
		select {
		case <- ch:
		case <- errChan:
		}
	}()
}

func blockDataPresent(input interface{}, cfg *config.Config, db *sql.DB) bool {
	present := true
	switch input.(type) {
	case rpc.BlockNumber:
		if w := cfg.Waiter; w != nil {
			w.WaitForNumber(int64(input.(rpc.BlockNumber)), cfg.WaitTime)
		}
		if uint64(input.(rpc.BlockNumber)) < cfg.EarliestBlock {
			present = false
			return present
		}
	case types.Hash:
		if w := cfg.Waiter; w != nil {
			w.WaitForHash(input.(types.Hash), cfg.WaitTime)
		}
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

func receiptDataPresentBlock(input types.Hash, cfg *config.Config, db *sql.DB) bool {
	var present bool
	var response int
	
	if w := cfg.Waiter; w != nil {
		w.WaitForHash(input, cfg.WaitTime)
	}
	statement := "SELECT number FROM blocks.blocks WHERE hash = ?;"
	db.QueryRow(statement, trimPrefix(input.Bytes())).Scan(&response)
	if response != 0 && uint64(response -1) >= cfg.EarliestBlock {
		present = true
	}
	return present
}

func receiptDataPresentTx(input types.Hash, cfg *config.Config, db *sql.DB) bool {
	var present bool
	var response int
	
	statement := "SELECT block FROM transactions.transactions WHERE hash = ?;"
	db.QueryRow(statement, trimPrefix(input.Bytes())).Scan(&response)
	if response != 0 && uint64(response -1) >= cfg.EarliestBlock {
		present = true
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

func isEIP(db *sql.DB, time, blockNumber uint64, eip string) bool {
	var response int
	statement := "SELECT 1 FROM blocks.features WHERE eip = ? AND ((startTime IS NOT NULL AND startTime <= ?) OR (startBlock IS NOT NULL AND startBlock <= ?));"
	db.QueryRow(statement, eip, time, blockNumber).Scan(&response)
	return response > 0
}

func getTransactionsQuery(ctx context.Context, db *sql.DB, offset, limit int, chainid uint64, query string, params ...interface{}) ([]map[string]interface{}, error) {
	rows, err := db.QueryContext(ctx, query, append(params, limit, offset)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	results := []map[string]interface{}{}
	for rows.Next() {
		var amount, to, from, data, blockHashBytes, txHash, r, s, cAccessListRLP, baseFeeBytes, gasFeeCapBytes, gasTipCapBytes, blobGasFeeBytes, bVHashesRLP, authListRLP []byte
		var nonce, gasLimit, blockNumber, gasPrice, txIndex, v uint64
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
			&blobGasFeeBytes,
			&bVHashesRLP,
			&authListRLP,
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
			item["yParity"] = uintToHexBig(v)
		case evm.DynamicFeeTxType:
			accessList = &evm.AccessList{}
			rlp.DecodeBytes(accessListRLP, accessList)
			item["accessList"] = accessList
			item["chainId"] = uintToHexBig(chainid)
			item["maxPriorityFeePerGas"] = bytesToHexBig(gasTipCapBytes)
			item["maxFeePerGas"] = bytesToHexBig(gasFeeCapBytes)
			item["yParity"] = uintToHexBig(v)		
		case evm.BlobTxType:
			accessList = &evm.AccessList{}
			rlp.DecodeBytes(accessListRLP, accessList)
			item["accessList"] = accessList
			item["chainId"] = uintToHexBig(chainid)
			item["maxPriorityFeePerGas"] = bytesToHexBig(gasTipCapBytes)
			item["maxFeePerGas"] = bytesToHexBig(gasFeeCapBytes)
			item["yParity"] = uintToHexBig(v)			
			item["maxFeePerBlobGas"] = bytesToHexBig(blobGasFeeBytes)
			if len(bVHashesRLP) > 0 {
				bVHashes := &[]types.Hash{}
				if err = rlp.DecodeBytes(bVHashesRLP, bVHashes); err != nil {
					log.Error("Error rlp decoding blockVersionedHashes, getTransactionsQuery", "err", err)
				}
				item["blobVersionedHashes"] = bVHashes
			}
		case evm.SetCodeTxType:
			accessList = &evm.AccessList{}
			rlp.DecodeBytes(accessListRLP, accessList)
			item["accessList"] = accessList
			item["chainId"] = uintToHexBig(chainid)
			item["maxPriorityFeePerGas"] = bytesToHexBig(gasTipCapBytes)
			item["maxFeePerGas"] = bytesToHexBig(gasFeeCapBytes)
			item["yParity"] = uintToHexBig(v)			
			item["maxFeePerBlobGas"] = bytesToHexBig(blobGasFeeBytes)
			if len(authListRLP) > 0 {
				authList := &[]evm.Authorization{}
				if err = rlp.DecodeBytes(authListRLP, authList); err != nil {
					log.Error("Error rlp decoding authList, getTransactionsQuery", "err", err)
				}
				item["authorizationList"] = authList
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
	query := fmt.Sprintf("SELECT blocks.hash, transactions.block, transactions.gas, transactions.gasPrice, transactions.hash, transactions.input, transactions.nonce, transactions.recipient, transactions.transactionIndex, transactions.value, transactions.v, transactions.r, transactions.s, transactions.sender, transactions.type, transactions.access_list, blocks.baseFee, transactions.gasFeeCap, transactions.gasTipCap, transactions.maxFeePerBlobGas, transactions.blobVersionedHashes, transactions.authListBytes FROM transactions.transactions INNER JOIN blocks.blocks ON blocks.number = transactions.block WHERE %v ORDER BY transactions.transactionIndex LIMIT ? OFFSET ?;", whereClause)
	return getTransactionsQuery(ctx, db, offset, limit, chainid, query, params...)
}

var emptyStateTrieHash types.Hash = types.HexToHash("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")


func getBlocks(ctx context.Context, db *sql.DB, includeTxs bool, chainid uint64, whereClause string, params ...interface{}) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT hash, parentHash, uncleHash, coinbase, root, txRoot, receiptRoot, bloom, difficulty, extra, mixDigest, uncles, td, number, gasLimit, gasUsed, time, nonce, size, baseFee, withdrawalHash, blobGasUsed, excessBlobGas, parentBeaconRoot, requestsHash FROM blocks.blocks WHERE %v;", whereClause)
	rows, err := db.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	results := []map[string]interface{}{}
	for rows.Next() {
		var hash, parentHash, uncleHash, coinbase, root, txRoot, receiptRoot, bloomBytes, extra, mixDigest, uncles, td, baseFee, withdrawalHashBytes, parentBeaconBlockRootBytes, requestsHashBytes []byte
		var number, gasLimit, gasUsed, time, size, difficulty uint64
		var nonce int64
		var intermediateBGU, intermediateEBG nullable[int64]
		err := rows.Scan(&hash, &parentHash, &uncleHash, &coinbase, &root, &txRoot, &receiptRoot, &bloomBytes, &difficulty, &extra, &mixDigest, &uncles, &td, &number, &gasLimit, &gasUsed, &time, &nonce, &size, &baseFee, &withdrawalHashBytes, &intermediateBGU, &intermediateEBG, &parentBeaconBlockRootBytes, &requestsHashBytes)
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
		if intermediateBGU.Valid {
			fields["blobGasUsed"] = hexutil.EncodeUint64(uint64(intermediateBGU.Actual))
		}
		if intermediateBGU.Valid {
			fields["excessBlobGas"] = hexutil.EncodeUint64(uint64(intermediateEBG.Actual)) 
		}
		if len(parentBeaconBlockRootBytes) > 0 {
			fields["parentBeaconBlockRoot"] = bytesToHash(parentBeaconBlockRootBytes)
		}
		if len(withdrawalHashBytes) > 0 {
			fields["withdrawalsRoot"] = bytesToHash(withdrawalHashBytes)
		}
		if len(requestsHashBytes) > 0 {
			fields["requestsHash"] = bytesToHash(requestsHashBytes)
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
	query := fmt.Sprintf("SELECT transactions.gas, transactions.gasPrice, transactions.hash, transactions.input, transactions.nonce, transactions.recipient, transactions.value, transactions.v, transactions.r, transactions.s, transactions.sender, transactions.type, transactions.access_list, transactions.gasFeeCap, transactions.gasTipCap, transactions.maxFeePerBlobGas, transactions.blobVersionedHashes, transactions.authListBytes FROM mempool.transactions WHERE %v LIMIT ? OFFSET ?;", whereClause)
	rows, err := db.QueryContext(ctx, query, append(params, limit, offset)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var amount, to, from, data, txHash, r, s, cAccessListRLP, gasFeeCapBytes, gasTipCapBytes, blobGasFeeBytes, bVHashesRLP, authListRLP []byte
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
			&blobGasFeeBytes,
			&bVHashesRLP,
			&authListRLP,
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
		var nilBlockHash *interface{}
		var nilBlockNumber *interface{}
		var nilTxIndex *interface{}
		item := map[string]interface{}{
			"blockHash": nilBlockHash,
			"blockNumber": nilBlockNumber,
			"transactionIndex": nilTxIndex,
			"from":       bytesToAddress(from),
			"gas":        hexutil.Uint64(gasLimit),
			"gasPrice":   uintToHexBig(gasPrice),
			"hash":       bytesToHash(txHash),
			"input":      hexutil.Bytes(inputBytes),
			"nonce":      hexutil.Uint64(nonce),
			"to":         bytesToAddressPtr(to),
			"value":      bytesToHexBig(amount),
			"v":          uintToHexBig(v),
			"r":          bytesToHexBig(r),
			"s":          bytesToHexBig(s),
			"type":       hexutil.Uint64(txType),
			"chainId":	  uintToHexBig(chainid),
		}

		switch txType {
		case evm.AccessListTxType:
			accessList = &evm.AccessList{}
			rlp.DecodeBytes(accessListRLP, accessList)
			item["accessList"] = accessList
			item["chainId"] = uintToHexBig(chainid)
			item["yParity"] = uintToHexBig(v)
		case evm.DynamicFeeTxType:
			accessList = &evm.AccessList{}
			rlp.DecodeBytes(accessListRLP, accessList)
			item["accessList"] = accessList
			item["maxPriorityFeePerGas"] = bytesToHexBig(gasTipCapBytes)
			item["maxFeePerGas"] = bytesToHexBig(gasFeeCapBytes)
			item["yParity"] = uintToHexBig(v)
		case evm.BlobTxType:
			accessList = &evm.AccessList{}
			rlp.DecodeBytes(accessListRLP, accessList)
			item["accessList"] = accessList
			item["maxPriorityFeePerGas"] = bytesToHexBig(gasTipCapBytes)
			item["maxFeePerGas"] = bytesToHexBig(gasFeeCapBytes)
			item["yParity"] = uintToHexBig(v)			
			item["maxFeePerBlobGas"] = bytesToHexBig(blobGasFeeBytes)
			if len(bVHashesRLP) > 0 {
				bVHashes := &[]types.Hash{}
				if err = rlp.DecodeBytes(bVHashesRLP, bVHashes); err != nil {
					log.Error("Error rlp decoding blockVersionedHashes, getTransactionsQuery", "err", err)
				}
				item["blobVersionedHashes"] = bVHashes
			}
		case evm.SetCodeTxType:
			accessList = &evm.AccessList{}
			rlp.DecodeBytes(accessListRLP, accessList)
			item["accessList"] = accessList
			item["chainId"] = uintToHexBig(chainid)
			item["maxPriorityFeePerGas"] = bytesToHexBig(gasTipCapBytes)
			item["maxFeePerGas"] = bytesToHexBig(gasFeeCapBytes)
			item["yParity"] = uintToHexBig(v)			
			item["maxFeePerBlobGas"] = bytesToHexBig(blobGasFeeBytes)
			if len(authListRLP) > 0 {
				authList := &[]evm.Authorization{}
				if err = rlp.DecodeBytes(authListRLP, authList); err != nil {
					log.Error("Error rlp decoding authList, getTransactionsQuery", "err", err)
				}
				item["authorizationList"] = authList
			}
		}
		results = append(results, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	
	return results, nil
}

func getTransactions(ctx context.Context, db *sql.DB, offset, limit int, chainid uint64, whereClause string, params ...interface{}) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT blocks.hash, transactions.block, transactions.gas, transactions.gasPrice, transactions.hash, transactions.input, transactions.nonce, transactions.recipient, transactions.transactionIndex, transactions.value, transactions.v, transactions.r, transactions.s, transactions.sender, transactions.type, transactions.access_list, blocks.baseFee, transactions.gasFeeCap, transactions.gasTipCap, transactions.blobVersionedHashes, transactions.authListBytes FROM transactions.transactions INNER JOIN blocks.blocks ON blocks.number = transactions.block WHERE transactions.rowid IN (SELECT transactions.rowid FROM transactions.transactions INNER JOIN blocks.blocks ON transactions.block = blocks.number WHERE %v) LIMIT ? OFFSET ?;", whereClause)
	return getTransactionsQuery(ctx, db, offset, limit, chainid, query, params...)
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
		item["yParity"] = uintToHexBig(v)
	case evm.DynamicFeeTxType:
		accessList = &evm.AccessList{}
		rlp.DecodeBytes(accessListRLP, accessList)
		item["accessList"] = accessList
		item["chainId"] = uintToHexBig(chainid)
		item["maxPriorityFeePerGas"] = bytesToHexBig(gasTipCapBytes)
		item["maxFeePerGas"] = bytesToHexBig(gasFeeCapBytes)
		item["yParity"] = uintToHexBig(v)
	}

	results = append(results, item)
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Sort(results)
	}
return results, nil
}

func getTransactionReceipts(ctx context.Context, db *sql.DB, offset, limit int, chainid uint64, whereClause string, params ...interface{}) ([]map[string]interface{}, error) {
	var postBlast int
	var query string
	statement := "SELECT 1 FROM transactions.transactions WHERE id > 0 LIMIT 1;"
	db.QueryRow(statement).Scan(&postBlast)
	if postBlast == 0 {
		query = fmt.Sprintf(`SELECT blocks.hash, blocks.time, prev_blocks.blobGasUsed AS prev_blobGasUsed, prev_blocks.excessBlobGas AS prev_excessBlobGas, prev_blocks.baseFee AS prev_baseFee, transactions.block, transactions.gasUsed, transactions.cumulativeGasUsed, transactions.hash, transactions.recipient, transactions.transactionIndex, transactions.sender, transactions.contractAddress, transactions.logsBloom, transactions.status, transactions.type, transactions.gasPrice, transactions.blobVersionedHashes, blobSchedule.target, blobSchedule.max, blobSchedule.updateFrac 
		FROM transactions.transactions 
		INNER JOIN blocks.blocks ON blocks.number = transactions.block 
		LEFT JOIN blocks.blocks AS prev_blocks ON prev_blocks.number = blocks.number - 1
		LEFT JOIN blocks.blobSchedule ON blocks.time BETWEEN blobSchedule.startTime AND blobSchedule.endTime 
		WHERE %v ORDER BY transactions.block, transactions.transactionIndex LIMIT ? OFFSET ?;`, whereClause)
	} else {
		query = fmt.Sprintf(`SELECT blocks.hash, blocks.time, prev_blocks.blobGasUsed AS prev_blobGasUsed, prev_blocks.excessBlobGas AS prev_excessBlobGas, prev_blocks.baseFee AS prev_baseFee, transactions.block, transactions.gasUsed, transactions.cumulativeGasUsed, transactions.hash, transactions.recipient, transactions.transactionIndex, transactions.sender, transactions.contractAddress, transactions.logsBloom, transactions.status, transactions.type, transactions.gasPrice, transactions.blobVersionedHashes, blobSchedule.target, blobSchedule.max, blobSchedule.updateFrac 
		FROM transactions.transactions 
		INNER JOIN blocks.blocks ON blocks.number = transactions.block 
		LEFT JOIN blocks.blocks AS prev_blocks ON prev_blocks.number = blocks.number - 1
		LEFT JOIN blocks.blobSchedule ON blocks.time BETWEEN blobSchedule.startTime AND blobSchedule.endTime 
		WHERE %v ORDER BY transactions.rowid LIMIT ? OFFSET ?;`, whereClause)
	}
	
	logsQuery := fmt.Sprintf(`
		SELECT event_logs.transactionHash, event_logs.block, event_logs.address, event_logs.topic0, event_logs.topic1, event_logs.topic2, event_logs.topic3, event_logs.data, event_logs.logIndex, blocks.time
		FROM event_logs
		INNER JOIN blocks.blocks ON event_logs.block = blocks.number
		WHERE (event_logs.transactionHash, event_logs.block) IN (
			SELECT transactions.hash, block
			FROM transactions.transactions INNER JOIN blocks.blocks ON transactions.block = blocks.number
			WHERE %v LIMIT ? OFFSET ?
		);`, whereClause)
	return getTransactionReceiptsQuery(ctx, db, offset, limit, chainid, query, logsQuery, params...)

}

func getTransactionReceiptsQuery(ctx context.Context, db *sql.DB, offset, limit int, chainid uint64, query, logsQuery string, params ...interface{}) ([]map[string]interface{}, error) {
	logRows, err := db.QueryContext(ctx, logsQuery, append(params, limit, offset)...)
	if err != nil {
		log.Error("Error selecting logs", "query", query, "err", err.Error())
		return nil, err
	}
	txLogs := make(map[types.Hash]sortLogs)
	for logRows.Next() {
		var txHashBytes, address, topic0, topic1, topic2, topic3, data []byte
		var logIndex uint
		var blockNumber, time uint64
		err := logRows.Scan(&txHashBytes, &blockNumber, &address, &topic0, &topic1, &topic2, &topic3, &data, &logIndex, &time)
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
			BlockTimestamp: hexutil.EncodeUint64(time),
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
		var to, from, blockHash, txHash, contractAddress, bloomBytes, bVHashesRLP, prevBaseFee []byte
		var blockNumber, txIndex, time, gasUsed, cumulativeGasUsed, status, gasPrice uint64
		var prevExcessBlobGas, prevBlobGasUsed, blobScheduleTarget, blobScheduleMax, blobScheduleUpdateFraction nullable[int64]
		var txTypeRaw sql.NullInt32
		err := rows.Scan(
			&blockHash,
			&time,
			&prevBlobGasUsed,
			&prevExcessBlobGas,
			&prevBaseFee,
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
			&bVHashesRLP,
			&blobScheduleTarget,
			&blobScheduleMax,
			&blobScheduleUpdateFraction,
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
		if txType == evm.BlobTxType {
			if len(bVHashesRLP) > 0 {
				bVHashes := &[]types.Hash{}
				if err = rlp.DecodeBytes(bVHashesRLP, bVHashes); err != nil {
					log.Error("Error rlp decoding blockVersionedHashes, getTransactionsQuery", "err", err)
				}

				fields["blobGasUsed"] = hexutil.EncodeUint64(uint64(blobTxBlobGasPerBlob * len(*bVHashes)))
			}
			var pebg, pbgu int64
			if prevExcessBlobGas.Valid {
				pebg = prevExcessBlobGas.Actual
			}
			if prevBlobGasUsed.Valid {
				pbgu = prevBlobGasUsed.Actual
			}
			if blobScheduleUpdateFraction.Actual == 0 {
				log.Error("The update fraction is zero")
			}
			excess := calcExcessBlobGas(pebg, pbgu, blobScheduleTarget.Actual, blobScheduleMax.Actual, big.NewInt(int64(blobScheduleUpdateFraction.Actual)), new(big.Int).SetBytes(prevBaseFee), isEIP(db, time, blockNumber, "7918"))
			fields["blobGasPrice"] = fakeExponential(big.NewInt(int64(blobTxMinBlobGasprice)), big.NewInt(int64(excess)),  big.NewInt(int64(blobScheduleUpdateFraction.Actual)))
		}
		results = append(results, fields)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Sort(results)
	return results, nil
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

func getBaseFeeDenominator(db *sql.DB, blockNumber int64) *big.Int {

	var denominator int64
	statement := "SELECT denominator FROM blocks.baseFeeDenominatorSchedule where startBlock < ? AND endBlock > ?;"
	db.QueryRow(statement, blockNumber, blockNumber).Scan(&denominator)

	return big.NewInt(denominator)
}

// eip4844 helper functions

var (
	blobTxBlobGasPerBlob = 1 << 17 // Gas consumption of a single data blob (== blob byte size)
	blobTxMinBlobGasprice  = 1 // Minimum gas price for data blobs
	blobBaseCost = big.NewInt(1 << 13) // Base execution gas cost for a blob.
	minBlobGasPrice = big.NewInt(int64(blobTxMinBlobGasprice))
)

func calcExcessBlobGas(parentExcessBlobGas, parentBlobGasUsed, target, max int64, updateFraction, parentBaseFee *big.Int, osakaActive bool) uint64 {

	excessBlobGas := uint64(parentExcessBlobGas + parentBlobGasUsed)
	targetGas := uint64(target) * uint64(blobTxBlobGasPerBlob)
	if excessBlobGas < targetGas {
		return 0
	}
	
	if osakaActive {
		
		reservePrice := blobBaseCost.Mul(blobBaseCost, parentBaseFee)
		blobPrice    := blobPrice(parentExcessBlobGas, updateFraction)
		
		if reservePrice.Cmp(blobPrice) > 0 {
			scaledExcess := parentBlobGasUsed * (max-target) / max
			return uint64(parentExcessBlobGas + scaledExcess)
		}
	}

	return excessBlobGas - targetGas
}

func blobPrice(excessBlobGas int64, updateFraction *big.Int) *big.Int {
	f := blobBaseFee(uint64(excessBlobGas), updateFraction)
	return new(big.Int).Mul(f, big.NewInt(int64(blobTxBlobGasPerBlob)))
}

func blobBaseFee(excessBlobGas uint64, updateFraction *big.Int) *big.Int {
	return fakeExponential(minBlobGasPrice, new(big.Int).SetUint64(excessBlobGas), new(big.Int).SetUint64(updateFraction.Uint64())).ToInt()
}


func fakeExponential(factor, numerator, denominator *big.Int) *hexutil.Big {
	if denominator == nil {
		log.Error("the denominator in fake exponential is nil")
	}
	var (
		output = new(big.Int)
		accum  = new(big.Int).Mul(factor, denominator)
	)
	for i := 1; accum.Sign() > 0; i++ {
		output.Add(output, accum)

		accum.Mul(accum, numerator)
		accum.Div(accum, denominator)
		accum.Div(accum, big.NewInt(int64(i)))
	}
	return (*hexutil.Big)(output.Div(output, denominator)) 
}