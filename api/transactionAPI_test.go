package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	_ "net/http/pprof"

	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-evm/common"
	"github.com/openrelayxyz/cardinal-flume/config"
	"github.com/openrelayxyz/cardinal-flume/plugins"
	rpc "github.com/openrelayxyz/cardinal-rpc"
	types "github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-types/hexutil"
)

// TODO: eip 4844 -> some changes were necessary both in this file and and the flumeAPI test file to accommodate api changes to support EIP 4844
// at some point we need to go in and investigate the discrepencies and address them.

func getTransactionsForTesting(blockObject []map[string]json.RawMessage) []map[string]json.RawMessage {
	result := []map[string]json.RawMessage{}
	for _, block := range blockObject {
		txns := []map[string]json.RawMessage{}
		json.Unmarshal(block["transactions"], &txns)
		result = append(result, txns...)
	}
	log.Info("result", "len", len(result))
	return result
}

func getTransactionsListsForTesting(blockObject []map[string]json.RawMessage) [][]map[string]json.RawMessage {
	result := [][]map[string]json.RawMessage{}
	for _, block := range blockObject {
		txns := []map[string]json.RawMessage{}
		json.Unmarshal(block["transactions"], &txns)
		result = append(result, txns)
	}
	return result
}

func getTransactionHashes(blockObject []map[string]json.RawMessage) []types.Hash {
	result := []types.Hash{}
	for _, block := range blockObject {
		txnLevel := []map[string]interface{}{}
		json.Unmarshal(block["transactions"], &txnLevel)
		if len(txnLevel) > 0 {
			for _, tx := range txnLevel {
				result = append(result, types.HexToHash(tx["hash"].(string)))
			}
		}
	}
	return result
}

func getSenderAddreses(blockObject []map[string]json.RawMessage) []common.Address {
	result := []common.Address{}
	for _, block := range blockObject {
		txnLevel := []map[string]interface{}{}
		json.Unmarshal(block["transactions"], &txnLevel)
		if len(txnLevel) > 0 {
			for _, tx := range txnLevel {
				result = append(result, common.HexToAddress(tx["from"].(string)))
			}
		}
	}
	return result
}

func removeDuplicateValues(addressSlice []common.Address) []common.Address {
	keys := make(map[common.Address]bool)
	list := []common.Address{}

	for _, entry := range addressSlice {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}

func TestTransactionAPI(t *testing.T) {
	cfg, err := config.LoadConfig("../testing-resources/api_test_config.yml")
	if err != nil {
		t.Fatal("Error parsing config TestTransactionAPI", "err", err.Error())
	}
	db, mempool, err := connectToDatabase(cfg)
	if err != nil {
		t.Fatal(err.Error())
	}
	for _, path := range cfg.Databases {
		defer os.Remove(path + "-wal")
		defer os.Remove(path + "-shm")
	}
	defer db.Close()
	pl, _ := plugins.NewPluginLoader(cfg)
	tx := NewTransactionAPI(db, 1, pl, cfg, mempool)
	blockObject, _ := blocksDecompress()
	receiptsMap, _ := receiptsDecompress()
	transactionLists := getTransactionsListsForTesting(blockObject)
	transactions := getTransactionsForTesting(blockObject)
	txHashes := getTransactionHashes(blockObject)

	for i, hash := range txHashes {
		expectedTx := transactions[i]
		t.Run(fmt.Sprintf("GetTransactionByHash %v", i), func(t *testing.T) {
			actual, err := tx.GetTransactionByHash(context.Background(), hash)
			if err != nil {
				t.Fatal(err.Error())
			}
			actualTx := *actual
			for key, expectedValue := range expectedTx {
				if key == "chainId" {
					continue
				}
				if key == "accessList" {
					accessListRoutine(t, actualTx["accessList"], expectedValue, "GetTransactionByHash", hash, actualTx["transactionIndex"])
					continue
				}
				data, err := json.Marshal(actualTx[key])
				if err != nil {
					t.Fatalf("failed to marshal actual transactions %v on block:%v, err:%v", key,i,err)
				}
				if !bytes.Equal(data, expectedValue) {
					t.Fatalf("error on getTransactionByHash, \nindex %v, key %v; \n actual result: %v, \n expected result: %v", i, key, string(data), string(expectedValue))
				}
			}
		})
		t.Run(fmt.Sprintf("GetTransactionReceipt%v", i), func(t *testing.T) {
			expectedReceipt := receiptsMap[i]
			actual, _ := tx.GetTransactionReceipt(context.Background(), hash)
			// TODO: eip 4844 (both the comment and the nested if below)
			// if len(*actual)+1 != len(receiptsMap[i]) {
			// 	t.Fatalf("length error GetTransactionReceipt on hash %v", hash)
			// }

			actualReceipt := *actual;
			for key, expectedValue := range expectedReceipt {
				if key == "root" {
					continue
				} else {
					actualValue := actualReceipt[key]
					data, err := json.Marshal(actualValue)
					if err != nil {
						t.Fatalf("failed to marshal actual receipts %v on block:%v, err:%v", key,i,err)
					}
					if !bytes.Equal(data, expectedValue) {
						t.Fatalf("error on getTransactionReceipt, \n index %v, key %v; \n actual result: %v, \n expected result: %v", i, key, string(data), string(expectedValue))
					}
				}
			}
		})
	}
	for i, block := range blockObject {
		expectedTxList := transactionLists[i]
		t.Run(fmt.Sprintf("GetTransactionByBlockHashAndIndex %v", i), func(t *testing.T) {
			var blockHash types.Hash
			if err := json.Unmarshal(block["hash"], &blockHash); err != nil {
				t.Fatalf("failed to unmarshal block hash: %v", err)
			}

			for j, expectedTx := range expectedTxList {
				actual, err := tx.GetTransactionByBlockHashAndIndex(context.Background(), blockHash, hexutil.Uint64(j)); if err!=nil{
					t.Fatalf("failed to getTransactionByBlockHashAndIndex at block %v index %v: %v", i, j, err)
				}
				actualTx := *actual
				for key, expectedValue := range expectedTx {
					if key == "chainId" {
						continue
					}
					if key == "accessList" {
						accessListRoutine(t, actualTx["accessList"], expectedValue, "GetTransactionByBlockHashAndIndex", blockHash, hexutil.Uint64(j))
						continue
					}
					data, err := json.Marshal(actualTx[key])
					if err != nil {
						t.Fatalf("failed to marshal actual transactions %v on block:%v, err:%v", key,i,err)
					}
					if !bytes.Equal(data, expectedValue) {
						t.Fatalf("error on getTransactionByBlockHashAndIndex, \n index %v, key %v; \n actual result: %v, \n expected result: %v", i, key, string(data), string(expectedValue))
					}
				}
			}
		})
		t.Run(fmt.Sprintf("GetTransactionByBlockNumberAndIndex %v", i), func(t *testing.T) {
			expectedTxList := transactionLists[i]
			var blockNo rpc.BlockNumber
			json.Unmarshal(block["number"], &blockNo)
			for j, expectedTx := range expectedTxList {
				actual, err := tx.GetTransactionByBlockNumberAndIndex(context.Background(), blockNo, hexutil.Uint64(j)); if err!=nil{
					t.Fatalf("failed to getTransactionByBlockNumberAndIndex at block %v index %v: %v", i, j, err)
				}
				actualTx := *actual
				for key, expectedValue := range expectedTx {
					if key == "chainId" {
						continue
					}
					if key == "accessList" {
						accessListRoutine(t, actualTx["accessList"], expectedValue, "GetTransactionByBlockNumberAndIndex", blockNo, hexutil.Uint64(j))
						continue
					}
					data, err := json.Marshal(actualTx[key])
					if err != nil {
						t.Fatalf("failed to marshal actual transactions %v on block:%v, err:%v", key,i,err)
					}
					if !bytes.Equal(data, expectedValue) {
						t.Fatalf("error on getTransactionByBlockNumberAndIndex, \n index %v, key %v; \n actual result: %v, \n expected result: %v", i, key, string(data), string(expectedValue))
					}
				}
			}
		})
	}
	nonces := make(map[common.Address]hexutil.Uint64)
	for _, tx := range transactions {
		var sender common.Address
		json.Unmarshal(tx["from"], &sender)
		var nonce hexutil.Uint64
		json.Unmarshal(tx["nonce"], nonce)
		if nonces[sender] < nonce {
			nonces[sender] = nonce
		}
	}

	for sender, nonce := range nonces {
		t.Run(fmt.Sprintf("GetTransactionCount"), func(t *testing.T) {
			actual, _ := tx.GetTransactionCount(context.Background(), sender, rpc.LatestBlockNumber)
			if *actual != nonce {
				t.Fatalf("GetTransactionCountError %v %v", actual, nonce)
			}
		})
	}
}
