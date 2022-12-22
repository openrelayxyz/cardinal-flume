package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"os"

	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-evm/common"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-types/hexutil"
	"github.com/openrelayxyz/cardinal-flume/config"
	"github.com/openrelayxyz/cardinal-flume/plugins"
	_ "net/http/pprof"
)

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
	db, err := connectToDatabase(cfg)
	if err != nil {
		t.Fatal(err.Error())
	}
	for _, path := range cfg.Databases {
		defer os.Remove(path + "-wal")
		defer os.Remove(path + "-shm")
	}
	defer db.Close()
	pl, _ := plugins.NewPluginLoader(cfg)
	tx := NewTransactionAPI(db, 1, pl, cfg)
	blockObject, _ := blocksDecompress()
	receiptsMap, _ := receiptsDecompress()
	transactionLists := getTransactionsListsForTesting(blockObject)
	transactions := getTransactionsForTesting(blockObject)
	txHashes := getTransactionHashes(blockObject)

	for i, hash := range txHashes {
		t.Run(fmt.Sprintf("GetTransactionByHash %v", i), func(t *testing.T) {
			actual, err := tx.GetTransactionByHash(context.Background(), hash)
			if err != nil {
				t.Fatal(err.Error())
			}
			if len(*actual) != len(transactions[i]) {
				t.Fatalf("length error GetTransactionByHash on hash %v", hash)
			}
			for k, v := range *actual {
				data, err := json.Marshal(v)
				if err != nil {
					t.Errorf("marshalling error gtbh on key: %v", k)
				}
				if !bytes.Equal(data, transactions[i][k]) {
					t.Fatalf("error on transaction%v, key%v", hash, k)
				}
			}
		})
		t.Run(fmt.Sprintf("GetTransactionReceipt%v", i), func(t *testing.T) {
			actual, _ := tx.GetTransactionReceipt(context.Background(), hash)
			if len(*actual) != len(receiptsMap[i]) {
				t.Fatalf("length error GetTransactionReceipt on hash %v", hash)
			}
			for k, v := range *actual {
				data, err := json.Marshal(v)
				if err != nil {
					t.Errorf(err.Error())
				}
				if !bytes.Equal(data, receiptsMap[i][k]) {
					// t.Fatalf("receipts error %v %v %v %v", i, k, string(data), string(receiptsMap[i+7][k]))
					t.Fatalf("receipts error %v %v %v %v %v", i, k, v, "test"+string(data), "control"+string(receiptsMap[i][k]))
				}
			}
		})
	}
	for i, block := range blockObject {
		t.Run(fmt.Sprintf("GetTransactionByBlockHashAndIndex %v", i), func(t *testing.T) {
			var h types.Hash
			json.Unmarshal(block["hash"], &h)
			for j := range transactionLists[i] {
				actual, _ := tx.GetTransactionByBlockHashAndIndex(context.Background(), h, hexutil.Uint64(j))
				if len(*actual) != len(transactionLists[i][j]) {
					t.Fatalf("length error GetTransactionByBlockHashAndIndex on blockHash %v, index %v", h, j)
				}
				for k, v := range *actual {
					data, err := json.Marshal(v)
					if err != nil {
						t.Errorf(err.Error())
					}
					if !bytes.Equal(data, transactionLists[i][j][k]) {
						t.Fatalf("error on blockHash%v, transaction%v, key%v", h, j, k)
					}
				}
			}
		})
		t.Run(fmt.Sprintf("GetTransactionByBlockNumberAndIndex %v", i), func(t *testing.T) {
			var n plugins.BlockNumber
			json.Unmarshal(block["number"], &n)
			for j := range transactionLists[i] {
				actual, _ := tx.GetTransactionByBlockNumberAndIndex(context.Background(), n, hexutil.Uint64(j))
				if len(*actual) != len(transactionLists[i][j]) {
					t.Fatalf("length error GetTransactionByBlockNumberAndIndex on blockNumber %v, index %v", n, j)
				}
				for k, v := range *actual {
					data, err := json.Marshal(v)
					if err != nil {
						t.Errorf(err.Error())
					}
					if !bytes.Equal(data, transactionLists[i][j][k]) {
						var x interface{}
						json.Unmarshal(transactionLists[i][j][k], &x)
						t.Fatalf("error on block%v, transaction%v, key%v", i, j, k)
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
			actual, _ := tx.GetTransactionCount(context.Background(), sender)
			if *actual != nonce {
				t.Fatalf("GetTransactionCountError %v %v", actual, nonce)
			}
		})
	}
}
