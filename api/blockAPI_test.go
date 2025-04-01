package api

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"testing"

	"github.com/mattn/go-sqlite3"

	_ "net/http/pprof"

	log "github.com/inconshreveable/log15"
	ctypes "github.com/openrelayxyz/cardinal-evm/types"
	"github.com/openrelayxyz/cardinal-flume/config"
	"github.com/openrelayxyz/cardinal-flume/migrations"
	"github.com/openrelayxyz/cardinal-flume/plugins"
	rpc "github.com/openrelayxyz/cardinal-rpc"
	types "github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-types/hexutil"
)

var register sync.Once

func connectToDatabase(cfg *config.Config) (*sql.DB, bool, error) {

	register.Do(func() {
		sql.Register("sqlite3_hooked",
			&sqlite3.SQLiteDriver{
				ConnectHook: func(conn *sqlite3.SQLiteConn) error {
					for name, path := range cfg.Databases {
						conn.Exec(fmt.Sprintf("ATTACH DATABASE '%v' AS '%v'; PRAGMA %v.journal_mode = WAL ; PRAGMA %v.synchronous = OFF ;", path, name, name, name), nil)
					}
					return nil
				},
			})
	})

	logsdb, err := sql.Open("sqlite3_hooked", (":memory:?_sync=0&_journal_mode=WAL&_foreign_keys=off"))
	if err != nil {
		log.Error(err.Error())
	}

	_, hasLogs := cfg.Databases["logs"]
	if hasLogs {
		log.Info("has logs", "logs", cfg.Databases["logs"])
	}
	_, hasBlocks := cfg.Databases["blocks"]
	if hasBlocks {
		log.Info("has blocks", "blocks", cfg.Databases["blocks"])
	}
	_, hasTx := cfg.Databases["transactions"]
	if hasTx {
		log.Info("has transactions", "transactions", cfg.Databases["transactions"])
	}
	_, hasMempool := cfg.Databases["mempool"]
	if hasMempool {
		log.Info("has mempool", "mempool", cfg.Databases["mempool"])
	}

	if hasBlocks {
		if err := migrations.MigrateBlocks(logsdb, cfg.Chainid); err != nil {
			log.Error(err.Error())
		}
	}
	if hasTx {
		if err := migrations.MigrateTransactions(logsdb, cfg.Chainid); err != nil {
			log.Error(err.Error())
		}
	}
	if hasLogs {
		if err := migrations.MigrateLogs(logsdb, cfg.Chainid); err != nil {
			log.Error(err.Error())
		}
	}
	if hasMempool {
		if err := migrations.MigrateMempool(logsdb, cfg.Chainid); err != nil {
			log.Error(err.Error())
		}
	}

	return logsdb, hasMempool, nil
}

func blocksDecompress() ([]map[string]json.RawMessage, error) {
	file, _ := ioutil.ReadFile("../testing-resources/block_test_data.json.gz")
	r, err := gzip.NewReader(bytes.NewReader(file))
	if err != nil {
		return nil, err
	}
	raw, _ := ioutil.ReadAll(r)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, err
	}
	var blocksObject []map[string]json.RawMessage
	json.Unmarshal(raw, &blocksObject)
	return blocksObject, nil
}

func receiptsDecompress() ([]map[string]json.RawMessage, error) {
	file, _ := ioutil.ReadFile("../testing-resources/receipt_test_data.json.gz")
	r, err := gzip.NewReader(bytes.NewReader(file))
	if err != nil {
		return nil, err
	}
	raw, _ := ioutil.ReadAll(r)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, err
	}
	var receiptsObject []map[string]json.RawMessage
	json.Unmarshal(raw, &receiptsObject)
	return receiptsObject, nil
}

func blockReceiptsTransform() (map[rpc.BlockNumber][]map[string]json.RawMessage, map[types.Hash][]map[string]json.RawMessage, error) {
	numResults := make(map[rpc.BlockNumber][]map[string]json.RawMessage)
	hashResults := make(map[types.Hash][]map[string]json.RawMessage)

	unmodified, err := receiptsDecompress()
	if err != nil {
		return nil, nil, err
	}

	var previousNum rpc.BlockNumber

	for i, item := range unmodified {

		var blockNumber rpc.BlockNumber
		if err := blockNumber.UnmarshalJSON(item["blockNumber"]); err != nil {
			log.Error("Cannot unmarshal blockNumber blockReceiptsTransform", "index", i)
			return nil, nil, err
		}
		var blockHash types.Hash
		json.Unmarshal(item["blockHash"], &blockHash)

		if blockNumber > previousNum {
			numResults[blockNumber] = []map[string]json.RawMessage{item}
			hashResults[blockHash] = []map[string]json.RawMessage{item}
			previousNum = blockNumber
		} else if blockNumber == previousNum {
			numResults[blockNumber] = append(numResults[blockNumber], item)
			hashResults[blockHash] = append(hashResults[blockHash], item)
		} else {
			return nil, nil, errors.New(fmt.Sprintf("Expectations violated in blockReceiptsTransform on index %v", i))
		}
	}

	return numResults, hashResults, nil
}

func withdrawalsDecompress() ([][]map[string]json.RawMessage, error) {
	file, _ := ioutil.ReadFile("../testing-resources/withdrawal_test_data.json.gz")
	r, err := gzip.NewReader(bytes.NewReader(file))
	if err != nil {
		return nil, err
	}
	raw, _ := ioutil.ReadAll(r)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, err
	}
	var withdrawalsObject [][]map[string]json.RawMessage
	json.Unmarshal(raw, &withdrawalsObject)
	return withdrawalsObject, nil
}

func getBlockNumbers(jsonBlockObject []map[string]json.RawMessage) []rpc.BlockNumber {
	result := []rpc.BlockNumber{}
	for _, block := range jsonBlockObject {
		var x rpc.BlockNumber
		json.Unmarshal(block["number"], &x)
		result = append(result, x)
	}
	return result
}

func getBlockHashes(jsonBlockObject []map[string]json.RawMessage) []types.Hash {
	result := []types.Hash{}
	for _, block := range jsonBlockObject {
		var x types.Hash
		json.Unmarshal(block["hash"], &x)
		result = append(result, x)
	}
	return result
}

func accessListRoutine(t *testing.T, test interface{}, control json.RawMessage, method string, delineator, txIndex interface{}) {
	testList := test.(*ctypes.AccessList)
	if len(*testList) > 0 {
		var controlList *ctypes.AccessList
		json.Unmarshal(control, &controlList)
		var iterable ctypes.AccessList
		iterable = *controlList
		for i, testItem := range *testList {
			if testItem.Address != iterable[i].Address {
				t.Fatalf("address value error in %v accessListRoutine block %v, txn %v, accessTuple %v, test %v, control %v", method, delineator, txIndex, i, testItem.Address, iterable[i].Address)
			}
			if len(testItem.StorageKeys) > 0 {
				for j, key := range testItem.StorageKeys {
					if key != iterable[i].StorageKeys[j] {
						t.Fatalf("storageKey value error in %v accessListRoutine block %v, txn %v, accessTuple %v, key %v, test %v, control %v", method, delineator, txIndex, i, j, testItem.Address, iterable[i].Address)
					}
				}
			}
		}
	}
}

func TestBlockNumber(t *testing.T) {
	cfg, err := config.LoadConfig("../testing-resources/api_test_config.yml")
	if err != nil {
		t.Fatal("Error parsing config TestBlockNumber", "err", err.Error())
	}
	db, _, err := connectToDatabase(cfg)
	if err != nil {
		t.Fatal(err.Error())
	}
	for _, path := range cfg.Databases {
		defer os.Remove(path + "-wal")
		defer os.Remove(path + "-shm")
	}
	defer db.Close()
	pl, _ := plugins.NewPluginLoader(cfg)
	b := NewBlockAPI(db, 1, pl, cfg)
	expectedResult, _ := hexutil.DecodeUint64("0x12ab5da")
	test := b.BlockNumber(context.Background())
	if test != hexutil.Uint64(expectedResult) {
		t.Fatalf("BlockNumber() result not accurate")
	}
}

func TestBlockAPI(t *testing.T) {
	cfg, err := config.LoadConfig("../testing-resources/api_test_config.yml")
	if err != nil {
		t.Fatal("Error parsing config TestBlockApi", "err", err.Error())
	}
	db, _, err := connectToDatabase(cfg)
	if err != nil {
		t.Fatal(err.Error())
	}
	for _, path := range cfg.Databases {
		defer os.Remove(path + "-wal")
		defer os.Remove(path + "-shm")
	}
	defer db.Close()
	pl, _ := plugins.NewPluginLoader(cfg)
	b := NewBlockAPI(db, 1, pl, cfg)
	blockObject, _ := blocksDecompress()
	blockNumbers := getBlockNumbers(blockObject)
	receiptDataNumber, receiptDataHash, err := blockReceiptsTransform()
	if err != nil {
		log.Error("Error returned from blockReceiptsTransform", "err", err)
	}
	for i, block := range blockObject {
		blockNumber := blockNumbers[i]
		t.Run(fmt.Sprintf("GetBlockByNumber %v", i), func(t *testing.T) {
			actualBlock, err := b.GetBlockByNumber(context.Background(), blockNumber, true); if err != nil {
				t.Fatalf("failed to getBlockByNumber, block%v, err:%v", block, err.Error())
			}
			for key, expectedValue := range block {
				if key == "withdrawals" || key == "withdrawalsRoot" || key == "chainId" {
					continue // withdrawals have their own test data and test below, withdrawalsRoot has no test and will require new test data
				}
				if key == "transactions" {
					var expectedTxs []map[string]json.RawMessage
					if err := json.Unmarshal(expectedValue, &expectedTxs); err != nil {
						t.Fatalf("failed to unmarshal expected transactions: %v", err)
					}
					actualTxs, _ := (*actualBlock)["transactions"].([]map[string]interface{})
					for j, expectedTx := range expectedTxs {
						actualTx := actualTxs[j]
						for k, v := range expectedTx {
							if k == "chainId" {
								continue
							}
							if k == "accessList" {
								accessListRoutine(t, actualTx[k], v, "GetBlockByNumber", blockNumber, j)
								continue
							}
							d, err := json.Marshal(actualTx[k])
							if err != nil {
								t.Fatalf("transaction key marshalling error on block %v  tx index %v", i, j)
							}

							if !bytes.Equal(d, v) {
								t.Fatalf("error in getBlockByNumber, transactions on block %v, transaction %v, key %v,\n actual:%v \nexpected:%v",  i, j, k, string(d), string(v))
							}

						}
					}
				} else {
					actualValue := (*actualBlock)[key]
					data, err := json.Marshal(actualValue)
					if err != nil {
						t.Fatalf("failed to marshal actual block field:%v on block:%v, err:%v", key,i,err)
					}
					if !bytes.Equal(data, expectedValue) {
						t.Fatalf("error on getBlockByNumber, \nindex %v, key %v; \n actual result: %v, \n expected result: %v", i, key, string(data), string(expectedValue))
					}
				}
			}
		})

		t.Run("GetBlockTransactionCountByNumber", func(t *testing.T) {
			actual, err := b.GetBlockTransactionCountByNumber(context.Background(), blockNumber)
			if err != nil {
				t.Fatal(err.Error())
			}
			var txSlice []map[string]interface{}
			json.Unmarshal(blockObject[i]["transactions"], &txSlice)
			if *actual != hexutil.Uint64(len(txSlice)) {
				t.Fatalf("transaction count mismatch for block %v, \nactual:%v,\nexpected%v", i, actual, hexutil.Uint64(len(txSlice)))
			}
		})

		t.Run("GetUncleCountByBlockNumber", func(t *testing.T) {
			actual, err := b.GetUncleCountByBlockNumber(context.Background(), blockNumber)
			if err != nil {
				t.Fatal(err.Error())
			}
			var uncleSlice []types.Hash
			json.Unmarshal(blockObject[i]["uncles"], &uncleSlice)
			if *actual != hexutil.Uint64(len(uncleSlice)) {
				t.Fatalf("uncle count mismatch for block %v, \nactual:%v,\nexpected%v", i, actual, hexutil.Uint64(len(uncleSlice)))
			}
		})

		blockNo := BlockNumberOrHashWithNumber(blockNumber)
		t.Run("GetBlockReceipts", func(t *testing.T) {
			actualReceipts, err := b.GetBlockReceipts(context.Background(), blockNo); if err != nil {
				t.Fatal(err.Error())
			}
			expectedReceipts := receiptDataNumber[*blockNo.BlockNumber]
			for i, expectedReceipt := range expectedReceipts {
				for key, expectedValue := range expectedReceipt {
					keysToCheck := []string{"blockNumber", "blockHash", "transactionIndex"}
					for _, k := range keysToCheck {
						if key == k {
							if data, err := json.Marshal(actualReceipts[i][key]); err == nil {
								if !bytes.Equal(data, expectedValue) {
									t.Fatalf("error on GetBlockReceipts blockno:%v, key %v; \nactual result: %v, \nexpected result: %v", *blockNo.BlockNumber, key, string(data), string(expectedValue))
								}
							}
						}
					}
				}
			}

		})
	}

	blockHashes := getBlockHashes(blockObject)
	for i, block := range blockObject {
		hash := blockHashes[i]
		t.Run(fmt.Sprintf("GetBlockByHash %v", i), func(t *testing.T) {
			actualBlock, err := b.GetBlockByHash(context.Background(), hash, true)
			if err != nil {
				t.Fatal(err.Error())
			}
			for key, expectedValue := range block {
				if key == "withdrawals" || key == "withdrawalsRoot" {
					continue // withdrawals have their own test data and test below, withdrawalsRoot has no test and will require new test data
				}
				if key == "transactions" {
					var expectedTxs []map[string]json.RawMessage
					if err := json.Unmarshal(expectedValue, &expectedTxs); err != nil {
						t.Fatalf("failed to unmarshal expected transactions: %v", err)
					}
					actualTxs, _ := (*actualBlock)["transactions"].([]map[string]interface{})
					for j, expectedTx := range expectedTxs {
						actualTx := actualTxs[j]
						for k, v := range expectedTx {
							if k == "chainId" {
								continue
							}
							if k == "accessList" {
								accessListRoutine(t, actualTx[k], v, "GetBlockByHash", hash, j)
								continue
							}
							d, err := json.Marshal(actualTx[k])
							if err != nil {
								t.Fatalf("transaction key marshalling error on block %v  tx index %v", i, j)
							}
							if !bytes.Equal(d, v) {
								t.Fatalf("error in getBlockByHash, transactions on block %v, transaction %v, key %v,\n actual:%v \nexpected:%v", i, j, k, string(d), string(v))
							}

						}
					}
				} else {
					actualValue := (*actualBlock)[key]
					data, err := json.Marshal(actualValue)
					if err != nil {
						t.Fatalf("failed to marshal actual block field:%v on block:%v, err:%v", key,i,err)
					}
					if !bytes.Equal(data, expectedValue) {
						t.Fatalf("error on getBlockByHash, \nindex %v, key %v; \n actual result: %v, \n expected result: %v", i, key, string(data), string(expectedValue))
					}
				}
			}
		})
		t.Run("GetBlockTransactionCountByHash", func(t *testing.T) {
			actual, err := b.GetBlockTransactionCountByHash(context.Background(), hash)
			if err != nil {
				t.Fatal(err.Error())
			}
			var txSlice []map[string]interface{}
			json.Unmarshal(blockObject[i]["transactions"], &txSlice)
			if *actual != hexutil.Uint64(len(txSlice)) {
				t.Fatalf("transaction count mismatch for block %v, \nactual:%v,\nexpected%v", i, actual, hexutil.Uint64(len(txSlice)))
			}
		})
		t.Run("GetUncleCountByBlockHash", func(t *testing.T) {
			actual, err := b.GetUncleCountByBlockHash(context.Background(), hash)
			if err != nil {
				t.Fatal(err.Error())
			}
			var uncleSlice []types.Hash
			json.Unmarshal(blockObject[i]["uncles"], &uncleSlice)
			if *actual != hexutil.Uint64(len(uncleSlice)) {
				t.Fatalf("uncle count mismatch for block %v, \nactual:%v,\nexpected%v", i, actual, hexutil.Uint64(len(uncleSlice)))
			}
		})
		blockHash := BlockNumberOrHashWithHash(hash, false)
		t.Run("GetBlockReceipts", func(t *testing.T) {
			actualReceipts, err := b.GetBlockReceipts(context.Background(), blockHash); if err != nil {
				t.Fatal(err.Error())
			}
			expectedReceipts := receiptDataHash[*blockHash.BlockHash]
			for i, expectedReceipt := range expectedReceipts {
				for key, expectedValue := range expectedReceipt {
					keysToCheck := []string{"blockNumber", "blockHash", "transactionIndex"}
					for _, k := range keysToCheck{
						if key == k{
							if data, err := json.Marshal(actualReceipts[i][key]); err == nil {
								if !bytes.Equal(data, expectedValue){
									t.Fatalf("error on GetBlockReceipts hash:%v, key %v; \nactual result: %v, \nexpected result: %v", *blockHash.BlockHash, key, string(data), string(expectedValue))
								}
							}
						}
					}
				}
			}

		})
	}
	withdrawalObject, err := withdrawalsDecompress()
	if err != nil {
		log.Error(err.Error())
	}
	for i, block := range withdrawalObject[len(withdrawalObject)-10] {
		blockNo := blockNumbers[i]
		t.Run(fmt.Sprintf("GetBlockByNumber - withdrawls %v", i), func(t *testing.T) {
			actualBlock, err := b.GetBlockByNumber(context.Background(), blockNo, false); if err != nil {
				t.Fatalf("Error fetching block, withdrawals test on block %v with error %v", i, err.Error())
			}
			for key, expectedValue := range block {
				if key == "withdrawals" {
					var expectedWithdrawals []map[string]json.RawMessage
					if err := json.Unmarshal(expectedValue, &expectedWithdrawals); err != nil {
						t.Fatalf("failed to unmarshal expected withdrawals: %v", err)
					}
					actualWithdrawals, _ := (*actualBlock)["withdrawals"].([]map[string]interface{})
					for j, expectedWithdrawal := range expectedWithdrawals {
						actualWithdrawal := actualWithdrawals[j]
						for k, v := range expectedWithdrawal {
							d, err := json.Marshal(actualWithdrawal[k])
							if err != nil {
								t.Fatalf("withdrawal key marshalling error on block %v, index %v", i, j)
							}
							if !bytes.Equal(d, v) {
								t.Fatalf("error in getBlockByNumber -withdrawals, block:%v, withdrawal:%v, key:%v,\n actual:%v \nexpected:%v",  i, j, k, string(d), string(v))
							}

						}
					}
				}
			}
		})
	}

}
