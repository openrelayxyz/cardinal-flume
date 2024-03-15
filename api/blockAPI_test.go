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
	"reflect"
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

func blockReceiptsTransform() (map[[2]interface{}][]map[string]json.RawMessage, error) {
	results := make(map[[2]interface{}][]map[string]json.RawMessage)
	k := [2]interface{}{} // hold a pair of block number and hashes
	unmodified, err := receiptsDecompress()

	if err != nil {
		return nil, err
	}

	var previousNum uint64

	for i, item := range unmodified {
		var blockNumber interface{}
		json.Unmarshal(item["blockNumber"], &blockNumber)

		var blockHash interface{}
		json.Unmarshal(item["blockHash"], &blockHash)

		if hexutil.MustDecodeUint64(blockNumber.(string)) > previousNum {
			k = [2]interface{}{blockNumber, blockHash}
			results[k] = []map[string]json.RawMessage{item}
			previousNum = hexutil.MustDecodeUint64(blockNumber.(string))
		} else if hexutil.MustDecodeUint64(blockNumber.(string)) == previousNum {
			results[k] = append(results[k], item)
		} else {
			return nil, errors.New(fmt.Sprintf("Expectations violated in blockReceiptsTransform on index %v", i))
		}
	}

	return results, nil
}

func isolateReceiptMap(arg interface{}, object map[[2]interface{}][]map[string]json.RawMessage) []map[string]json.RawMessage {
	num, ok := arg.(rpc.BlockNumber)
	if ok {
		for k, v := range object {
			if k[0].(rpc.BlockNumber) == num {
				return v
			}
		}
	}
	return nil
}

func TestReceiptFunc(t *testing.T) {
	r, err := blockReceiptsTransform()
	if err != nil {
		log.Error("error getting data", "err", err)
	}
	for k, v := range r {
		if k[0].(string) == "0xf4240" {
			log.Info("ok you got one", "len", len(v))
		}
	}
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

func getReceiptBlockNumber(receiptObject []map[string]json.RawMessage) []rpc.BlockNumber {
	uniqueBlockNumbers := make(map[rpc.BlockNumber]bool)
	result := []rpc.BlockNumber{}
	for _, block := range receiptObject {
		var num rpc.BlockNumber
		json.Unmarshal(block["blockNumber"], &num)
		_, exists := uniqueBlockNumbers[num]
		if !exists {
			result = append(result, num)
			uniqueBlockNumbers[num] = true
		}
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
	expectedResult, _ := hexutil.DecodeUint64("0xd59f95")
	test, err := b.BlockNumber(context.Background())
	if err != nil {
		t.Fatalf(err.Error())
	}
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
	for i, block := range blockNumbers {
		t.Run(fmt.Sprintf("GetBlockByNumber %v", i), func(t *testing.T) {
			actual, err := b.GetBlockByNumber(context.Background(), block, true)
			if err != nil {
				t.Fatal(err.Error())
			}
			for k, v := range *actual {
				if k == "withdrawals" {
					continue // withdrawals have their own test data and test below
				}
				if k == "withdrawalsRoot" {
					continue // withdrawalsRoot has not test and will require new test data
				}
				if k == "transactions" {
					txs := v.([]map[string]interface{})
					var blockTxs []map[string]json.RawMessage
					json.Unmarshal(blockObject[i]["transactions"], &blockTxs)
					for j, item := range txs {
						for key, value := range item {
							if key == "accessList" {
								control := item["accessList"]
								accessListRoutine(t, control, blockTxs[j]["accessList"], "GetBlockByNumber", block, j)
								continue
							}
							d, err := json.Marshal(value)
							if err != nil {
								t.Fatalf("transaction key marshalling error on block %v  tx index %v", i, j)
							}
							if !bytes.Equal(d, blockTxs[j][key]) {
								t.Fatalf("error in getBlockByNumber, transactions on block %v, , key %v, txn %v", block, key, j)
							}

						}
					}
				} else {
					data, err := json.Marshal(v)
					if err != nil {
						t.Errorf("nope %v", k)
					}
					if !bytes.Equal(data, blockObject[i][k]) {
						var generic interface{}
						json.Unmarshal(blockObject[i][k], &generic)
						t.Fatalf("value error GetBlockByNumber on block %v, index %v, key %v", block, i, k)
					}
				}
			}
		})

		t.Run("GetBlockTransactionCountByNumber", func(t *testing.T) {
			actual, err := b.GetBlockTransactionCountByNumber(context.Background(), block)
			if err != nil {
				t.Fatal(err.Error())
			}
			var txSlice []map[string]interface{}
			json.Unmarshal(blockObject[i]["transactions"], &txSlice)
			if *actual != hexutil.Uint64(len(txSlice)) {
				t.Fatalf("transaction count by block %v %v", actual, hexutil.Uint64(len(txSlice)))
			}
		})

		t.Run("GetUncleCountByBlockNumber", func(t *testing.T) {
			actual, err := b.GetUncleCountByBlockNumber(context.Background(), block)
			if err != nil {
				t.Fatal(err.Error())
			}
			var uncleSlice []types.Hash
			json.Unmarshal(blockObject[i]["uncles"], &uncleSlice)
			if *actual != hexutil.Uint64(len(uncleSlice)) {
				t.Fatalf("uncle count by block %v %v", actual, hexutil.Uint64(len(uncleSlice)))
			}
		})

	}

	receiptData, err := blockReceiptsTransform()
	if err != nil {
		log.Error(err.Error())
	}

	receiptObject, err := receiptsDecompress()
	if err != nil {
		log.Error(err.Error())
	}
	receiptBlockNumbers := getReceiptBlockNumber(receiptObject)
	for _, block := range receiptBlockNumbers {
		receiptList := isolateReceiptMap(block, receiptData)
		t.Run("GetBlockReceipts", func(t *testing.T) {
			blockNo := BlockNumberOrHashWithNumber(block)
			actual, err := b.GetBlockReceipts(context.Background(), blockNo)
			if err != nil {
				t.Fatal(err.Error())
			}

			for j, item := range actual {
				for k, v := range item {
					if k == "blockNumber" {
						log.Error("These are the types", "Type", reflect.TypeOf(v), "Second type", j, reflect.TypeOf(receiptList[j][k]))
						// if v != receiptList[j][k] {
						// 	t.Fatal("There was a problem")
						// }
					}
				}
			}

		})
	}

	blockHashes := getBlockHashes(blockObject)
	for i, hash := range blockHashes {
		t.Run(fmt.Sprintf("GetBlockByHash %v", i), func(t *testing.T) {
			actual, err := b.GetBlockByHash(context.Background(), hash, true)
			if err != nil {
				t.Fatal(err.Error())
			}
			for k, v := range *actual {
				if k == "withdrawals" {
					continue // withdrawals have their own test data and test below
				}
				if k == "withdrawalsRoot" {
					continue // withdrawalsRoot has no test and will require new test data
				}
				if k == "transactions" {
					txs := v.([]map[string]interface{})
					var blockTxs []map[string]json.RawMessage
					json.Unmarshal(blockObject[i]["transactions"], &blockTxs)
					for j, item := range txs {
						for key, value := range item {
							if key == "accessList" {
								control := item["accessList"]
								accessListRoutine(t, control, blockTxs[j]["accessList"], "GetBlockByHash", hash, j)
								continue
							}
							d, err := json.Marshal(value)
							if err != nil {
								t.Fatalf("transaction key marshalling error on block %v  tx index %v", i, j)
							}
							if !bytes.Equal(d, blockTxs[j][key]) {
								t.Fatalf("value error in getBlockByHash, transactions on block %v, , key %v, txn %v", hash, key, j)
							}

						}
					}
				} else {
					data, err := json.Marshal(v)
					if err != nil {
						t.Errorf("nope %v", k)
					}
					if !bytes.Equal(data, blockObject[i][k]) {
						t.Fatalf("value error GetBlockByHash on hash %v, index %v, key %v", hash, i, k)
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
				t.Fatalf("transaction count by hash %v %v", actual, hexutil.Uint64(len(txSlice)))
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
				t.Fatalf("uncle count by hash %v %v", actual, hexutil.Uint64(len(uncleSlice)))
			}
		})
	}
	withdrawalObject, err := withdrawalsDecompress()
	if err != nil {
		log.Error(err.Error())
	}
	for i, block := range blockNumbers[(len(blockNumbers) - 7):] {
		t.Run(fmt.Sprintf("GetBlockByNumber - withdrawls %v", i), func(t *testing.T) {
			actual, err := b.GetBlockByNumber(context.Background(), block, false)
			if err != nil {
				t.Fatalf("Error fetching block, withdrawals test on block %v with error %v", i, err.Error())
			}
			for k, v := range *actual {
				if k == "withdrawals" {
					wthdrls := v.([]map[string]interface{})
					for j, item := range wthdrls {
						for key, value := range item {
							d, err := json.Marshal(value)
							if err != nil {
								t.Fatalf("withdrawal key marshalling error on block %v", i)
							}
							if !bytes.Equal(d, withdrawalObject[i][j][key]) {
								t.Fatalf("value mismatch on withdrawals block %v, key %v", block, key)
							}

						}
					}
				}
			}
		})
	}

}
