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

func getHashReceipts(jsonBlockObject, jsonReceiptObject []map[string]json.RawMessage) map[types.Hash][]map[string]json.RawMessage {
	bkHashes := getBlockHashes(jsonBlockObject)
	result := map[types.Hash][]map[string]json.RawMessage{}
	for _, hash := range bkHashes {
		receipts := []map[string]json.RawMessage{}
		for _, receipt := range jsonReceiptObject {
			var h types.Hash
			json.Unmarshal(receipt["blockHash"], &h)
			if hash == h {
				receipts = append(receipts, receipt)
				result[hash] = receipts
			}
		}
	}
	return result
}

func getBlockReceipts(jsonBlockObject, jsonReceiptObject []map[string]json.RawMessage) map[rpc.BlockNumber][]map[string]json.RawMessage {
	bkNumbers := getBlockNumbers(jsonBlockObject)
	result := map[rpc.BlockNumber][]map[string]json.RawMessage{}
	for _, number := range bkNumbers {
		receipts := []map[string]json.RawMessage{}
		for _, receipt := range jsonReceiptObject {
			var n rpc.BlockNumber
			json.Unmarshal(receipt["blockNumber"], &n)
			if number == n {
				receipts = append(receipts, receipt)
				result[number] = receipts
			}
		}
	}
	return result
}

func getTransactionList(jsonBlockObject []map[string]json.RawMessage, address, key string) []map[string]json.RawMessage {
	results := []map[string]json.RawMessage{}
	transactions := getTransactionsForTesting(jsonBlockObject)
	addr, _ := json.Marshal(address)
	for _, tx := range transactions {
		if bytes.Equal(tx[key], addr) {
			results = append(results, tx)
		}
	}
	return results
}

func getParticipantTransactionList(jsonBlockObject []map[string]json.RawMessage, address, keyOne, keyTwo string) []map[string]json.RawMessage {
	results := []map[string]json.RawMessage{}
	transactions := getTransactionsForTesting(jsonBlockObject)
	addr, _ := json.Marshal(address)
	for _, tx := range transactions {
		if bytes.Equal(tx[keyOne], addr) || bytes.Equal(tx[keyTwo], addr) {
			results = append(results, tx)
		}
	}
	return results
}

func getSenderReceiptList(jsonReceiptObject []map[string]json.RawMessage) []map[string]json.RawMessage {
	results := []map[string]json.RawMessage{}
	addr, _ := json.Marshal("0x52bc44d5378309ee2abf1539bf71de1b7d7be3b5")
	for _, receipt := range jsonReceiptObject {
		if bytes.Equal(receipt["from"], addr) {
			results = append(results, receipt)
		}
	}
	return results
}

func getReceiptList(jsonReceiptObject []map[string]json.RawMessage, address, key string) []map[string]json.RawMessage {
	results := []map[string]json.RawMessage{}
	addr, _ := json.Marshal(address)
	for _, receipt := range jsonReceiptObject {
		if bytes.Equal(receipt[key], addr) {
			results = append(results, receipt)
		}
	}
	return results
}

func getParticipantReceiptList(jsonReceiptObject []map[string]json.RawMessage, address, keyOne, keyTwo string) []map[string]json.RawMessage {
	results := []map[string]json.RawMessage{}
	addr, _ := json.Marshal(address)
	for _, receipt := range jsonReceiptObject {
		if bytes.Equal(receipt[keyOne], addr) || bytes.Equal(receipt[keyTwo], addr) {
			results = append(results, receipt)
		}
	}
	return results
}

func getHashblocks(jsonBlockObject []map[string]json.RawMessage) (map[types.Hash]map[string]json.RawMessage, error) {
	result := make(map[types.Hash]map[string]json.RawMessage)

	for i, block := range jsonBlockObject {
		txns := []map[string]interface{}{}
		if err := json.Unmarshal(block["transactions"], &txns); err != nil {
			log.Error("Cannot Unmarshal transactions getHashBlocks", "index", i)
			return nil, err
		}
		if len(txns) > 0 {
			var txHash types.Hash
			txHash = types.HexToHash(txns[0]["hash"].(string))
			result[txHash] = block
		}

	}
	return result, nil
}

var (
	senderAddr    = "0x52bc44d5378309ee2abf1539bf71de1b7d7be3b5"
	recipientAddr = "0x7a250d5630b4cf539739df2c5dacb4c659f2488d"
	genericAddr   = "0x3cd751e6b0078be393132286c442345e5dc49699"
)

func vlist(ms []map[string]json.RawMessage, key string) []string {
	result := make([]string, 0, len(ms))
	for _, m := range ms {
		if v, ok := m[key]; ok {
			result = append(result, string(v))
		}
	}
	return result
}
func vlisti(ms []map[string]interface{}, key string) []string {
	result := make([]string, 0, len(ms))
	for _, m := range ms {
		if v, ok := m[key]; ok {
			result = append(result, fmt.Sprintf("%v", v))
		}
	}
	return result
}

func (ms sortTxMap) TransactionIndexes() []hexutil.Uint64 {
	result := make([]hexutil.Uint64, 0, len(ms))
	for _, m := range ms {
		if bni, ok := m["transactionIndex"]; ok {
			if bn, ok := bni.(hexutil.Uint64); ok {
				result = append(result, bn)
			}
		}
	}
	return result
}

var mockContext *rpc.CallContext = rpc.NewContext(context.Background())

func TestFlumeAPI(t *testing.T) {
	cfg, err := config.LoadConfig("../testing-resources/api_test_config.yml")
	if err != nil {
		t.Fatal("Error parsing config TestFlumeAPI", "err", err.Error())
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
	f := NewFlumeAPI(db, 1, pl, cfg, mempool)

	blockObject, _ := blocksDecompress()
	receiptObject, _ := receiptsDecompress()

	bkHashes := getBlockHashes(blockObject)
	bkNumbers := getBlockNumbers(blockObject)

	receiptsByHash := getHashReceipts(blockObject, receiptObject)
	receiptsByBlock := getBlockReceipts(blockObject, receiptObject)

	for i, hash := range bkHashes {
		t.Run(fmt.Sprintf("GetTransactionReceiptsByBlockHash %v", i), func(t *testing.T) {
			actualReceipts, err := f.GetTransactionReceiptsByBlockHash(context.Background(), hash); if err != nil{
				t.Fatalf("failed to get transaction receipts for block hash %v: %v", hash, err.Error())
			}
			expectedReceipts := receiptsByHash[hash] 
			for j, expectedReceipt := range expectedReceipts {
				// TODO: eip 4844 (both the comment and the nested if below)
				// if len(receipt) != len(receiptsByHash[hash][j]) {
				// 	t.Fatalf("length error GetTransactionReceiptsByBlockHash on hash %v, receipt %v", hash, j)
				// }
				for key, expectedValue := range expectedReceipt {
					if key == "blobGasPrice" || key == "root" {
						continue
					}
					actualValue := actualReceipts[j][key]
					data, err := json.Marshal(actualValue)
						if err != nil {
							t.Errorf(err.Error())
						}
					if !bytes.Equal(data, expectedValue) { 
						if key == "timestamp" && actualValue.(*hexutil.Big).String() == hexutil.EncodeUint64(timeStamps[i]) {
							continue
						} else {
							t.Fatalf("error on getTransactionReceiptsByBlockHash hash %v \n,receipt %v, key %v;\napi result: %v,\n expected result:%v,\n", hash, j, key, string(data), string(expectedValue))
						}
					}
				}
			}
		})
	}

	for i, number := range bkNumbers {
		t.Run(fmt.Sprintf("GetTransactionReceiptsByBlockNumber %v", i), func(t *testing.T) {
			actualReceipts, err := f.GetTransactionReceiptsByBlockNumber(context.Background(), number); if err != nil{
				t.Fatalf("failed to get transaction receipts for block %v: %v", number, err.Error())
			}
			expectedReceipts := receiptsByBlock[number]
			for j, expectedReceipt := range expectedReceipts {
				// TODO: eip 4844 (both the comment and the nested if below)
				// if len(receipt) != len(receiptsByBlock[number][j]) {
				// 	t.Fatalf("length error GetTransactionReceiptsByBlockNumber on number %v, receipt %v", number, j)
				// }
				for key, expectedValue := range expectedReceipt { 
					if key == "blobGasPrice" || key == "root" {
						continue
					}
					actualValue := actualReceipts[j][key]
					data, err := json.Marshal(actualValue)
						if err != nil {
							t.Errorf(err.Error())
						}
						if !bytes.Equal(data, expectedValue) {
							if key == "timestamp" && actualValue.(*hexutil.Big).String() == hexutil.EncodeUint64(timeStamps[i]) {
								continue
							} else {
								t.Fatalf("error on getTransactionReceiptsByBlockNumber, \n index %v, key %v; \n actual result: %v, \n expected result: %v, \n ", i, key, string(data), string(expectedValue))
							}
						}
				}
			}
		})
	}

	blockhashesData, _ := getHashblocks(blockObject)
	for txhash := range blockhashesData {
		t.Run("GetBlockByTransactionHash", func(t *testing.T) {
			actualBlock, err := f.GetBlockByTransactionHash(context.Background(), txhash)
			if err != nil {
				t.Fatalf("failed to get block, txhash %v: %v", txhash, err.Error())
			}

			expectedBlock := blockhashesData[txhash]
			for key, expectedValue := range expectedBlock {
				if key == "blockHash" {
					if data, err := json.Marshal(*actualBlock); err == nil {
						if !bytes.Equal(data, expectedValue) {
							t.Fatalf("Error in getBlockByTransactionHash, mismatch on 'blockHash', \napi result: %v,\nexpected result: %v", string(data), expectedValue)
						}
					}
				}
				if key == "blockNumber" {
					if data, err := json.Marshal(*actualBlock); err == nil {
						if !bytes.Equal(data, expectedValue) {
							t.Fatalf("Error in getBlockByTransactionHash, mismatch on 'blockNumber',\napi result:%v,\nexpected result %v", string(data), expectedValue)
						}
					}
				}
			}
		})
	}

	sender := common.HexToAddress(senderAddr)
	t.Run(("GetTransactionsBySender"), func(t *testing.T) {
		actualTxs, err := f.GetTransactionsBySender(mockContext, sender, nil); if err != nil{
			t.Fatalf("failed to getTransactionsBySender, address%v, err:%v", sender, err.Error())
		}
		expectedTxs := getTransactionList(blockObject, senderAddr, "from")
		if len(expectedTxs) != 47 {
			t.Fatalf("sender transactions list of incorrect length expected 47 got %v", len(expectedTxs))
		}
		if len(actualTxs.Items) != len(expectedTxs) {
			t.Fatalf("length error getTransactionsBySender on address %v", sender)
		}
		for i, expectedTx := range expectedTxs{
			for key, expectedValue := range expectedTx {
			   actualValue := actualTxs.Items[i][key]
				data, err := json.Marshal(actualValue)
				if err != nil {
					t.Errorf(err.Error())
				}
				if !bytes.Equal(data, expectedValue) {
					if key == "timestamp" || key == "chainId" {
						continue
					} else {
						t.Fatalf("error on getTransactionsBySender, \n index %v, key %v; \n actual result: %v, \n expected result: %v", i, key, string(data), string(expectedValue))
					}
				}
			}
		}
	})

	t.Run("GetTransactionReceiptsBySender", func(t *testing.T) {
		actualReceipts, err := f.GetTransactionReceiptsBySender(mockContext, sender, nil); if err!= nil{
			t.Fatalf("failed to getTransactionReceiptsBySender, address%v, err:%v", sender, err.Error())
		}
		expectedReceipts := getReceiptList(receiptObject, senderAddr, "from")
		if len(expectedReceipts) != 47 {
			t.Fatalf("sender transactions list of incorrect length expected 47 got %v", len(expectedReceipts))
		}
		if len(actualReceipts.Items) != len(expectedReceipts) {
			t.Fatalf("length error getTransactionReceiptsBySender on address %v", sender)
		}
		for i, expectedReceipt := range expectedReceipts {
			// TODO: eip 4844 (both the comment and the nested if below)
			// if len(tx) != len(senderReceipts[i]) {
			// 	t.Fatalf("length error getTransactionReceiptsBySender on address %v, reciept %v", sender, i)
			// }
			for key, expectedValue := range expectedReceipt {
				actualValue := actualReceipts.Items[i][key]
				data, err := json.Marshal(actualValue)
				if err != nil {
					t.Errorf(err.Error())
				}
				if !bytes.Equal(data, expectedValue) {
					if key == "timestamp" || key == "root" {
						continue
					} else {
						t.Fatalf("error on getTransactionsBySender, \n index %v, key %v; \n actual result: %v, \n expected result: %v", i, key, string(data), string(expectedValue))
					}
				}
			}
		}
	})
	
	recipient := common.HexToAddress(recipientAddr)
	t.Run("GetTransactionsByRecipient", func(t *testing.T) {
		actualTxs, err := f.GetTransactionsByRecipient(mockContext, recipient, nil); if err!=nil {
			t.Fatalf("failed to getTransactionsByRecipient, address%v, err:%v", recipient, err.Error())
		}
		expectedTxs := getTransactionList(blockObject, recipientAddr, "to")
		if len(expectedTxs) != 143 {
			t.Fatalf("recipient transactions list of incorrect length expected 143 got %v", len(expectedTxs))
		}
		if len(actualTxs.Items) != len(expectedTxs) {
			t.Fatalf("getTransactionsByRecipient result of incorrect length expected %v got %v", len(actualTxs.Items), len(expectedTxs))
		}
		for i, expectedTx := range expectedTxs {
			for key, expectedValue := range expectedTx{
				actualValue := actualTxs.Items[i][key]
				data, err := json.Marshal(actualValue)
				if err != nil {
					t.Errorf(err.Error())
				}
				if !bytes.Equal(data, expectedValue) {
					if key == "timestamp" || key == "chainId" {
						continue
					} else {
						t.Fatalf("error on getTransactionsByRecipient, \n index %v, key %v; \n actual result: %v, \n expected result: %v, \n", i, key, string(data), string(expectedValue))
					}
				}
			}
		}
	})

	t.Run("GetTransactionsReceiptsByRecipient", func(t *testing.T) {
		actualReceipts, err := f.GetTransactionReceiptsByRecipient(mockContext, recipient, nil); if err != nil{
			t.Fatalf("failed to getTransactionsReceiptsByRecipient, address%v, err:%v", recipient, err.Error())
		}
		expectedReceipts := getReceiptList(receiptObject, recipientAddr, "to")
		if len(expectedReceipts) != 143 {
			t.Fatalf("recipient transactions list of incorrect length expected 143 got %v", len(expectedReceipts))
		}
		if len(actualReceipts.Items) != len(expectedReceipts) {
			t.Fatalf("getTransactionReceiptsByRecipient result of incorrect length expected %v got %v", len(actualReceipts.Items), len(expectedReceipts))
		}
		for i, expectedReceipt := range expectedReceipts {
			// TODO: eip 4844 (both the comment and the nested if below)
			// if len(tx) != len(recipientReceipts[i]) {
			// 	t.Fatalf("length error getTransactionReceiptsByRecipient on address %v, reciept %v", recipient, i)
			// }
			for key, expectedValue := range expectedReceipt {
				actualValue := actualReceipts.Items[i][key]
				data, err := json.Marshal(actualValue)
				if err != nil {
					t.Errorf(err.Error())
				}
				if !bytes.Equal(data, expectedValue) {
					if key == "timestamp" {
						continue
					} else {
						t.Fatalf("error on getTransactionReceiptsByRecipient, \n index %v, key %v; \n actual result: %v, \n expected result: %v, \n ", i, key, string(data), string(expectedValue))
					}
				}
			}
		}
	})
	participant := common.HexToAddress(genericAddr)
	t.Run("GetTransactionsByParticipant", func(t *testing.T) {
		actualTxs, err := f.GetTransactionsByParticipant(mockContext, participant, nil); if err != nil{
			t.Fatalf("failed to getTransactionsByParticipant, address%v, err:%v", participant, err.Error())
		}
		expectedTxs := getParticipantTransactionList(blockObject, genericAddr, "to", "from");
		if len(actualTxs.Items) != len(expectedTxs) {
			t.Fatalf("getTransactionsByParticipant result of incorrect length expected %v got %v", len(actualTxs.Items), len(expectedTxs))
		}
		for i, expectedTx := range expectedTxs {
			if len(expectedTx) + 1 != len(actualTxs.Items[i]) {
				t.Fatalf("length error getTransactionsByParticipant on address %v, tx %v", participant, i)
			}
			for key, expectedValue := range expectedTx {
				actualValue := actualTxs.Items[i][key]
				data, err := json.Marshal(actualValue)
				if err != nil {
					t.Errorf(err.Error())
				}
				if !bytes.Equal(data,expectedValue) {
					if key == "timestamp" {
						continue
					} else {
						t.Fatalf("error on getTransactionsByParticipant, \n index %v, key %v; \n actual result: %v, \n expected result: %v, \n ", i, key, string(data), string(expectedValue))
					}
				}
			}
		}
	})

	t.Run("GetTransactionsReceiptsByParticipant", func(t *testing.T) {
		actualReceipts, err := f.GetTransactionReceiptsByParticipant(mockContext, participant, nil); if err!=nil{
			t.Fatalf("failed to getTransactionsReceiptsByParticipant, address%v, err:%v", participant, err.Error())
		}
		expectedReceipts := getParticipantReceiptList(receiptObject, genericAddr, "to", "from")
		if len(actualReceipts.Items) != len(expectedReceipts) {
			t.Fatalf("getTransactionReceiptsByParticipant result of incorrect length expected %v got %v", len(actualReceipts.Items), len(expectedReceipts))
		}
		for i, expectedReceipt := range expectedReceipts {
			// TODO: eip 4844 (both the comment and the nested if below)
			// if len(tx) != len(participantReceipts[i]) {
			// 	t.Fatalf("length error getTransactionReceiptsByParticipant on address %v, reciept %v", participant, i)
			// }
			for key, expectedValue := range expectedReceipt {
				actualValue := actualReceipts.Items[i][key]
				data, err := json.Marshal(actualValue)
				if err != nil {
					t.Errorf(err.Error())
				}
				if !bytes.Equal(data, expectedValue) {
					if key == "timestamp" {
						continue
					} else {
						t.Fatalf("error on getTransactionReceiptsByParticipant, \n index %v, key %v; \n actual result: %v, \n expected result: %v, \n ", i, key, string(data), string(expectedValue))
					}
				}
			}
		}
	})
}

var timeStamps = []uint64{0, 1438269988, 1455404053, 1463003133, 1470173578, 1477324790, 1484475035, 1499633567, 1509953783, 1532118564, 1554358137, 1574706444,
	1576239700, 1581934143, 1588598533, 1601957824, 1615234816, 1618482942, 1621898262, 1628632419, 1635345781, 1642114795, 1642114800, 1642114824, 1642114825, 1642114850,
	1642114852, 1642114865, 1642114881, 1642114895, 1642114917, 1642114924, 1642114928, 1642114931, 1642114961, 1642114971, 1642114982, 1642114988, 1642115010, 1642115039,
	1642115047, 1642115052, 1642115064, 1642115064, 1712159807, 1712159819, 1712159831}

// var timeStamps = []string{"0", "1438269988", "1455404053", "1463003133", "1470173578", "1477324790", "1484475035", "1499633567", "1509953783", "1532118564", "1554358137",
// "1574706444", "1576239700", "1581934143", "1588598533", "1601957824", "1615234816", "1618482942", "1621898262", "1628632419", "1635345781", "1642114795", "1642114800",
// "1642114824", "1642114825", "1642114850", "1642114852", "1642114865", "1642114881", "1642114895", "1642114917", "1642114924", "1642114928", "1642114931", "1642114961",
// "1642114971", "1642114982", "1642114988", "1642115010", "1642115039", "1642115047", "1642115052", "1642115064"}
