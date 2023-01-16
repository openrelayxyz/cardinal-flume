package api

import (
	"context"
	"testing"
	"os"

	// log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-evm/common"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-types/hexutil"

	"github.com/openrelayxyz/cardinal-flume/config"
	"github.com/openrelayxyz/cardinal-flume/heavy"
	"github.com/openrelayxyz/cardinal-flume/plugins"
)

func TestCallHeavy(t *testing.T) {
	cfg, err := config.LoadConfig("../testing-resources/heavy_test_config.yml")
	if err != nil {
		t.Fatal("Error parsing config TestCallHeavy", "err", err.Error())
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
	cfg.EarliestBlock = 1
	pl, _ := plugins.NewPluginLoader(cfg)

	b := NewBlockAPI(db, 1, pl, cfg)

	testBlockNumber := plugins.BlockNumber(0)

	testHash := types.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000")

	testAddress := common.HexToAddress("0x0000000000000000000000000000000000000000")

	testUint64 := hexutil.Uint64(1)

	_, err = b.GetBlockByNumber(context.Background(), testBlockNumber, true)
	if err == nil {
		t.Fatal("GetBlockByNumber did not return expected error, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Method != "eth_getBlockByNumber" {
		t.Fatal("GetBlockByNumber did not return expected method name, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[0].(hexutil.Uint64) != hexutil.Uint64(testBlockNumber) {
		t.Fatal("GetBlockByNumber did not return expected parameter blockNumber, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[1].(bool) != true {
		t.Fatal("GetBlockByNumber did not return expected parameter bool, heavy test", "err", err.Error())
	}

	_, err = b.GetBlockByHash(context.Background(), testHash, true)
	if err == nil {
		t.Fatal("GetBlockByHash did not return expected error, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Method != "eth_getBlockByHash" {
		t.Fatal("GetBlockByHash did not return expected method name, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[0].(types.Hash) != testHash {
		t.Fatal("GetBlockByHash did not return expected parameter hash, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[1].(bool) != true {
		t.Fatal("GetBlockByHash did not return expected parameter bool, heavy test", "err", err.Error())
	}

	_, err = b.GetBlockTransactionCountByNumber(context.Background(), testBlockNumber)
	if err == nil {
		t.Fatal("GetBlockTransactionCountByNumber did not return expected error, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Method != "eth_getBlockTransactionCountByNumber" {
		t.Fatal("GetBlockTransactionCountByNumber did not return expected method name, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[0].(plugins.BlockNumber) != testBlockNumber {
		t.Fatal("GetBlockTransactionCountByNumber did not return expected parameter blockNumber, heavy test", "err", err.Error())
	}

	_, err = b.GetBlockTransactionCountByHash(context.Background(), testHash)
	if err == nil {
		t.Fatal("GetBlockTransactionCountByHash did not return expected error, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Method != "eth_getBlockTransactionCountByHash" {
		t.Fatal("GetBlockTransactionCountByHash did not return expected method name, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[0].(types.Hash) != testHash {
		t.Fatal("GetBlockTransactionCountByHash did not return expected parameter hash, heavy test", "err", err.Error())
	}

	_, err = b.GetUncleCountByBlockNumber(context.Background(), testBlockNumber)
	if err == nil {
		t.Fatal("GetUncleCountByBlockNumber did not return expected error, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Method != "eth_getUncleCountByBlockNumber" {
		t.Fatal("GetUncleCountByBlockNumber did not return expected method name, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[0].(plugins.BlockNumber) != testBlockNumber {
		t.Fatal("GetUncleCountByBlockNumber did not return expected parameter blockNumber, heavy test", "err", err.Error())
	}

	_, err = b.GetUncleCountByBlockHash(context.Background(), testHash)
	if err == nil {
		t.Fatal("GetUncleCountByBlockHash did not return expected error, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Method != "eth_getUncleCountByBlockHash" {
		t.Fatal("GetUncleCountByBlockHash did not return expected method name, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[0].(types.Hash) != testHash {
		t.Fatal("GetUncleCountByBlockHash did not return expected parameter hash, heavy test", "err", err.Error())
	}

	tx := NewTransactionAPI(db, 1, pl, cfg)

	_, err = tx.GetTransactionByHash(context.Background(), testHash)
	if err == nil {
		t.Fatal("GetTransactionByHash did not return expected error, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Method != "eth_getTransactionByHash" {
		t.Fatal("GetTransactionByHash did not return expected method name, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[0].(types.Hash) != testHash {
		t.Fatal("GetTransactionByHash did not return expected parameters, heavy test", "err", err.Error())
	}

	_, err = tx.GetTransactionByBlockHashAndIndex(context.Background(), testHash, testUint64)
	if err == nil {
		t.Fatal("GetTransactionByBlockHashAndIndex did not return expected error, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Method != "eth_getTransactionByBlockHashAndIndex" {
		t.Fatal("GetTransactionByBlockHashAndIndex did not return expected method name, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[0].(types.Hash) != testHash {
		t.Fatal("GetTransactionByBlockHashAndIndex did not return expected parameter hash, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[1].(hexutil.Uint64) != testUint64 {
		t.Fatal("GetTransactionByBlockHashAndIndex did not return expected parameter index, heavy test", "err", err.Error())
	}

	_, err = tx.GetTransactionByBlockNumberAndIndex(context.Background(), testBlockNumber, testUint64)
	if err == nil {
		t.Fatal("GetTransactionByBlockNumberAndIndex did not return expected error, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Method != "eth_getTransactionByBlockNumberAndIndex" {
		t.Fatal("GetTransactionByBlockNumberAndIndex did not return expected method name, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[0].(plugins.BlockNumber) != testBlockNumber {
		t.Fatal("GetTransactionByBlockNumberAndIndex did not return expected parameter blockNumber, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[1].(hexutil.Uint64) != testUint64 {
		t.Fatal("GetTransactionByBlockNumberAndIndex did not return expected parameter index, heavy test", "err", err.Error())
	}

	_, err = tx.GetTransactionReceipt(context.Background(), testHash)
	if err == nil {
		t.Fatal("GetTransactionReceipt did not return expected error, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Method != "eth_getTransactionReceipt" {
		t.Fatal("GetTransactionReceipt did not return expected method name, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[0].(types.Hash) != testHash {
		t.Fatal("GetTransactionReceipt did not return expected parameters, heavy test", "err", err.Error())
	}

	_, err = tx.GetTransactionCount(context.Background(), testAddress)
	if err == nil {
		t.Fatal("GetTransactionCount did not return expected error, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Method != "eth_getTransactionCount" {
		t.Fatal("GetTransactionCount did not return expected method name, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[0].(common.Address) != testAddress {
		t.Fatal("GetTransactionCount did not return expected parameter address, heavy test", "err", err.Error())
	}

	l := NewLogsAPI(db, 1, pl, cfg)

	testFilterQuery := FilterQuery{
		BlockHash: &testHash,
		Addresses: []common.Address{testAddress},
	}

	_, err = l.GetLogs(context.Background(), testFilterQuery)
	if err == nil {
		t.Fatal("GetLogs did not return expected error, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Method != "eth_getLogs" {
		t.Fatal("GetLogs did not return expected method name, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[0].(FilterQuery).BlockHash != testFilterQuery.BlockHash {
		t.Fatal("GetLogs did not return expected parameter hash, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[0].(FilterQuery).Addresses[0] != testFilterQuery.Addresses[0] {
		t.Fatal("GetLogs did not return expected parameter address, heavy test", "err", err.Error())
	}

	g := NewGasAPI(db, 1, pl, cfg)

	var blockCount DecimalOrHex = 0x1
	percentiles := []float64{.1, .5, .9}

	_, err = g.FeeHistory(context.Background(), blockCount, testBlockNumber, percentiles)
	if err == nil {
		t.Fatal("FeeHistory did not return expected error, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Method != "eth_feeHistory" {
		t.Fatal("FeeHistory did not return expected method name, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[0].(DecimalOrHex) != blockCount {
		t.Fatal("FeeHistory did not return expected parameter blockCount, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[1].(plugins.BlockNumber) != testBlockNumber {
		t.Fatal("FeeHistory did not return expected parameter blockNumber, heavy test", "err", err.Error())
	}
	for i, p := range err.(*heavy.MockError).Params[2].([]float64) {
		if p != percentiles[i] {
			t.Fatal("FeeHistory did not return expected parameter percentiles, heavy test", "index", i, "err", err.Error())
		}
	}

	f := NewFlumeAPI(db, 1, pl, cfg)

	_, err = f.GetTransactionsBySender(context.Background(), testAddress, nil)
	if err == nil {
		t.Fatal("GetTransactionsBySender did not return expected error, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Method != "flume_getTransactionsBySender" {
		t.Fatal("GetTransactionsBySender did not return expected method name, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[0].(common.Address) != testAddress {
		t.Fatal("GetTransactionsBySender did not return expected parameter address, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[1].(*int) != nil {
		t.Fatal("GetTransactionsBySender did not return expected parameter offset, heavy test", "err", err.Error())
	}

	_, err = f.GetTransactionReceiptsBySender(context.Background(), testAddress, nil)
	if err == nil {
		t.Fatal("GetTransactionReceiptsBySender did not return expected error, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Method != "flume_getTransactionReceiptsBySender" {
		t.Fatal("GetTransactionReceiptsBySender did not return expected method name, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[0].(common.Address) != testAddress {
		t.Fatal("GetTransactionReceiptsBySender did not return expected parameter address, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[1].(*int) != nil {
		t.Fatal("GetTransactionReceiptsBySender did not return expected parameter offset, heavy test", "err", err.Error())
	}

	_, err = f.GetTransactionsByRecipient(context.Background(), testAddress, nil)
	if err == nil {
		t.Fatal("GetTransactionsByRecipient did not return expected error, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Method != "flume_getTransactionsByRecipient" {
		t.Fatal("GetTransactionsByRecipient did not return expected method name, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[0].(common.Address) != testAddress {
		t.Fatal("GetTransactionsByRecipient did not return expected parameter address, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[1].(*int) != nil {
		t.Fatal("GetTransactionsByRecipient did not return expected parameter offset, heavy test", "err", err.Error())
	}

	_, err = f.GetTransactionReceiptsByRecipient(context.Background(), testAddress, nil)
	if err == nil {
		t.Fatal("GetTransactionReceiptsByRecipient did not return expected error, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Method != "flume_getTransactionReceiptsByRecipient" {
		t.Fatal("GetTransactionReceiptsByRecipient did not return expected method name, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[0].(common.Address) != testAddress {
		t.Fatal("GetTransactionReceiptsByRecipient did not return expected parameter address, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[1].(*int) != nil {
		t.Fatal("GetTransactionReceiptsByRecipient did not return expected parameter offset, heavy test", "err", err.Error())
	}

	_, err = f.GetTransactionsByParticipant(context.Background(), testAddress, nil)
	if err == nil {
		t.Fatal("GetTransactionsByParticipant did not return expected error, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Method != "flume_getTransactionsByParticipant" {
		t.Fatal("GetTransactionsByParticipant did not return expected method name, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[0].(common.Address) != testAddress {
		t.Fatal("GetTransactionsByParticipant did not return expected parameter address, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[1].(*int) != nil {
		t.Fatal("GetTransactionsByParticipant did not return expected parameter offset, heavy test", "err", err.Error())
	}

	_, err = f.GetTransactionReceiptsByParticipant(context.Background(), testAddress, nil)
	if err == nil {
		t.Fatal("GetTransactionReceiptsByParticipant did not return expected error, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Method != "flume_getTransactionReceiptsByParticipant" {
		t.Fatal("GetTransactionsByParticipant did not return expected method name, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[0].(common.Address) != testAddress {
		t.Fatal("GetTransactionReceiptsByParticipant did not return expected parameter address, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[1].(*int) != nil {
		t.Fatal("GetTransactionReceiptsByParticipant did not return expected parameter offset, heavy test", "err", err.Error())
	}

	_, err = f.GetTransactionReceiptsByBlockHash(context.Background(), testHash)
	if err == nil {
		t.Fatal("GetTransactionReceiptsByBlockHash did not return expected error, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Method != "flume_getTransactionReceiptsByBlockHash" {
		t.Fatal("GetTransactionReceiptsByBlockHash did not return expected method name, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[0].(types.Hash) != testHash {
		t.Fatal("GetTransactionReceiptsByBlockHash did not return expected parameter hash, heavy test", "err", err.Error())
	}

	_, err = f.GetTransactionReceiptsByBlockNumber(context.Background(), testBlockNumber)
	if err == nil {
		t.Fatal("GetTransactionReceiptsByBlockNumber did not return expected error, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Method != "flume_getTransactionReceiptsByBlockNumber" {
		t.Fatal("GetTransactionReceiptsByBlockNumber did not return expected method name, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[0].(plugins.BlockNumber) != testBlockNumber {
		t.Fatal("GetTransactionReceiptsByBlockNumber did not return expected parameter BlockNumber, heavy test", "err", err.Error())
	}

	ft := NewFlumeTokensAPI(db, 1, pl, cfg)
	var offset *int
	offset = new(int)
	*offset = 1

	_, err = ft.Erc20ByAccount(context.Background(), testAddress, offset)
	if err == nil {
		t.Fatal("Erc20ByAccount did not return expected error, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Method != "flume_erc20ByAccount" {
		t.Fatal("Erc20ByAccount did not return expected method name, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[0].(common.Address) != testAddress {
		t.Fatal("Erc20ByAccount did not return expected parameter address, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[1].(*int) != offset {
		t.Fatal("Erc20ByAccount did not return expected parameter offset, heavy test", "err", err.(*heavy.MockError).Params[1])
	}

	_, err = ft.Erc20Holders(context.Background(), testAddress, offset)
	if err == nil {
		t.Fatal("Erc20Holders did not return expected error, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Method != "flume_erc20Holders" {
		t.Fatal("Erc20ByAccount did not return expected method name, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[0].(common.Address) != testAddress {
		t.Fatal("Erc20Holders did not return expected parameter address, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Params[1].(*int) != offset {
		t.Fatal("Erc20Holders did not return expected parameter offset, heavy test", "err", err.Error())
	}

	cfg.EarliestBlock = 14000022

	_, err = g.GasPrice(context.Background())
	if err == nil {
		t.Fatal("GasPrice did not return expected error, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Method != "eth_gasPrice" {
		t.Fatal("GasPrice did not return expected method name, heavy test", "err", err.Error())
	}

	_, err = g.MaxPriorityFeePerGas(context.Background())
	if err == nil {
		t.Fatal("MaxPriorityFeePerGas did not return expected error, heavy test", "err", err.Error())
	}
	if err.(*heavy.MockError).Method != "eth_maxPriorityFeePerGas" {
		t.Fatal("MaxPriorityFeePerGas did not return expected method name, heavy test", "err", err.Error())
	}

}
