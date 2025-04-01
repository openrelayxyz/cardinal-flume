package api

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"

	_ "net/http/pprof"

	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-flume/config"
	"github.com/openrelayxyz/cardinal-flume/plugins"
	rpc "github.com/openrelayxyz/cardinal-rpc"
)

func feeDataDecompress() (map[string]json.RawMessage, error) {
	file, _ := ioutil.ReadFile("../testing-resources/fee_test_data.json.gz")
	r, err := gzip.NewReader(bytes.NewReader(file))
	if err != nil {
		return nil, err
	}
	raw, _ := ioutil.ReadAll(r)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, err
	}
	var feeData map[string]json.RawMessage
	json.Unmarshal(raw, &feeData)
	return feeData, nil
}

func getRewardsList(jsonObject json.RawMessage) []json.RawMessage {
	var result []json.RawMessage
	json.Unmarshal(jsonObject, &result)
	return result
}

func TestGasAPI(t *testing.T) {
	cfg, err := config.LoadConfig("../testing-resources/api_test_config.yml")
	if err != nil {
		t.Fatal("Error parsing config TestGasAPI", "err", err.Error())
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
	g := NewGasAPI(db, 1, pl, cfg, mempool)

	price := "0xa972a9bf6"
	fee := "0x77359400"

	t.Run("GasPrice", func(t *testing.T) {
		actual, _ := g.GasPrice(context.Background())
		if actual != price {
			t.Fatalf("GasPrice error")
		}
	})
	t.Run("MaxPriorityFeePerGas", func(t *testing.T) {
		actual, _ := g.MaxPriorityFeePerGas(context.Background())
		if actual != fee {
			t.Fatalf("MaxPriorityFeePerGas error")
		}
	})

	feeData, _ := feeDataDecompress()
	t.Run("FeeHistory", func(t *testing.T) {
		var blockCount DecimalOrHex = 0xf
		var lastBlock rpc.BlockNumber = 0xd59f95
		percentiles := []float64{10, 50, 90}

		actual, err := g.FeeHistory(context.Background(), blockCount, lastBlock, percentiles); if err!=nil{
			t.Fatalf("failed to call FeeHistory: %v", err)
		}
		expectedOldest := feeData["oldestBlock"]
		actualOldest, err := json.Marshal(actual.OldestBlock); if err != nil {
			t.Errorf("failed to marshal actual oldestBlock: %v", err)
		}
		if !bytes.Equal(actualOldest, expectedOldest) {
			t.Fatalf("FeeHistory oldestBlock mismatch\nexpected: %v\nactual: %v", string(expectedOldest), string(actualOldest))
		}

		var expectedReward [][]json.RawMessage
		if err := json.Unmarshal(feeData["reward"], &expectedReward); err != nil {
			t.Fatalf("failed to unmarshal expected reward: %v", err)
		}
		for i, expectedSlice := range expectedReward {
			actualSlice := actual.Reward[i]
			for j, expectedValue := range expectedSlice {
				actualValue := actualSlice[j]
				data, err := json.Marshal(actualValue)
				if err != nil {
					t.Fatalf("failed to marshal reward value at [%d][%d]: %v", i, j, err)
				}
				if !bytes.Equal(data, expectedValue) {
					log.Error(fmt.Sprintf("FeeHistory reward mismatch at [%v][%v]\nexpected: %v\nactual: %s",i, j, string(expectedValue), string(data)))
				}
			}
		}
		
		var expectedBaseFees []json.RawMessage
		json.Unmarshal(feeData["baseFeePerGas"], &expectedBaseFees)
		for i, expectedValue := range expectedBaseFees {
			actualValue := actual.BaseFee[i]
			data, err := json.Marshal(actualValue)
			if err != nil {
				t.Errorf("failed to marshal baseFee at index %d: %v", i, err)
			}
			if !bytes.Equal(data, expectedValue) {
				t.Fatalf("FeeHistory baseFeePerGas mismatch at index %v\nexpected: %v\nactual: %v",i, string(expectedValue), string(data))
			}
		}
		var expectedGasUsed []json.RawMessage
		json.Unmarshal(feeData["gasUsedRatio"], &expectedGasUsed)
		for i, expectedValue := range expectedGasUsed {
			actualValue := actual.GasUsedRatio[i]
			data, err := json.Marshal(actualValue)
			if err != nil {
				t.Errorf("failed to marshal gasUsedRatio at index %v: %v", i, err)
			}
			if !bytes.Equal(data, expectedValue) {
				t.Fatalf("FeeHistory gasUsedRatio mismatch at index %d\nexpected: %s\nactual: %s",i, string(expectedValue), string(data))
			}
		}
	})
}
