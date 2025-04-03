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
		test, _ := g.GasPrice(context.Background())
		if test != price {
			t.Fatalf("GasPrice error")
		}
	})
	t.Run("MaxPriorityFeePerGas", func(t *testing.T) {
		test, _ := g.MaxPriorityFeePerGas(context.Background())
		if test != fee {
			t.Fatalf("MaxPriorityFeePerGas error")
		}
	})

	feeData, _ := feeDataDecompress()
	t.Run("FeeHistory", func(t *testing.T) {
		var blockCount DecimalOrHex = 0xf
		var lastBlock rpc.BlockNumber = 0xd59f95
		percentiles := []float64{10, 50, 90}

		test, err := g.FeeHistory(context.Background(), blockCount, lastBlock, percentiles); if err!=nil{
			t.Fatalf("failed to call FeeHistory: %v", err)
		}
		controlOldest := feeData["oldestBlock"]
		testOldest, err := json.Marshal(test.OldestBlock); if err != nil {
			t.Errorf("failed to marshal test oldestBlock: %v", err)
		}
		if !bytes.Equal(testOldest, controlOldest) {
			t.Fatalf("FeeHistory oldestBlock mismatch\ncontrol: %v\ntest: %v", string(controlOldest), string(testOldest))
		}

		var controlReward [][]json.RawMessage
		if err := json.Unmarshal(feeData["reward"], &controlReward); err != nil {
			t.Fatalf("failed to unmarshal control reward: %v", err)
		}
		for i, controlSlice := range controlReward {
			testSlice := test.Reward[i]
			for j, controlValue := range controlSlice {
				testValue := testSlice[j]
				data, err := json.Marshal(testValue)
				if err != nil {
					t.Fatalf("failed to marshal reward value at [%d][%d]: %v", i, j, err)
				}
				if !bytes.Equal(data, controlValue) {
					log.Error(fmt.Sprintf("FeeHistory reward mismatch at [%v][%v]\ncontrol: %v\ntest: %s",i, j, string(controlValue), string(data)))
				}
			}
		}
		
		var controlBaseFees []json.RawMessage
		json.Unmarshal(feeData["baseFeePerGas"], &controlBaseFees)
		for i, controlValue := range controlBaseFees {
			testValue := test.BaseFee[i]
			data, err := json.Marshal(testValue)
			if err != nil {
				t.Errorf("failed to marshal baseFee at index %d: %v", i, err)
			}
			if !bytes.Equal(data, controlValue) {
				t.Fatalf("FeeHistory baseFeePerGas mismatch at index %v\ncontrol: %v\ntest: %v",i, string(controlValue), string(data))
			}
		}
		var controlGasUsed []json.RawMessage
		json.Unmarshal(feeData["gasUsedRatio"], &controlGasUsed)
		for i, controlValue := range controlGasUsed {
			testValue := test.GasUsedRatio[i]
			data, err := json.Marshal(testValue)
			if err != nil {
				t.Errorf("failed to marshal gasUsedRatio at index %v: %v", i, err)
			}
			if !bytes.Equal(data, controlValue) {
				t.Fatalf("FeeHistory gasUsedRatio mismatch at index %d\ncontrol: %s\ntest: %s",i, string(controlValue), string(data))
			}
		}
	})
}
