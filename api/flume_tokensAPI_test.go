package api

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"io/ioutil"
	_ "net/http/pprof"
	"os"
	"testing"

	"github.com/openrelayxyz/cardinal-evm/common"
	"github.com/openrelayxyz/cardinal-flume/config"
	"github.com/openrelayxyz/cardinal-flume/plugins"
)

func tokenDataDecompress() ([][]common.Address, error) {
	file, _ := ioutil.ReadFile("../testing-resources/token_test_data.json.gz")
	r, err := gzip.NewReader(bytes.NewReader(file))
	if err != nil {
		return nil, err
	}
	raw, _ := ioutil.ReadAll(r)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, err
	}
	var tokenData [][]common.Address
	json.Unmarshal(raw, &tokenData)
	return tokenData, nil
}

func TestERCMethods(t *testing.T) {
	cfg, err := config.LoadConfig("../testing-resources/api_test_config.yml")
	if err != nil {
		t.Fatal("Error parsing config TestFlumeTokensAPI", "err", err.Error())
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
	ft := NewFlumeTokensAPI(db, 1, pl, cfg)

	controlData, _ := tokenDataDecompress()

	address := "0xdac17f958d2ee523a2206206994597c13d831ec7"

	t.Run("Erc20Holders", func(t *testing.T) {
		expected := controlData[0]
		actual, err := ft.Erc20Holders(mockContext, common.HexToAddress(address), nil); if err != nil {
			t.Fatalf("failed to call Erc20Holders: %v", err)
		}

		for i, expectedAddr := range expected {
			if i >= len(actual.Items) {
				t.Fatalf("Index %v is out of range for actual.Items with length %v", i, len(expected))
			}
			if actual.Items[i] != expectedAddr {
				t.Fatalf("Erc20Holders error at index %d: expected %v, got %v", i, expectedAddr, actual.Items[i])
			}
		}
	})
	t.Run("Erc20ByAccount", func(t *testing.T) {
		expected := controlData[1]
		actual, err := ft.Erc20ByAccount(mockContext, common.HexToAddress(address), nil); if err != nil {
			t.Fatalf("failed to call Erc20ByAccount: %v", err)
		}
		for i, expectedAddr := range expected {
			if len(expected) != len(actual.Items) {
				t.Logf("length mismatch in Erc20ByAccount: expected %d, got %d", len(expected), len(actual.Items))
				continue
			} else if actual.Items[i] != expectedAddr {
				t.Fatalf("Erc20ByAccount error at index %v: expected %v, got %v", i, expectedAddr, actual.Items[i])
			}
			
		}
	})
}
 