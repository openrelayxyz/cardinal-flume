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

	topic2 := "0xdac17f958d2ee523a2206206994597c13d831ec7"

	t.Run("Erc20Holders", func(t *testing.T) {
		control := controlData[0]
		test, err := ft.Erc20Holders(mockContext, common.HexToAddress(topic2), nil); if err != nil {
			t.Fatalf("failed to call Erc20Holders: %v", err)
		}
		if len(control) != len(test.Items) {
			t.Fatalf("length mismatch in Erc20Holders: control len %d, test len %d", len(control), len(test.Items))
		} 
		for i, controlAddr := range control {
			if controlAddr != test.Items[i] {
				t.Fatalf("Erc20Holders error at index %d: expected %v, got %v", i, controlAddr, test.Items[i])
			}
		}
	})

	address := "0x74de5d4fcbf63e00296fd95d33236b9794016631"

	t.Run("Erc20ByAccount", func(t *testing.T) {
		control := controlData[1]
		test, err := ft.Erc20ByAccount(mockContext, common.HexToAddress(address), nil); if err != nil {
			t.Fatalf("failed to call Erc20ByAccount: %v", err)
		}
		if len(control) != len(test.Items) {
			t.Fatalf("length mismatch in Erc20ByAccount: control len %d, test len %d", len(control), len(test.Items))
		}
		for i, controlAddr := range control {
			if controlAddr != test.Items[i] {
				t.Fatalf("Erc20ByAccount error at index %v: control %v, test %v", i, controlAddr, test.Items[i])
			}
		}
	})
}
 