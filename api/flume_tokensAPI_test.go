package api

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	_ "net/http/pprof"
	"testing"
	"os"

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
	ft := NewFlumeTokensAPI(db, 1, pl, cfg)

	data, _ := tokenDataDecompress()

	address := "0xdac17f958d2ee523a2206206994597c13d831ec7"

	t.Run(fmt.Sprintf("Erc20Holders"), func(t *testing.T) {
		actual, _ := ft.Erc20Holders(context.Background(), common.HexToAddress(address), nil)
		for i, addr := range actual.Items {
			if addr != data[0][i] {
				t.Fatalf("Erc20Holders error")
			}
		}
	})
	t.Run(fmt.Sprintf("Erc20ByAccount"), func(t *testing.T) {
		actual, _ := ft.Erc20ByAccount(context.Background(), common.HexToAddress(address), nil)
		for i, addr := range actual.Items {
			if addr != data[1][i] {
				t.Fatalf("Erc20ByAccount error")
			}
		}
	})
}
