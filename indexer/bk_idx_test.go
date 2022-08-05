package indexer

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"compress/gzip"
	"database/sql"
	"encoding/json"
	"github.com/mattn/go-sqlite3"
	"io"
	"io/ioutil"
	_ "net/http/pprof"
	"path/filepath"

	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-streams/delivery"
	"github.com/openrelayxyz/cardinal-streams/transports"
)

func openControlDatabase(dbs map[string]string) (*sql.DB, error) {
	registrar := filepath.Base(dbs["control"])
	i := strings.LastIndex(registrar, ".sqlite")
	sql.Register(fmt.Sprintf("sqlite3_%v", registrar[:i]),
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				for name, path := range dbs {
					conn.Exec(fmt.Sprintf("ATTACH DATABASE '%v' AS '%v';", path, name), nil)
				}
				return nil
			},
		})

	memDB, err := sql.Open(fmt.Sprintf("sqlite3_%v", registrar[:i]), ":memory:")
	if err != nil {
		log.Error(err.Error())
	}
	memDB.SetConnMaxLifetime(0)
	memDB.SetMaxIdleConns(32)
	return memDB, nil
}

func pendingBatchDecompress() ([]*delivery.PendingBatch, error) {
	file, _ := ioutil.ReadFile("flume-test-data.json.gz")
	r, err := gzip.NewReader(bytes.NewReader(file))
	if err != nil {
		return nil, err
	}
	raw, _ := ioutil.ReadAll(r)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, err
	}
	var transportsObjectSlice []*transports.TransportBatch
	json.Unmarshal(raw, &transportsObjectSlice)
	pbSlice := []*delivery.PendingBatch{}
	for _, item := range transportsObjectSlice {
		pb := item.ToPendingBatch()
		pbSlice = append(pbSlice, pb)
	}
	return pbSlice, nil
}

func TestBlockIndexer(t *testing.T) {

	test_dbs := make(map[string]string)
	test_dbs["control"] = "../blocks.sqlite"

	controlDB, err := openControlDatabase(test_dbs)
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer controlDB.Close()
	_, err = controlDB.Exec(`CREATE TABLE blocks (
				number      BIGINT PRIMARY KEY,
				hash        varchar(32) UNIQUE,
				parentHash  varchar(32),
				uncleHash   varchar(32),
				coinbase    varchar(20),
				root        varchar(32),
				txRoot      varchar(32),
				receiptRoot varchar(32),
				bloom       blob,
				difficulty  varchar(32),
				gasLimit    BIGINT,
				gasUsed     BIGINT,
				time        BIGINT,
				extra       blob,
				mixDigest   varchar(32),
				nonce       BIGINT,
				uncles      blob,
				size        BIGINT,
				td          varchar(32),
				baseFee varchar(32))`)
	if err != nil {
		t.Fatalf(err.Error())
	}

	batches, err := pendingBatchDecompress()
	if err != nil {
		t.Fatalf(err.Error())
	}
	log.Info("Block indexer test", "Decompressing batches of length:", len(batches))
	b := NewBlockIndexer(1)

	statements := make([]string, len(batches))
	for _, pb := range batches {
		group, err := b.Index(pb)
		if err != nil {
			t.Fatalf(err.Error())
		}
		statements = append(statements, group...)
	}

	megaStatement := strings.Join(statements, ";")
	_, err = controlDB.Exec(megaStatement)
	if err != nil {
		t.Fatalf(err.Error())
	}

	query := "SELECT b.number = blocks.number, b.hash = blocks.hash, b.parentHash = blocks.parentHash, b.uncleHash = blocks.uncleHash, b.coinbase = blocks.coinbase, b.root = blocks.root, b.txRoot = blocks.txRoot, b.receiptRoot = blocks.receiptRoot, b.bloom IS blocks.bloom, b.difficulty = blocks.difficulty, b.gasLimit = blocks.gasLimit, b.gasUsed = blocks.gasUsed, b.time = blocks.time, b.extra = blocks.extra, b.mixDigest = blocks.mixDigest, b.nonce = blocks.Nonce, b.uncles = blocks.uncles, b.size =  blocks.size, b.td = blocks.td, b.baseFee IS blocks.baseFee FROM blocks INNER JOIN control.blocks as b on blocks.number = b.number"

	results := make([]any, 20)
	for i := 0; i < len(results); i++ {
		var x bool
		results[i] = &x
	}
	rows, err := controlDB.Query(query)
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer rows.Close()

	for rows.Next() {
		rows.Scan(results...)
		for i, item := range results {
			if i == 8 {
				continue
			}
			if v, ok := item.(*bool); !*v || !ok {
				t.Errorf("failed on index %v, %v, %v", i, *v, ok)
			}
		}
	}
}
