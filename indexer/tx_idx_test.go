package indexer

import (
	"bytes"
	"fmt"
	log "github.com/inconshreveable/log15"
	"github.com/klauspost/compress/zlib"
	_ "github.com/mattn/go-sqlite3"
	"os"
	"io"
	"io/ioutil"
	"reflect"
	"testing"
)

func decompress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}
	r, err := zlib.NewReader(bytes.NewBuffer(data))
	if err != nil {
		return []byte{}, err
	}
	raw, err := ioutil.ReadAll(r)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return raw, nil
	}
	return raw, err
}

func TestTransactionIndexer(t *testing.T) {
	
	test_dbs := make(map[string]string)
	test_dbs["control"] = "../testing-resources/transactions.sqlite"
	test_dbs["transactions"] = "../testing-resources/test.sqlite"
	
	controlDB, err := openControlDatabase(test_dbs)
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer os.Remove(test_dbs["control"] + "-wal")
	defer os.Remove(test_dbs["control"] + "-shm")
	defer os.Remove("../testing-resources/test.sqlite")
	defer controlDB.Close()
	_, err = controlDB.Exec(`CREATE TABLE transactions.transactions (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				gas BIGINT,
				gasPrice BIGINT,
				hash varchar(32) UNIQUE,
				input blob,
				nonce BIGINT,
				recipient varchar(20),
				transactionIndex MEDIUMINT,
				value varchar(32),
				v SMALLINT,
				r varchar(32),
				s varchar(32),
				sender varchar(20),
				func varchar(4),
				contractAddress varchar(20),
				cumulativeGasUsed BIGINT,
				gasUsed BIGINT,
				logsBloom blob,
				status TINYINT,
				block BIGINT,
				type TINYINT,
				access_list blob,
				gasFeeCap varchar(32),
				gasTipCap varchar(32),
				MaxFeePerBlobGas BIGINT,
				blobVersionedHashes blob)`)
	if err != nil {
		t.Fatalf(err.Error())
	}

	batches, err := pendingBatchDecompress()
	if err != nil {
		t.Fatalf(err.Error())
	}
	ti := NewTxIndexer(1, 2675000, 1150000, false)
	log.Info("Transaciton indexer test", "Decompressing batches of length:", len(batches))
	statements := []string{}
	for _, pb := range batches {
		group, err := ti.Index(pb)
		if err != nil {
			t.Fatalf(err.Error())
		}
		statements = append(statements, group...)
	}
	//megaStatement adds significant time to test. TODO: investigate why
	// megaStatement := strings.Join(statements, ";")
	// _, err = controlDB.Exec(megaStatement)
	// if err != nil {t.Fatalf(err.Error())}
	for i, statement := range statements {
		_, err := controlDB.Exec(statement)
		if err != nil {
			t.Fatalf("error: %v, statement: %v, index: %v", err.Error(), statement, i)
		}
	}

	fields := []string{"block", "gas", "gasPrice", "hash", "input", "nonce", "recipient", "transactionIndex", "value", "v", "r", "s", "sender", "func", "contractAddress", "cumulativeGasUsed", "gasUsed", "logsBloom", "status", "type", "access_list", "gasFeeCap", "gasTipCap"}

	for _, item := range fields {
		query := fmt.Sprintf("SELECT t.block, t.transactionIndex, t.type, t.%v, transactions.%v  FROM transactions INNER JOIN control.transactions as t on transactions.hash = t.hash", item, item)
		rows, err := controlDB.Query(query)
		if err != nil {
			t.Fatalf(err.Error())
		}
		defer rows.Close()
		for rows.Next() {
			var block, txDx, typ int64
			var control, test interface{}
			rows.Scan(&block, &txDx, &typ, &control, &test)
			cs := reflect.TypeOf(control)
			ts := reflect.TypeOf(test)
			switch v := test.(type) {
			default:
				t.Errorf("unknown type: item:%v, block:%v, txDx:%v, control type:%v, test type:%v, switch type:%v", item, block, txDx, cs, ts, v)
			case nil:
				ctl, _ := control.([]uint8)
				if len(ctl) != 0 {
					t.Fatalf("deivergent values on field %v: block %v : transaction %v:", item, block, txDx)
				}
			case int64:
				x, _ := control.(int64)
				if x != v {
					t.Fatalf("divergent values on field %v: block %v : transaction %v:", item, block, txDx)
				}
			case []uint8:
				var cntrl, tst []uint8
				switch item {
				default:
					cntrl, _ = control.([]uint8)
					tst = v
				case "gasTipCap", "gasFeeCap":
					if typ < 2 {
						continue
					}
					cntrl, _ = control.([]uint8)
					tst = v
				case "input", "logsBloom", "access_list":
					var err error
					tst, err = decompress(v)
					if err != nil {
						t.Fatalf(err.Error())
					}
					cntrl, err = decompress(control.([]uint8))
					if err != nil {
						t.Fatalf(err.Error())
					}
				}
				if len(cntrl) != len(tst) {
					t.Fatalf("divergent lengths on field %v: block %v : transaction %v:", item, block, txDx)
				}
				for i, item := range cntrl {
					if tst[i] != item {
						t.Fatalf("divergent values on field %v: block %v : transaction %v:", item, block, txDx)
					}
				}
			}
		}
	}
}
