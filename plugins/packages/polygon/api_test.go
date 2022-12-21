package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/mattn/go-sqlite3"
	"io"
	"io/ioutil"
	_ "net/http/pprof"
	"os"
	"strings"
	"sync"
	"testing"

	log "github.com/inconshreveable/log15"

	"github.com/openrelayxyz/cardinal-evm/common"
	"github.com/openrelayxyz/cardinal-flume/api"
	"github.com/openrelayxyz/cardinal-flume/config"
	"github.com/openrelayxyz/cardinal-flume/indexer"
	"github.com/openrelayxyz/cardinal-flume/migrations"
	"github.com/openrelayxyz/cardinal-flume/plugins"
	"github.com/openrelayxyz/cardinal-streams/delivery"
	"github.com/openrelayxyz/cardinal-streams/transports"
	"github.com/openrelayxyz/cardinal-types"
)

func pendingBatchDecompress() ([]*delivery.PendingBatch, error) {
	file, _ := ioutil.ReadFile("./test-resources/pending_batches.json.gz")
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

func snapshotDecompress() ([]map[string]json.RawMessage, error) {
	file, _ := ioutil.ReadFile("test-resources/snapshots.json.gz")
	r, err := gzip.NewReader(bytes.NewReader(file))
	if err != nil {
		return nil, err
	}
	raw, _ := ioutil.ReadAll(r)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, err
	}
	var blocks []map[string]json.RawMessage
	json.Unmarshal(raw, &blocks)
	return blocks, nil
}

func txReceiptBlockDecompress() ([][]map[string]json.RawMessage, error) {
	file, _ := ioutil.ReadFile("test-resources/blockTxReceipts.json.gz")
	r, err := gzip.NewReader(bytes.NewReader(file))
	if err != nil {
		return nil, err
	}
	raw, _ := ioutil.ReadAll(r)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, err
	}
	var blockTxReceipts [][]map[string]json.RawMessage
	json.Unmarshal(raw, &blockTxReceipts)
	return blockTxReceipts, nil
}

func rootHashDecompress() ([]string, error) {
	file, _ := ioutil.ReadFile("test-resources/root_hashes.json.gz")
	r, err := gzip.NewReader(bytes.NewReader(file))
	if err != nil {
		return nil, err
	}
	raw, _ := ioutil.ReadAll(r)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, err
	}
	var rootHashes []string
	json.Unmarshal(raw, &rootHashes)
	return rootHashes, nil
}

func authorDecompress() ([]*common.Address, error) {
	file, _ := ioutil.ReadFile("test-resources/authors.json.gz")
	r, err := gzip.NewReader(bytes.NewReader(file))
	if err != nil {
		return nil, err
	}
	raw, _ := ioutil.ReadAll(r)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, err
	}
	var authors []*common.Address
	json.Unmarshal(raw, &authors)
	return authors, nil
}

func signerDecompress() (map[types.Hash][]common.Address, error) {
	file, _ := ioutil.ReadFile("test-resources/signers.json.gz")
	r, err := gzip.NewReader(bytes.NewReader(file))
	if err != nil {
		return nil, err
	}
	raw, _ := ioutil.ReadAll(r)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, err
	}
	var signers map[types.Hash][]common.Address
	json.Unmarshal(raw, &signers)
	return signers, nil
}

func blockDecompress() ([]map[string]json.RawMessage, error) {
	file, _ := ioutil.ReadFile("test-resources/blocks.json.gz")
	r, err := gzip.NewReader(bytes.NewReader(file))
	if err != nil {
		return nil, err
	}
	raw, _ := ioutil.ReadAll(r)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, err
	}
	var blocks []map[string]json.RawMessage
	json.Unmarshal(raw, &blocks)
	return blocks, nil
}

func txDecompress() ([]map[string]json.RawMessage, error) {
	file, _ := ioutil.ReadFile("test-resources/txns.json.gz")
	r, err := gzip.NewReader(bytes.NewReader(file))
	if err != nil {
		return nil, err
	}
	raw, _ := ioutil.ReadAll(r)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, err
	}
	var txns []map[string]json.RawMessage
	json.Unmarshal(raw, &txns)
	return txns, nil
}

func blockReceiptsDecompress() ([]map[string]json.RawMessage, error) {
	file, _ := ioutil.ReadFile("test-resources/block_receipts.json.gz")
	r, err := gzip.NewReader(bytes.NewReader(file))
	if err != nil {
		return nil, err
	}
	raw, _ := ioutil.ReadAll(r)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, err
	}
	var receipts []map[string]json.RawMessage
	json.Unmarshal(raw, &receipts)
	return receipts, nil
}

func singleTxReceiptsDecompress() ([]map[string]json.RawMessage, error) {
	file, _ := ioutil.ReadFile("test-resources/tx_receipts.json.gz")
	r, err := gzip.NewReader(bytes.NewReader(file))
	if err != nil {
		return nil, err
	}
	raw, _ := ioutil.ReadAll(r)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, err
	}
	var receipts []map[string]json.RawMessage
	json.Unmarshal(raw, &receipts)
	return receipts, nil
}

var register sync.Once

func testNumbers() []plugins.BlockNumberOrHash {
	var result []plugins.BlockNumberOrHash
	for i := uint64(35779967); i < uint64(35780033); i++ {
		number := plugins.BlockNumber(i)
		num := plugins.BlockNumberOrHash{
			BlockNumber: &number,
		}
		result = append(result, num)
	}
	return result
}

func connectToDatabase(cfg *config.Config) (*sql.DB, error) {

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

	return logsdb, nil
}

func NewBorAPI(db *sql.DB, cfg *config.Config) *PolygonBorService {
	return &PolygonBorService{
		db:  db,
		cfg: cfg,
	}
}

func NewEthAPI(db *sql.DB, cfg *config.Config) *PolygonEthService {
	return &PolygonEthService{
		db:  db,
		cfg: cfg,
	}
}

func TestPolygonApi(t *testing.T) {
	cfg, err := config.LoadConfig("./test-resources/polygon_test_config.yml")
	if err != nil {
		t.Fatalf(err.Error())
	}
	db, _ := connectToDatabase(cfg)
	defer db.Close()
	for _, path := range cfg.Databases {
		defer os.Remove(path)
		defer os.Remove(path + "-wal")
		defer os.Remove(path + "-shm")
	}
	if err := migrations.MigrateBlocks(db, cfg.Chainid); err != nil {
		t.Fatalf(err.Error())
	}
	if err := migrations.MigrateTransactions(db, cfg.Chainid); err != nil {
		t.Fatalf(err.Error())
	}
	if err := migrations.MigrateLogs(db, cfg.Chainid); err != nil {
		t.Fatalf(err.Error())
	}
	if err := migrations.MigrateMempool(db, cfg.Chainid); err != nil {
		t.Fatalf(err.Error())
	}
	if err := Migrate(db, cfg.Chainid); err != nil {
		t.Fatalf(err.Error())
	}
	batches, err := pendingBatchDecompress()
	if err != nil {
		log.Error("pending batch decompression error", "err", err.Error())
		t.Fatalf("error decompressing pending batches")
	}
	indexers := []indexer.Indexer{}
	indexers = append(indexers, indexer.NewBlockIndexer(cfg.Chainid))
	indexers = append(indexers, indexer.NewTxIndexer(cfg.Chainid, cfg.Eip155Block, cfg.HomesteadBlock, false))
	indexers = append(indexers, indexer.NewLogIndexer(cfg.Chainid))
	indexers = append(indexers, Indexer(cfg))

	statements := []string{}
	for _, idx := range indexers {
		for _, pb := range batches {
			group, err := idx.Index(pb)
			if err != nil {
				t.Fatalf("indexing error, indexer %v, err %v", idx, err.Error())
			}
			statements = append(statements, group...)
		}
	}
	megaStatement := strings.Join(statements, ";")
	_, err = db.Exec(megaStatement)
	if err != nil {
		t.Fatalf("error executing megastatement, err %v", err.Error())
	}

	blockNumbers := testNumbers()

	controlSnapshots, err := snapshotDecompress()
	if err != nil {
		t.Fatalf("error decompressing sanpshots, err %v", err.Error())
	}
	controlRootHashes, err := rootHashDecompress()
	if err != nil {
		t.Fatalf("error decompressing rootHashes, err %v", err.Error())
	}
	controlAuthors, err := authorDecompress()
	if err != nil {
		t.Fatalf("error decompressing authors, err %v", err.Error())
	}
	controlSigners, err := signerDecompress()
	if err != nil {
		t.Fatalf("error decompressing signers, err %v", err.Error())
	}
	controlBlockTxReceipts, err := txReceiptBlockDecompress()
	if err != nil {
		t.Fatalf("error decompressing tx receipt blocks, err %v", err.Error())
	}
	controlTxns, err := txDecompress()
	if err != nil {
		t.Fatalf("error decompressing transactions, err %v", err.Error())
	}
	controlReceipts, err := blockReceiptsDecompress()
	if err != nil {
		t.Fatalf("error decompressing block receipts, err %v", err.Error())
	}
	controlTxReceipts, err := singleTxReceiptsDecompress()
	if err != nil {
		t.Fatalf("error decompressing tx receipts, err %v", err.Error())
	}

	bor := NewBorAPI(db, cfg)
	eth := NewEthAPI(db, cfg)

	firstBlock, _ := blockNumbers[0].Number()

	var passThroughBlocks []plugins.BlockNumber

	for i, block := range blockNumbers {
		currentBlock, _ := block.Number()
		if currentBlock%64 == 0 {
			passThroughBlocks = append(passThroughBlocks, plugins.BlockNumber(currentBlock))
		}

		t.Run(fmt.Sprintf("GetTransactionReceiptsByBlock %v", i), func(t *testing.T) {
			testTxReceiptBlock, err := eth.GetTransactionReceiptsByBlock(context.Background(), block)
			if err != nil {
				t.Fatalf("error calling getTransacitonReceiptsByBlock, block %v, err %v", currentBlock, err.Error())
			}
			for j, receipt := range testTxReceiptBlock {
				for key, value := range receipt {
					if key == "logs" {
						logs := value.(plugins.SortLogs)
						var controlLogs plugins.SortLogs
						json.Unmarshal(controlBlockTxReceipts[i][j]["logs"], &controlLogs)
						for k, lg := range logs {
							if lg.Address != controlLogs[k].Address {
								t.Fatalf("getTransactionReceiptsByBlock log mismatch found on block %v receipt %v, log %v, field: Address", currentBlock, j, k)
							}
							if len(lg.Data) != len(controlLogs[k].Data) {
								t.Fatalf("getTransactionReceiptsByBlock log mismatch found on block %v receipt %v, log %v, field: Data ", currentBlock, j, k)
							}
							if lg.BlockNumber != controlLogs[k].BlockNumber {
								t.Fatalf("getTransactionReceiptsByBlock log mismatch found on block %v receipt %v, log %v, field: BlockNumber ", currentBlock, j, k)
							}
							if lg.TxHash != controlLogs[k].TxHash {
								t.Fatalf("getTransactionReceiptsByBlock log mismatch found on block %v receipt %v, log %v, field: TxHash ", currentBlock, j, k)
							}
							if lg.TxIndex != controlLogs[k].TxIndex {
								t.Fatalf("getTransactionReceiptsByBlock log mismatch found on block %v receipt %v, log %v, field: TxIndex ", currentBlock, j, k)
							}
							if lg.BlockHash != controlLogs[k].BlockHash {
								t.Fatalf("getTransactionReceiptsByBlock log mismatch found on block %v receipt %v, log %v, field: BlockHash ", currentBlock, j, k)
							}
							if lg.Index != controlLogs[k].Index {
								t.Fatalf("getTransactionReceiptsByBlock log mismatch found on block %v receipt %v, log %v, field: Index ", currentBlock, j, k)
							}
							if lg.Removed != controlLogs[k].Removed {
								t.Fatalf("getTransactionReceiptsByBlock log mismatch found on block %v receipt %v, log %v, field: Removed ", currentBlock, j, k)
							}
							for idx, topic := range lg.Topics {
								if topic != controlLogs[k].Topics[idx] {
									t.Fatalf("getTransactionReceiptsByBlock log mismatch found on block %v receipt %v, log %v, field Topics, topic index %v", currentBlock, j, idx, k)
								}
							}
						}
					} else {
						d, err := json.Marshal(value)
						if err != nil {
							t.Fatalf("transaction key marshalling error on block %v  tx index %v", i, j)
						}
						if !bytes.Equal(d, controlBlockTxReceipts[i][j][key]) {
							t.Fatalf("getTransactionReceiptsByBlock mismatch found on block %v receipt %v, key %v", block, j, key)
						}
					}
				}
			}
		})

		t.Run(fmt.Sprintf("GetRootHash %v", i), func(t *testing.T) {
			testRootHash, err := bor.GetRootHash(context.Background(), uint64(firstBlock), uint64(currentBlock))
			if err != nil {
				t.Fatalf("error calling getRootHash, block %v, err %v", currentBlock, err.Error())
			}
			if testRootHash != controlRootHashes[i] {
				t.Fatalf("getRootHash mismatch on block %v", currentBlock)
			}
		})

		t.Run(fmt.Sprintf("GetAuthor %v", i), func(t *testing.T) {
			testAuthor, err := bor.GetAuthor(context.Background(), currentBlock)
			if err != nil {
				t.Fatalf("error calling getAuthor, block %v, err %v", currentBlock, err.Error())
			}
			if *testAuthor != *controlAuthors[i] {
				t.Fatalf("getAuthor mismatch on block %v", currentBlock)
			}
		})

		t.Run(fmt.Sprintf("GetSnapshot %v", i), func(t *testing.T) {
			testSnapshot, err := bor.GetSnapshot(context.Background(), block)
			if err != nil {
				t.Fatalf("error calling getSnapshot, block %v, err %v", currentBlock, err.Error())
			}
			var number uint64
			json.Unmarshal(controlSnapshots[i]["number"], &number)
			if testSnapshot.Number != number {
				t.Fatalf("getSnapshot ValidatorSet.Number mismatch found on block %v", currentBlock)
			}
			var hash types.Hash
			json.Unmarshal(controlSnapshots[i]["hash"], &hash)
			if testSnapshot.Hash != hash {
				t.Fatalf("getSnapshot ValidatorSet.Hash mismatch found on block %v", currentBlock)
			}
			var controlValidatorSet ValidatorSet
			json.Unmarshal(controlSnapshots[i]["validatorSet"], &controlValidatorSet)
			if *testSnapshot.ValidatorSet.Proposer != *controlValidatorSet.Proposer {
				t.Fatalf("getSnapshot ValidatorSet.Proposer mismatch found on block %v", currentBlock)
			}
			for j, validator := range testSnapshot.ValidatorSet.Validators {
				if *validator != *controlValidatorSet.Validators[j] {
					t.Fatalf("getSnapshot ValidatorSet.Validator mismatch found on block %v, inddex %v", currentBlock, j)
				}
			}
			if currentBlock == plugins.BlockNumber(35779968) || currentBlock == plugins.BlockNumber(35780031) || currentBlock == plugins.BlockNumber(35780032) {
				var controlRecents map[uint64]common.Address
				json.Unmarshal(controlSnapshots[i]["recents"], &controlRecents)
				for block, address := range testSnapshot.Recents {
					if address != controlRecents[block] {
						t.Fatalf("getSnapshot ValidatorSet.Recents mismatch found on block %v, key %v", currentBlock, block)
					}
				}

				//because signers are derrived there were only three of the test blocks in which they could be accurately reproduced which is why they are included in this conditional loop

				blockHash := plugins.BlockNumberOrHash{
					BlockHash: &testSnapshot.Hash,
				}
				testSigners, err := bor.GetSignersAtHash(context.Background(), blockHash)
				if err != nil {
					t.Fatalf("Error calling getSignersAtHash on block %v, err %v", currentBlock, err.Error())
				}
				hash, _ := blockHash.Hash()
				for j, signer := range testSigners {
					if signer != controlSigners[hash][j] {
						t.Fatalf("getSignersAtHash mismatch found on hash %v, index %v", hash, j)
					}
				}
			}
		})
	}

	pl, err := plugins.NewPluginLoader(cfg)
	if err != nil {
		log.Error("No PluginLoader initialized", "err", err.Error())
	}
	pl.Initialize(cfg)
	b := api.NewBlockAPI(db, 137, pl, cfg)

	testBlocks, err := blockDecompress()
	if err != nil {
		t.Fatalf("Error decompressing test blocks, err %v", err.Error())
	}

	var passThroughHashes []types.Hash

	for i, block := range passThroughBlocks {
		t.Run(fmt.Sprintf("GetBlockByNumber %v", i), func(t *testing.T) {
			shell, err := b.GetBlockByNumber(context.Background(), block, true)
			if err != nil {
				obj := *shell
				t.Fatalf("Error fetching block, getBlockByNumber, block %v, err %v", obj["number"], err.Error())
			}
			testBlock, err := GetBlockByNumber(*shell, db)
			if err != nil {
				t.Fatalf("Error engaging plugin method getBlockByNumber, block %v, err %v", testBlock["number"], err.Error())
			}
			passThroughHashes = append(passThroughHashes, testBlock["hash"].(types.Hash))
			for k, v := range testBlock {
				if k == "transactions" {
					txs := v.([]map[string]interface{})
					var blockTxs []map[string]json.RawMessage
					json.Unmarshal(testBlocks[i]["transactions"], &blockTxs)
					for j, item := range txs {
						for key, value := range item {
							d, err := json.Marshal(value)
							if err != nil {
								t.Fatalf("transaction key marshalling error on block %v tx index %v", testBlock["number"], j)
							}
							if !bytes.Equal(d, blockTxs[j][key]) {
								t.Fatalf("didnt work")
							}
						}
					}
				} else {
					data, err := json.Marshal(v)
					if err != nil {
						t.Fatalf("Error json marshalling, getBlockByNumber, block %v, key %v", testBlock["number"], k)
					}
					if !bytes.Equal(data, testBlocks[i][k]) {
						t.Fatalf("getBlockByNumber mismatch found on block %v, key %v", testBlock["number"], k)
					}
				}
			}
		})
	}

	var borTxHashes []types.Hash

	for i, hash := range passThroughHashes {
		t.Run(fmt.Sprintf("GetBlockByHash %v", i), func(t *testing.T) {
			shell, err := b.GetBlockByHash(context.Background(), hash, true)
			if err != nil {
				obj := *shell
				t.Fatalf("Error fetching block, getBlockByNumber, block %v, err %v", obj["number"], err.Error())
			}
			testBlock, err := GetBlockByHash(*shell, db)
			if err != nil {
				t.Fatalf("Error engaging plugin method getBlockByNumber, block %v, err %v", testBlock["number"], err.Error())
			}

			for k, v := range testBlock {
				if k == "transactions" {
					txs := v.([]map[string]interface{})
					borTxHashes = append(borTxHashes, txs[len(txs)-1]["hash"].(types.Hash)) // capturing bor tx hashes
					var blockTxs []map[string]json.RawMessage
					json.Unmarshal(testBlocks[i]["transactions"], &blockTxs)
					for j, item := range txs {
						for key, value := range item {
							d, err := json.Marshal(value)
							if err != nil {
								t.Fatalf("transaction key marshalling error on block %v tx index %v", testBlock["number"], j)
							}
							if !bytes.Equal(d, blockTxs[j][key]) {
								t.Fatalf("mismatch todo fix this")
							}
						}
					}
				} else {
					data, err := json.Marshal(v)
					if err != nil {
						t.Fatalf("Error json marshalling, getBlockByNumber, block %v, key %v", testBlock["number"], k)
					}
					if !bytes.Equal(data, testBlocks[i][k]) {
						t.Fatalf("getBlockByNumber mismatch found on block %v, key %v", testBlock["number"], k)
					}
				}
			}
		})

		t.Run(fmt.Sprintf("GetBorBlockReceipt %v", i), func(t *testing.T) {
			testReceipt, err := eth.GetBorBlockReceipt(context.Background(), hash)
			if err != nil {
				t.Fatalf("Error fetching block receipt, getBorBlockReceipt, hash %v, err %v", hash, err.Error())
			}
			for k, v := range *testReceipt {
				if k == "logs" {
					var controlLogs plugins.SortLogs
					json.Unmarshal(controlReceipts[i]["logs"], &controlLogs)
					for j, lg := range v.(plugins.SortLogs) {
						if lg.Address != controlLogs[j].Address {
							t.Fatalf("getBorBlockReceipt log mismatch found on hash %v, field: Address", hash)
						}
						if len(lg.Data) != len(controlLogs[j].Data) {
							t.Fatalf("getBorBlockReceipt log mismatch found on hash %v, field: Data", hash)
						}
						if lg.BlockNumber != controlLogs[j].BlockNumber {
							t.Fatalf("getBorBlockReceipt log mismatch found on hash %v, field: BlockNumber", hash)
						}
						if lg.TxHash != controlLogs[j].TxHash {
							t.Fatalf("getBorBlockReceipt log mismatch found on hash %v, field: TxHash", hash)
						}
						if lg.TxIndex != controlLogs[j].TxIndex {
							t.Fatalf("getBorBlockReceipt log mismatch found on hash %v, field: TxIndex", hash)
						}
						if lg.BlockHash != controlLogs[j].BlockHash {
							t.Fatalf("getBorBlockReceipt log mismatch found on hash %v, field: BlockHash", hash)
						}
						if lg.Index != controlLogs[j].Index {
							t.Fatalf("getBorBlockReceipt log mismatch found on hash %v, field: Index", hash)
						}
						if lg.Removed != controlLogs[j].Removed {
							t.Fatalf("getBorBlockReceipt log mismatch found on hash %v, field: Removed", hash)
						}
						for idx, topic := range lg.Topics {
							if topic != controlLogs[j].Topics[idx] {
								t.Fatalf("getBorBlockReceipt log mismatch found on hash %v, field Topics, topic %v", hash, idx)
							}
						}
					}
				} else {
					d, err := json.Marshal(v)
					if err != nil {
						t.Fatalf("json marshalling error, getBorBlockReceipt, on key %v hash %v", k, hash)
					}
					if !bytes.Equal(d, controlReceipts[i][k]) {
						t.Fatalf("value mismatch, getBorBlockReceipt, on key %v, hash %v", k, hash)
					}
				}
			}
		})
	}

	tx := api.NewTransactionAPI(db, 137, pl, cfg)

	for i, hash := range borTxHashes {
		t.Run(fmt.Sprintf("GetTransactionByHash %v", i), func(t *testing.T) {
			shell, err := tx.GetTransactionByHash(context.Background(), hash)
			if err != nil {
				t.Fatalf("Error fetching transaction, getTransactionByhash, hash %v, err %v", hash, err.Error())
			}
			testTx, err := GetTransactionByHash(*shell, hash, db)
			if err != nil {
				t.Fatalf("Error engaging plugin method getTransactionByhash, hash %v, err %v", hash, err.Error())
			}
			for k, v := range testTx {
				d, err := json.Marshal(v)
				if err != nil {
					t.Fatalf("json marshalling error, getTransactionByhash, on key %v hash %v", k, hash)
				}
				if !bytes.Equal(d, controlTxns[i][k]) {
					t.Fatalf("value mismatch, getTransactionByhash, on key %v, hash %v", k, hash)
				}
			}
		})

		t.Run(fmt.Sprintf("GetTransactionReceipt %v", i), func(t *testing.T) {
			receiptShell, err := tx.GetTransactionReceipt(context.Background(), hash)
			if err != nil {
				t.Fatalf("Error fetching receipt, getTransactionReceipt, hash %v, err %v", hash, err.Error())
			}
			testReciept, err := GetTransactionReceipt(*receiptShell, hash, db)
			if err != nil {
				t.Fatalf("Error engaging plugin method getTransactionReceipt, hash %v, err %v", hash, err.Error())
			}
			for k, v := range testReciept {
				if k == "logs" {
					var controlLogs plugins.SortLogs
					json.Unmarshal(controlTxReceipts[i]["logs"], &controlLogs)
					for j, lg := range v.(plugins.SortLogs) {
						if lg.Address != controlLogs[j].Address {
							t.Fatalf("getTransactionReceipt log mismatch found on hash %v, field: Address", hash)
						}
						if len(lg.Data) != len(controlLogs[j].Data) {
							t.Fatalf("getTransactionReceipt log mismatch found on hash %v, field: Data", hash)
						}
						if lg.BlockNumber != controlLogs[j].BlockNumber {
							t.Fatalf("getTransactionReceipt log mismatch found on hash %v, field: BlockNumber", hash)
						}
						if lg.TxHash != controlLogs[j].TxHash {
							t.Fatalf("getTransactionReceipt log mismatch found on hash %v, field: TxHash", hash)
						}
						if lg.TxIndex != controlLogs[j].TxIndex {
							t.Fatalf("getTransactionReceipt log mismatch found on hash %v, field: TxIndex", hash)
						}
						if lg.BlockHash != controlLogs[j].BlockHash {
							t.Fatalf("getTransactionReceipt log mismatch found on hash %v, field: BlockHash", hash)
						}
						if lg.Index != controlLogs[j].Index {
							t.Fatalf("getTransactionReceipt log mismatch found on hash %v, field: Index", hash)
						}
						if lg.Removed != controlLogs[j].Removed {
							t.Fatalf("getTransactionReceipt log mismatch found on hash %v, field: Removed", hash)
						}
						for idx, topic := range lg.Topics {
							if topic != controlLogs[j].Topics[idx] {
								t.Fatalf("getTransactionReceipt log mismatch found on hash %v, field Topics, topic %v", hash, idx)
							}
						}
					}
				} else {
					d, err := json.Marshal(v)
					if err != nil {
						t.Fatalf("json marshalling error, getTransactionReceipt, on key %v hash %v", k, hash)
					}
					if !bytes.Equal(d, controlTxReceipts[i][k]) {
						var generic interface{}
						json.Unmarshal(controlReceipts[i][k], &generic)
						t.Fatalf("value mismatch, getTransactionReceipt, on key %v, hash %v %v %v", k, hash, v, generic)
					}
				}
			}
		})
	}
}
