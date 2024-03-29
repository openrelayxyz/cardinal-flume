package indexer

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"

	"github.com/openrelayxyz/cardinal-evm/common"
	evm "github.com/openrelayxyz/cardinal-evm/types"
	"github.com/openrelayxyz/cardinal-rpc"
	"github.com/openrelayxyz/cardinal-streams/delivery"
	"github.com/openrelayxyz/cardinal-streams/transports"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-types/hexutil"
	"github.com/openrelayxyz/cardinal-types/metrics"
	"github.com/openrelayxyz/cardinal-flume/txfeed"

	log "github.com/inconshreveable/log15"
	"github.com/klauspost/compress/zlib"
	"math/big"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func trimPrefix(data []byte) []byte {
	if len(data) == 0 {
		return data
	}
	v := bytes.TrimLeft(data, string([]byte{0}))
	if len(v) == 0 {
		return []byte{0}
	}
	return v
}

var compressor *zlib.Writer
var compressionBuffer = bytes.NewBuffer(make([]byte, 0, 5*1024*1024))
var blockAgeTimer = metrics.NewMajorTimer("/flume/age")
var blockTime *time.Time

func compress(data []byte) []byte {
	if len(data) == 0 {
		return data
	}
	compressionBuffer.Reset()
	if compressor == nil {
		compressor = zlib.NewWriter(compressionBuffer)
	} else {
		compressor.Reset(compressionBuffer)
	}
	compressor.Write(data)
	compressor.Close()
	return compressionBuffer.Bytes()
}

func getCopy(in []byte) []byte {
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

func getFuncSig(data []byte) []byte {
	if len(data) >= 4 {
		return data[:4]
	}
	return data[:]
}

func nullZeroAddress(addr common.Address) []byte {
	if addr == (common.Address{}) {
		return []byte{}
	}
	return addr.Bytes()
}

type bytesable interface {
	Bytes() []byte
}

// applyParameters applies a set of parameters into a SQL statement in a manner
// that will be safe for execution. Note that this should only be used in the
// context of blocks, transactions, and logs - beyond the datatypes used in
// those datatypes, safety is not guaranteed.
func ApplyParameters(query string, params ...interface{}) string {
	preparedParams := make([]interface{}, len(params))
	for i, param := range params {
		switch value := param.(type) {
		case []byte:
			if len(value) == 0 {
				preparedParams[i] = "NULL"
			} else {
				preparedParams[i] = fmt.Sprintf("X'%x'", value)
			}
		case *common.Address:
			if value == nil {
				preparedParams[i] = "NULL"
				continue
			}
			b := trimPrefix(value.Bytes())
			if len(b) == 0 {
				preparedParams[i] = "NULL"
			} else {
				preparedParams[i] = fmt.Sprintf("X'%x'", b)
			}
		case *types.Hash:
			if value == nil {
				preparedParams[i] = "NULL"
				continue
			}
			b := trimPrefix(value.Bytes())
			if len(b) == 0 {
				preparedParams[i] = "NULL"
			} else {
				preparedParams[i] = fmt.Sprintf("X'%x'", b)
			}
		case common.Address:
			b := trimPrefix(value.Bytes())
			if len(b) == 0 {
				preparedParams[i] = "NULL"
			} else {
				preparedParams[i] = fmt.Sprintf("X'%x'", b)
			}
		case *big.Int:
			if value == nil {
				preparedParams[i] = "NULL"
				continue
			}
			b := trimPrefix(value.Bytes())
			if len(b) == 0 {
				preparedParams[i] = "NULL"
			} else {
				preparedParams[i] = fmt.Sprintf("X'%x'", b)
			}
		case bytesable:
			if value == nil {
				preparedParams[i] = "NULL"
				continue
			}
			b := trimPrefix(value.Bytes())
			if len(b) == 0 {
				preparedParams[i] = "NULL"
			} else {
				preparedParams[i] = fmt.Sprintf("X'%x'", b)
			}
		case hexutil.Bytes:
			if len(value) == 0 {
				preparedParams[i] = "NULL"
			} else {
				preparedParams[i] = fmt.Sprintf("X'%x'", []byte(value[:]))
			}
		case *hexutil.Big:
			if value == nil {
				preparedParams[i] = "NULL"
			} else {
				preparedParams[i] = fmt.Sprintf("X'%x'", trimPrefix(value.ToInt().Bytes()))
			}
		case hexutil.Uint64:
			preparedParams[i] = fmt.Sprintf("%v", uint64(value))
		case *uint64:
			if value == nil {
				preparedParams[i] = "NULL"
				continue
			} else { 
				preparedParams[i] = fmt.Sprintf("%v", *value)
			}
		default:
			preparedParams[i] = fmt.Sprintf("%v", value)
		}
	}
	return fmt.Sprintf(query, preparedParams...)
}

type HealthCheck struct {
	lastBlockTime time.Time
	processedCount uint
}

func (hc *HealthCheck) Healthy() rpc.HealthStatus {
	switch {
	case time.Since(hc.lastBlockTime) > 60 * time.Second:
		return rpc.Warning
	case hc.processedCount == 0:
		return rpc.Unavailable
	}
	return rpc.Healthy
}

func ProcessDataFeed(csConsumer transports.Consumer, txFeed *txfeed.TxFeed, db *sql.DB, quit <-chan struct{}, eip155Block, homesteadBlock uint64, mut *sync.RWMutex, mempoolSlots int, indexers []Indexer, hc *HealthCheck, memTxThreshold int64, rhf chan *rpc.HeightRecord, chainid uint64) {
	heightGauge := metrics.NewMajorGauge("/flume/height")
	blockTimer  := metrics.NewMajorTimer("/flume/blockProcessingTime")
	safeNumKey := fmt.Sprintf("c/%x/n/safe", chainid)
	finalizedNumKey := fmt.Sprintf("c/%x/n/finalized", chainid)
	var safeNum, finalizedNum *big.Int

	log.Info("Processing data feed")
	txCh := make(chan *evm.Transaction, 200)
	txSub := txFeed.Subscribe(txCh)
	csCh := make(chan *delivery.ChainUpdate, 10)
	if csConsumer != nil {
		csSub := csConsumer.Subscribe(csCh)
		defer csSub.Unsubscribe()
		log.Info("Starting consumer")
		csConsumer.Start()
		log.Info("Consumer started")
	}
	for _, idx := range indexers {
		log.Debug("got indexer", "indexer", idx)
	}
	processed := false
	pruneTicker := time.NewTicker(5 * time.Second)
	txDedup := make(map[types.Hash]struct{})
	defer txSub.Unsubscribe()
	db.Exec("DELETE FROM mempool.transactions WHERE 1;")
	for {
		select {
		case <-quit:
			if !processed {
				log.Error("Shutting down without processing any blocks")
				os.Exit(1)
			} else {
				log.Info("Shutting down index process")
				return
			}
		case <-pruneTicker.C:
			prune_mempool(db, mempoolSlots, txDedup, memTxThreshold)
		case tx := <-txCh:
			mempool_indexer(db, mempoolSlots, txDedup, tx)
		case chainUpdate := <-csCh:
			var lastBatch *delivery.PendingBatch
		UPDATELOOP:
			for {
				megaStatement := []string{}
				megaParameters := []interface{}{}
				// var ok bool
				for _, pb := range chainUpdate.Added() {
					if v, ok := pb.Values[safeNumKey]; ok {
						safeNum = new(big.Int).SetBytes(v)
					}
					if v, ok := pb.Values[finalizedNumKey]; ok {
						finalizedNum = new(big.Int).SetBytes(v)
					}
					for _, indexer := range indexers {
						s, err := indexer.Index(pb)
						log.Debug("inside indexer loop", "idx", indexer, "len", len(s))
						if err != nil {
							log.Error("Error computing updates", "err", err.Error())
							continue UPDATELOOP
						}
						megaStatement = append(megaStatement, s...)
					}
					lastBatch = pb
					if blockTime != nil { blockAgeTimer.UpdateSince(*blockTime) }
					resumption := pb.Resumption()
					if resumption != "" {
						tokens := strings.Split(resumption, ";")
						for _, token := range tokens {
							parts := strings.Split(token, "=")
							source, offsetS := parts[0], parts[1]
							parts = strings.Split(source, ":")
							topic, partitionS := parts[0], parts[1]
							offset, err := strconv.Atoi(offsetS)
							if err != nil {
								log.Error("offset error", "err", err.Error())
								continue
							}
							partition, err := strconv.Atoi(partitionS)
							if err != nil {
								log.Error("partition error", "err", err.Error())
								continue
							}
							megaStatement = append(megaStatement, "INSERT OR REPLACE INTO cardinal_offsets(offset, partition, topic) VALUES (?, ?, ?)")
							megaParameters = append(megaParameters, offset, partition, topic)
						}
					}
				}
				mut.Lock()
				start := time.Now()
				dbtx, err := db.BeginTx(context.Background(), nil)
				if err != nil {
					log.Error("Error creating a transaction", "err", err.Error())
					continue
				}
				if _, err := dbtx.Exec(strings.Join(megaStatement, " ; "), megaParameters...); err != nil {
					dbtx.Rollback()
					stats := db.Stats()
					log.Warn("Failed to execute statement", "err", err.Error(), "sql", strings.Join(megaStatement, " ; "))
					log.Info("SQLite Pool", "Open", stats.OpenConnections, "InUse", stats.InUse, "Idle", stats.Idle)
					mut.Unlock()
					continue
				}
				if err := dbtx.Commit(); err != nil {
					stats := db.Stats()
					log.Warn("Failed to commit", "err", err.Error())
					log.Info("SQLite Pool", "Open", stats.OpenConnections, "InUse", stats.InUse, "Idle", stats.Idle)
					mut.Unlock()
					continue
				}
				mut.Unlock()
				processed = true
				hc.lastBlockTime = time.Now()
				// add condition to check safe or finalized
				heightRecord := &rpc.HeightRecord{
					Latest: lastBatch.Number,
				}
				if safeNum != nil{
					i := safeNum.Int64()
					heightRecord.Safe = &i
				}
				if finalizedNum != nil {
					i := finalizedNum.Int64()
					heightRecord.Finalized = &i
				}
				rhf <- heightRecord
				hc.processedCount++
				heightGauge.Update(lastBatch.Number)
				blockTimer.UpdateSince(start)
				if blockTime != nil && time.Since(*blockTime) > time.Minute {
					log.Info("Committed Block", "number", uint64(lastBatch.Number), "hash", hexutil.Bytes(lastBatch.Hash.Bytes()), "in", time.Since(start), "age", time.Since(*blockTime))
					break
				}
				log.Info("Committed Block", "number", uint64(lastBatch.Number), "hash", hexutil.Bytes(lastBatch.Hash.Bytes()), "in", time.Since(start)) 
				break
			}
		}
	}
}
