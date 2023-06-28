package indexer

import (
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"context"
	"database/sql"

	log "github.com/inconshreveable/log15"

	evm "github.com/openrelayxyz/cardinal-evm/types"
	"github.com/openrelayxyz/cardinal-streams/delivery"
	"github.com/openrelayxyz/cardinal-streams/transports"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-types/hexutil"
	"github.com/openrelayxyz/cardinal-types/metrics"

	"github.com/openrelayxyz/cardinal-flume/txfeed"
)

func ProcessDataFeed(csConsumer transports.Consumer, txFeed *txfeed.TxFeed, db *sql.DB, quit <-chan struct{}, eip155Block, homesteadBlock uint64, mut *sync.RWMutex, mempoolSlots int, indexers []Indexer, hc *HealthCheck, memTxThreshold int64, rhf chan int64) {
	heightGauge := metrics.NewMajorGauge("/flume/height")
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
				for _, pb := range chainUpdate.Added() {
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
				// log.Printf("Spent %v on %v inserts", time.Since(istart), len(statements))
				// cstart := time.Now()
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
				rhf <- lastBatch.Number
				hc.processedCount++
				heightGauge.Update(lastBatch.Number)
				// completionFeed.Send(chainEvent.Block.Hash)
				// log.Printf("Spent %v on commit", time.Since(cstart))
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
