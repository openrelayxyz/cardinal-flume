package logfeed

import (
  "context"
  "fmt"
  "strings"
  "database/sql"
  "github.com/Shopify/sarama"
  "github.com/ethereum/go-ethereum/common"
  "github.com/ethereum/go-ethereum/core"
  "github.com/ethereum/go-ethereum/core/types"
  "github.com/ethereum/go-ethereum/event"
  "github.com/ethereum/go-ethereum/replica"
  "sync/atomic"
  "time"
  "log"
)

type ethKafkaFeed struct {
  lastBlockTime *atomic.Value
  eventConsumer replica.EventConsumer
  blockFeed event.Feed
  logFeed event.Feed
  db *sql.DB
}

func NewKafkaFeed(urlStr string, db *sql.DB) (Feed, error) {
  var tableName string
  db.QueryRowContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' and name='offsets';").Scan(&tableName)
  if tableName != "offsets" {
    if _, err := db.Exec("CREATE TABLE offsets (offset BIGINT), PRIMARY KEY (offsets);"); err != nil {
      return nil, fmt.Errorf("Offsets table does not exist and could not create: %v", err.Error())
    }
    db.Exec("INSERT INTO offsets(offest) VALUES (?);", sarama.OffsetOldest)
  }
  var resumeOffset int64
  db.QueryRowContext(context.Background(), "SELECT max(offset) FROM offsets;").Scan(&resumeOffset)
  parts := strings.Split(urlStr, ";")

  consumer, err := replica.NewKafkaEventConsumerFromURLs(parts[0], parts[1], common.Hash{}, resumeOffset)
  if err != nil { return nil, err }
  feed := &ethKafkaFeed{
    lastBlockTime: &atomic.Value{},
    eventConsumer: consumer,
    db: db,
  }
  feed.subscribe()
  return feed, nil
}

func (feeder *ethKafkaFeed) subscribe() {
  logsEventCh := make(chan []*types.Log, 100)
  logsEventSub := feeder.eventConsumer.SubscribeLogsEvent(logsEventCh)
  removedLogsEventCh := make(chan core.RemovedLogsEvent, 1000)
  removedLogsEventSub := feeder.eventConsumer.SubscribeRemovedLogsEvent(removedLogsEventCh)
  chainHeadEventCh := make(chan core.ChainHeadEvent, 100)
  chainHeadEventSub := feeder.eventConsumer.SubscribeChainHeadEvent(chainHeadEventCh)
  offsetCh := make(chan int64, 100)
  offsetSub := feeder.eventConsumer.SubscribeOffsets(offsetCh)
  go func() {
    defer logsEventSub.Unsubscribe()
    defer removedLogsEventSub.Unsubscribe()
    defer chainHeadEventSub.Unsubscribe()
    defer offsetSub.Unsubscribe()
    for {
      select {
      case addLogs := <-logsEventCh:
        for _, log := range addLogs {
          feeder.logFeed.Send(log)
        }
      case removeLogs := <-removedLogsEventCh:
        for _, log := range removeLogs.Logs {
          log.Removed = true
          feeder.logFeed.Send(log)
        }
      case <-chainHeadEventCh:
        offset := int64(-1)
        OUTER:
        for {
          // When there's a chainHeadEvent, pull anything off the offset
          // channel. If we find an offset, update the database.
          select {
          case offset = <-offsetCh:
          default:
            if offset != -1 {
              feeder.db.Exec("UPDATE offsets SET offset = ? WHERE offset < ?;", offset, offset)
            }
            break OUTER
          }
        }
      }
    }
  }()
}

func (feeder *ethKafkaFeed) SubscribeLogs(ch chan types.Log) event.Subscription {
  return feeder.logFeed.Subscribe(ch)
}
func (feeder *ethKafkaFeed) Ready() chan struct{} {
  return feeder.eventConsumer.Ready()
}
func (feeder *ethKafkaFeed) Healthy(d time.Duration) bool {
  lastBlockTime, ok := feeder.lastBlockTime.Load().(time.Time)
  if !ok {
    log.Printf("ethKafkaFeed unhealthy - lastBlockTime is not a time.Time")
    return false
  } else if time.Since(lastBlockTime) > d {
    log.Printf("ethKafkaFeed unhealthy - No blocks received in timeout")
    return false
  }
  return true
}
