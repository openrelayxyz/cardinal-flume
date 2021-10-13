package txfeed

import (
	"fmt"
	"github.com/openrelayxyz/cardinal-streams/utils"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/event"
	"log"
	"strings"
)

type TxFeed struct {
	feed event.Feed
}

func (f *TxFeed) Subscribe(ch chan *types.Transaction) event.Subscription {
	return f.feed.Subscribe(ch)
}

func (f *TxFeed) start(ch chan *types.Transaction) {
	go func(){
		for item := range ch {
			f.feed.Send(item)
		}
	}()
}

func ResolveTransactionFeed(feedURL, topic string) (*TxFeed, error) {
	feedURL = strings.TrimPrefix(feedURL, "cardinal://")
	if strings.HasPrefix(feedURL, "ws://") || strings.HasPrefix(feedURL, "wss://") {
		return nil, fmt.Errorf("transactions are not currently supported with websockets")
  } else if strings.HasPrefix(feedURL, "kafka://") {
    return KafkaTxFeed(feedURL, topic)
  }
	return &TxFeed{}, nil

}


func KafkaTxFeed(brokerURL, topic string) (*TxFeed, error) {
	ch := make(chan *types.Transaction, 200)
	tc, err := utils.NewTopicConsumer(brokerURL, topic, 200)
	if err != nil { return nil, err }
	go func() {
		for msg := range tc.Messages() {
			transaction := &types.Transaction{}
			if err := rlp.DecodeBytes(msg.Value, transaction); err != nil {
				log.Printf("Failed to decode message: %v", err.Error())
				continue
			}
			ch <- transaction
		}
	}()
	txFeed := &TxFeed{}
	txFeed.start(ch)
	return txFeed, nil
}
