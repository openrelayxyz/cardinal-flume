package txfeed

import (
	"fmt"

	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-evm/rlp"
	evm "github.com/openrelayxyz/cardinal-evm/types"
	"github.com/openrelayxyz/cardinal-streams/utils"
	"github.com/openrelayxyz/cardinal-types"
	"strings"
)

type TxFeed struct {
	feed types.Feed
}

func (f *TxFeed) Subscribe(ch chan *evm.Transaction) types.Subscription {
	return f.feed.Subscribe(ch)
}

func (f *TxFeed) start(ch chan *evm.Transaction) {
	go func() {
		for item := range ch {
			f.feed.Send(item)
		}
	}()
}

func ResolveTransactionFeed(feedURL, topic string) (*TxFeed, error) {
	feedURL = strings.TrimPrefix(feedURL, "cardinal://")
	feedURL = strings.Split(feedURL, ";")[0]
	if topic == "" {
		return &TxFeed{}, nil
	} else if strings.HasPrefix(feedURL, "ws://") || strings.HasPrefix(feedURL, "wss://") {
		return nil, fmt.Errorf("transactions are not currently supported with websockets")
	} else if strings.HasPrefix(feedURL, "kafka://") {
		return KafkaTxFeed(feedURL, topic)
	}
	return &TxFeed{}, nil

}

func KafkaTxFeed(brokerURL, topic string) (*TxFeed, error) {
	ch := make(chan *evm.Transaction, 200)
	tc, err := utils.NewTopicConsumer(strings.TrimPrefix(brokerURL, "kafka://"), topic, 200)
	if err != nil {
		return nil, err
	}
	go func() {
		log.Info("Starting kafka feed", "broker:", brokerURL, "topic", topic)
		for msg := range tc.Messages() {
			transaction := &evm.Transaction{}
			if err := rlp.DecodeBytes(msg.Value, transaction); err != nil {
				log.Error("Failed to decode message", "err", err.Error())
				continue
			}
			ch <- transaction
		}
	}()
	txFeed := &TxFeed{}
	txFeed.start(ch)
	return txFeed, nil
}
