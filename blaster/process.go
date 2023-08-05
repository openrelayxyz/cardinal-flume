package blaster

import (

	log "github.com/inconshreveable/log15"

	"github.com/openrelayxyz/cardinal-streams/delivery"
	"github.com/openrelayxyz/cardinal-streams/transports"
)

var dummyList = []string{"it", "is", "too", "hot", "in", "here"}

func BatchIndex(csConsumer transports.Consumer, quit <-chan struct{}) {
	log.Error("BBBBBBBBBBBBBAAAAAAAAAAAAAATTTTTTTTTTTTCCCCCCCCCCCHHHHHHHHHHHH")
	csCh := make(chan *delivery.ChainUpdate, 10)
	for {
		select {
		case <-quit:
				log.Info("Shutting down batch index process")
				return 
		case chainUpdate := <-csCh:
			log.Error("CCCCCCCCCHHHHHHHHHHAAAAAAAAAAIIIIIIIINNNNNNN")
			for {
				for _, pb := range chainUpdate.Added() {
					// for _, item := range dummyList {
					// 	log.Debug("inside indexer loop", "idx", item, "len")
					// }
					log.Info("batch index block", "number", pb.Number)
				}
			}
		}
	}
}