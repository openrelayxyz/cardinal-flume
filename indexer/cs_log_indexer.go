package indexer

import (
	"fmt"
	"github.com/openrelayxyz/cardinal-evm/rlp"
	"github.com/openrelayxyz/cardinal-evm/common"
	evm "github.com/openrelayxyz/cardinal-evm/types"
	"github.com/openrelayxyz/cardinal-streams/delivery"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-storage"
	"github.com/openrelayxyz/cardinal-storage/resolver"
	"github.com/RoaringBitmap/roaring/roaring64"
	log "github.com/inconshreveable/log15"
	"encoding/binary"
	"strconv"
	"os"
	"math/big"
)

type CSLogIndexer struct {
	chainid uint64
	s storage.Storage
}

func NewCSLogIndexer(chainid uint64, datadir string) Indexer {
	s, err := resolver.ResolveStorage(datadir, 128, nil)
	if err != nil {
		log.Error("Error opening current storage", "error", err, "datadir", datadir)
		if s != nil {
			s.Close()
		}
		if init, err := resolver.ResolveInitializer(datadir, false, false); err == nil {
			init.SetBlockData(
				types.HexToHash("0xFB3592B2E1143840AD336F723CD9AC532805434A7F656CE5CDFE991C144E0C2F"), 
				types.HexToHash("0x01DD02F5AA81E589DCE1E3FF90CEBEBE385E7CD143EF99303E1D6F0E829AA31F"),
				53981598, 
				new(big.Int),
			)
			init.AddData([]byte(fmt.Sprintf("bo%x", 53981598)), []byte{0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0})
			init.Close()
			s, err = resolver.ResolveStorage(datadir, 128, nil)
			if err != nil {
				log.Error("Error opening curnet storage after initializing", "err", err)
				os.Exit(1)
			}
		} else {
			log.Error("Error initializing current storage", "err", err)
			os.Exit(1)
		}
	}
	h, n, _, _ := s.LatestBlock()
	log.Info("Got storage", "h", h, "n", n)
	return &CSLogIndexer{chainid: chainid, s: s}
}

type lr struct {
	l *evm.Log
	raw []byte
}

func (indexer *CSLogIndexer) Index(pb *delivery.PendingBatch) ([]string, error) {

	logData := make(map[int64]*lr)
	topics := make(map[int]map[types.Hash][]uint64)
	addrs := make(map[common.Address][]uint64)

	for k, v := range pb.Values {
		switch {
		case logRegexp.MatchString(k):
			parts := logRegexp.FindSubmatch([]byte(k))
			txIndex, _ := strconv.ParseInt(string(parts[2]), 16, 64)
			logIndex, _ := strconv.ParseInt(string(parts[3]), 16, 64)

			logRecord := &evm.Log{}
			rlp.DecodeBytes(v, logRecord)
			logRecord.BlockNumber = uint64(pb.Number)
			logRecord.TxIndex = uint(txIndex)
			logRecord.BlockHash = types.Hash(pb.Hash)
			logRecord.Index = uint(logIndex)
			logData[int64(logIndex)] = &lr{logRecord, v}
			addrs[logRecord.Address] = append(addrs[logRecord.Address], uint64(logIndex))
			for i, v := range logRecord.Topics {
				if l, ok := topics[i]; ok {
					l[v] = append(l[v], uint64(logIndex))
				} else {
					topics[i] = make(map[types.Hash][]uint64)
					topics[i][v] = append(topics[i][v], uint64(logIndex))
				}
			}
		default:
		}
	}
	updates := []storage.KeyValue{}

	if err := indexer.s.View(pb.ParentHash, func(tr storage.Transaction) error {
		var parentOffset, parentCount uint64
		if err := tr.ZeroCopyGet([]byte(fmt.Sprintf("bo%x", pb.Number - 1)), func(data []byte) error {
			offsetBytes := data[:8]
			countBytes := data[8:16]
			parentOffset = binary.BigEndian.Uint64(offsetBytes)
			parentCount = binary.BigEndian.Uint64(countBytes)
			return nil
		}); err != nil {
			log.Warn("Failed to get key", "k", fmt.Sprintf("bo%x", pb.Number - 1))
			return err
		}
		startOffset := int64(parentOffset + parentCount)
		odata := make([]byte, 16)
		binary.BigEndian.PutUint64(odata[:8], uint64(startOffset))
		binary.BigEndian.PutUint64(odata[8:], uint64(len(logData)))
		updates = append(updates, storage.KeyValue{[]byte(fmt.Sprintf("bo%x", pb.Number)), odata})
		for i, lr := range logData {
			updates = append(updates, storage.KeyValue{[]byte(fmt.Sprintf("lv%x", startOffset + i)), lr.raw})
		}
		for tidx, m := range topics {
			for thash, idxs := range m {
				hk := fmt.Sprintf("%x%x", tidx, thash)
				if v, err := updateBM(tr, hk, uint64(startOffset), idxs); err != nil {
					return err
				} else {
					updates = append(updates, storage.KeyValue{[]byte(hk), v})
				}
			}
		}
		for addr, idxs := range addrs {
			hk := fmt.Sprintf("a%x", addr)
			if v, err := updateBM(tr, hk, uint64(startOffset), idxs); err != nil {
				return err
			} else {
				updates = append(updates, storage.KeyValue{[]byte(hk), v})
			}
		}
		return nil
	}); err != nil {
		log.Warn("Skipping block because parent is not viewable", "num", pb.Number, "parent", pb.ParentHash, "err", err)
		return nil, nil
	}

	if err := indexer.s.AddBlock(
		pb.Hash,
		pb.ParentHash,
		uint64(pb.Number),
		pb.Weight,
		updates,
		nil,
		[]byte(pb.Resumption()),
	); err != nil {
		log.Error("Error adding block", "block", pb.Hash, "parent", pb.ParentHash, "number", pb.Number, "error", err)
	}

	// statements := make([]string, 0, len(logData)+1)

	// statements = append(statements, ApplyParameters("DELETE FROM event_logs WHERE block >= %v", pb.Number))

	// for i := 0; i < len(logData); i++ {
	// 	logRecord := logData[int64(i)]
	// 	statements = append(statements, ApplyParameters(
	// 		"INSERT INTO event_logs(address,  topic0, topic1, topic2, topic3, data, block, logIndex, transactionHash, transactionIndex, blockHash) VALUES (%v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v)",
	// 		logRecord.Address,
	// 		getTopicIndex(logRecord.Topics, 0),
	// 		getTopicIndex(logRecord.Topics, 1),
	// 		getTopicIndex(logRecord.Topics, 2),
	// 		getTopicIndex(logRecord.Topics, 3),
	// 		compress(logRecord.Data),
	// 		pb.Number,
	// 		logRecord.Index,
	// 		txData[logRecord.TxIndex],
	// 		logRecord.TxIndex,
	// 		pb.Hash,
	// 	))
	// }
	return []string{}, nil
}


func updateBM(tr storage.Transaction, hk string, startOffset uint64, idxs []uint64) ([]byte, error) {
	var res []byte
	if err := tr.ZeroCopyGet([]byte(hk), func(data []byte) error {
		bm := new(roaring64.Bitmap)
		if err := bm.UnmarshalBinary(data); err != nil {
			return err
		}
		for _, idx := range idxs {
			bm.Add(uint64(startOffset) + idx)
		}
		var err error
		res, err = bm.MarshalBinary()
		if err != nil { log.Error("MB error", "err", err) }
		return err

	}); err == storage.ErrNotFound {
		bm := new(roaring64.Bitmap)
		for _, idx := range idxs {
			bm.Add(uint64(startOffset) + idx)
		}
		return bm.MarshalBinary()
	} else if err != nil {
		log.Error("ZCG error", "err", err)
		return nil, err
	}
	return res, nil
}