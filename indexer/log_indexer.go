package indexer

import (
	"regexp"
	"strconv"
	"encoding/binary"

	log "github.com/inconshreveable/log15"
	
	"github.com/openrelayxyz/cardinal-evm/crypto"
	"github.com/openrelayxyz/cardinal-evm/rlp"
	evm "github.com/openrelayxyz/cardinal-evm/types"
	"github.com/openrelayxyz/cardinal-streams/delivery"
	"github.com/openrelayxyz/cardinal-types"

	"github.com/openrelayxyz/cardinal-flume/blaster"
)

var (
	logRegexp = regexp.MustCompile("c/[0-9a-z]+/b/([0-9a-z]+)/l/([0-9a-z]+)/([0-9a-z]+)")
)

type LogIndexer struct {
	chainid uint64
	blastIdx *blaster.LogBlaster
}

func getTopicIndex(topics []types.Hash, idx int) []byte {
	if len(topics) > idx {
		return trimPrefix(topics[idx].Bytes())
	}
	return []byte{}
}

func NewLogIndexer(chainid uint64, blasterIndexer *blaster.LogBlaster) Indexer {
	return &LogIndexer{
		chainid: chainid,
		blastIdx: blasterIndexer,
	}
}

func (indexer *LogIndexer) Index(pb *delivery.PendingBatch) ([]string, error) {

	logData := make(map[int64]*evm.Log)
	txData := make(map[uint]types.Hash)

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
			logData[int64(logIndex)] = logRecord
		case txRegexp.MatchString(k):
			parts := txRegexp.FindSubmatch([]byte(k))
			txIndex, _ := strconv.ParseInt(string(parts[2]), 16, 64)
			txData[uint(txIndex)] = crypto.Keccak256Hash(v)
		default:
		}
	}

	if indexer.blastIdx != nil && pb.Number != 0 {
		_, err := indexer.batchLogIndex(pb, logData, txData)
		if err != nil {
			log.Error("Error returned from batch log index", "err", err)
			return nil, err
		}
		return nil, nil
	}

	statements := make([]string, 0, len(logData)+1)

	statements = append(statements, ApplyParameters("DELETE FROM event_logs WHERE block >= %v", pb.Number))

	for i := 0; i < len(logData); i++ {
		logRecord := logData[int64(i)]

		statements = append(statements, ApplyParameters(
			"INSERT INTO event_logs(address,  topic0, topic1, topic2, topic3, data, block, logIndex, transactionHash, transactionIndex, blockHash) VALUES (%v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v)",
			logRecord.Address,
			getTopicIndex(logRecord.Topics, 0),
			getTopicIndex(logRecord.Topics, 1),
			getTopicIndex(logRecord.Topics, 2),
			getTopicIndex(logRecord.Topics, 3),
			compress(logRecord.Data),
			pb.Number,
			logRecord.Index,
			txData[logRecord.TxIndex],
			logRecord.TxIndex,
			pb.Hash,
		))
	}
	return statements, nil
}

func (indexer *LogIndexer) batchLogIndex (pb *delivery.PendingBatch, logData map[int64]*evm.Log, txData map[uint]types.Hash) ([]string, error) {

	for i := 0; i < len(logData); i++ {
		log.Error("log idx", "i", i)

		logRecord := logData[int64(i)]

		var topicZero32Bytes [32]byte
		if t := getTopicIndex(logRecord.Topics, 0); t != nil {
			copy(topicZero32Bytes[:], t)
		} 

		var topicOne32Bytes [32]byte
		if t := getTopicIndex(logRecord.Topics, 1); t != nil {
			copy(topicOne32Bytes[:], t)
		} 

		var topicTwo32Bytes [32]byte
		if t := getTopicIndex(logRecord.Topics, 1); t != nil {
			copy(topicTwo32Bytes[:], t)
		} 

		var topicThree32Bytes [32]byte
		if t := getTopicIndex(logRecord.Topics, 3); t != nil {
			copy(topicThree32Bytes[:], t)
		} 

		var txDex [32]byte
		binary.BigEndian.PutUint32(txDex[:4], uint32(logRecord.TxIndex))
		
		var BlstLog = blaster.BlastLog{
			Block: uint64(pb.Number),
			LogIndex: uint64(logRecord.Index),
			Address: logRecord.Address,
			Topic0:	topicZero32Bytes,
			Topic1: topicOne32Bytes,
			Topic2:	topicTwo32Bytes,
			Topic3:	topicThree32Bytes,
			Data: compress(logRecord.Data),
			TransactionHash: txData[logRecord.TxIndex],
			TransactionIndex: txDex,
			BlockHash: pb.Hash,
		}
		log.Debug("calling put from within the log indexer")

		indexer.blastIdx.PutLog(BlstLog)

	}
	return nil, nil
}
