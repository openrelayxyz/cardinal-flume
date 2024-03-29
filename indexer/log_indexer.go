package indexer

import (
	"github.com/openrelayxyz/cardinal-evm/crypto"
	"github.com/openrelayxyz/cardinal-evm/rlp"
	evm "github.com/openrelayxyz/cardinal-evm/types"
	"github.com/openrelayxyz/cardinal-streams/delivery"
	"github.com/openrelayxyz/cardinal-types"
	"regexp"
	"strconv"
)

var (
	logRegexp = regexp.MustCompile("c/[0-9a-z]+/b/([0-9a-z]+)/l/([0-9a-z]+)/([0-9a-z]+)")
)

type LogIndexer struct {
	chainid uint64
}

func getTopicIndex(topics []types.Hash, idx int) []byte {
	if len(topics) > idx {
		return trimPrefix(topics[idx].Bytes())
	}
	return []byte{}
}

func NewLogIndexer(chainid uint64) Indexer {
	return &LogIndexer{chainid: chainid}
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
