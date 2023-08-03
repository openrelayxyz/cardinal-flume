package indexer
// #cgo CXXFLAGS: -std=c++11
// #cgo LDFLAGS: -lstdc++
import "C"

import (
	"fmt"
	"math/big"
	"regexp"
	"strconv"

	log "github.com/inconshreveable/log15"

	"github.com/openrelayxyz/cardinal-evm/common"
	"github.com/openrelayxyz/cardinal-evm/common/math"
	"github.com/openrelayxyz/cardinal-evm/rlp"
	evm "github.com/openrelayxyz/cardinal-evm/types"
	"github.com/openrelayxyz/cardinal-streams/delivery"
	"github.com/openrelayxyz/cardinal-types"

	"github.com/openrelayxyz/cardinal-flume/blaster"
)

var (
	receiptRegexp = regexp.MustCompile("c/[0-9a-z]+/b/([0-9a-z]+)/r/([0-9a-z]+)")
)

type cardinalReceiptMeta struct {
	ContractAddress   common.Address
	CumulativeGasUsed uint64
	GasUsed           uint64
	LogsBloom         []byte
	Status            uint64
	LogCount          uint
	LogOffset         uint
}

type TxIndexer struct {
	chainid        uint64
	eip155Block    uint64
	homesteadBlock uint64
	hasMempool     bool
	blastIdx *blaster.Blaster
}

func NewTxIndexer(chainid, eip155block, homesteadblock uint64, hasMempool bool, blasterIndexer *blaster.Blaster) Indexer {
	return &TxIndexer{
		chainid:        chainid,
		eip155Block:    eip155block,
		homesteadBlock: homesteadblock,
		hasMempool: hasMempool,
		blastIdx: blasterIndexer,
	}
}

func (indexer *TxIndexer) Index(pb *delivery.PendingBatch) ([]string, error) {
	headerBytes := pb.Values[fmt.Sprintf("c/%x/b/%x/h", indexer.chainid, pb.Hash.Bytes())]
	header := &evm.Header{}
	if err := rlp.DecodeBytes(headerBytes, &header); err != nil {
		panic(err.Error())
	}

	receiptData := make(map[int]*cardinalReceiptMeta)
	txData := make(map[int]*evm.Transaction)
	senderMap := make(map[types.Hash]<-chan common.Address)

	for k, v := range pb.Values {
		switch {
		case txRegexp.MatchString(k):
			parts := txRegexp.FindSubmatch([]byte(k))
			txIndex, _ := strconv.ParseInt(string(parts[2]), 16, 64)
			tx := &evm.Transaction{}
			tx.UnmarshalBinary(v)

			var signer evm.Signer
			ch := make(chan common.Address, 1)
			senderMap[tx.Hash()] = ch
			go func(tx *evm.Transaction, ch chan<- common.Address) {
				switch {
				case tx.Type() == evm.AccessListTxType:
					signer = evm.NewEIP2930Signer(tx.ChainId())
				case tx.Type() == evm.DynamicFeeTxType:
					signer = evm.NewLondonSigner(tx.ChainId())
				case uint64(pb.Number) > indexer.eip155Block:
					signer = evm.NewEIP155Signer(tx.ChainId())
				case uint64(pb.Number) > indexer.homesteadBlock:
					signer = evm.HomesteadSigner{}
				default:
					signer = evm.FrontierSigner{}
				}
				sender, err := evm.Sender(signer, tx)
				if err != nil {
					log.Error("Signer error", "err", err.Error())
				}
				ch <- sender
			}(tx, ch)

			txData[int(txIndex)] = tx
		case receiptRegexp.MatchString(k):
			parts := receiptRegexp.FindSubmatch([]byte(k))
			txIndex, _ := strconv.ParseInt(string(parts[2]), 16, 64)
			rmeta := &cardinalReceiptMeta{}
			rlp.DecodeBytes(v, rmeta)
			receiptData[int(txIndex)] = rmeta
		default:
		}
	}

	statements := make([]string, 0, len(txData)+1)

	statements = append(statements, ApplyParameters("DELETE FROM transactions.transactions WHERE block >= %v", pb.Number))

	for i := 0; i < len(txData); i++ {
		transaction := txData[int(i)]
		receipt := receiptData[int(i)]
		sender := <-senderMap[transaction.Hash()]
		v, r, s := transaction.RawSignatureValues()

		var accessListRLP []byte
		gasPrice := transaction.GasPrice().Uint64()
		switch transaction.Type() {
		case evm.AccessListTxType:
			accessListRLP, _ = rlp.EncodeToBytes(transaction.AccessList())
		case evm.DynamicFeeTxType:
			accessListRLP, _ = rlp.EncodeToBytes(transaction.AccessList())
			gasPrice = math.BigMin(new(big.Int).Add(transaction.GasTipCap(), header.BaseFee), transaction.GasFeeCap()).Uint64()
		}
		input := getCopy(compress(transaction.Data()))

		if indexer.blastIdx != nil && pb.Number != 0 {

			var BlstTx = blaster.BlastTx{
				Gas: uint64(transaction.Gas()),
				GasPrice: uint64(gasPrice),
				Hash: [32]byte(transaction.Hash()),
				Input: []byte(input)
				Nonce: uint64(transaction.Nonce()),
				Recipient: [20]byte(transaction.To()),
				TransactionIndex: uint64(i),
				Value: []byte(transactio.Value().Bytes()), // may be a problem
				V: uint64(v.Int64()),
				R: [32]byte(r),
				S: [32]byte(s),
				Sender: [20]byte(sender),
				Func: [4]byte(getFuncSig(transaction.Data())),
				ContractAddress: []byte(nullZeroAddress(receipt.ContractAddress)),
				CumulativeGasUsed: uint64(receipt.CumulativeGasUsed),
				GasUsed: uint64(receipt.GasUsed),
				LogsBloom: []byte(getCopy(compress(receipt.LogsBloom))),
				Status: uint64(receipt.Status),
				Block: Uint64(pb.Number),
				Type: uint64(transaction.Type()),
				Accesslist: []byte(compress(accessListRLP)),
				GasFeeCap: []byte(transaction.GasFeeCap()),
				GasTipCap: []byte(transaction.GasTipCap()),
			}
			log.Error("calling put from within the tx indexer")
			indexer.blastIdx.PutTx(BlstTx)
			return nil, nil
	
		}


		statements = append(statements, ApplyParameters(
			"INSERT INTO transactions.transactions(block, gas, gasPrice, hash, input, nonce, recipient, transactionIndex, `value`, v, r, s, sender, func, contractAddress, cumulativeGasUsed, gasUsed, logsBloom, `status`, `type`, access_list, gasFeeCap, gasTipCap) VALUES (%v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v)",
			pb.Number,
			transaction.Gas(),
			gasPrice,
			transaction.Hash(),
			input,
			transaction.Nonce(),
			transaction.To(),
			uint(i),
			trimPrefix(transaction.Value().Bytes()),
			v.Int64(),
			r,
			s,
			sender,
			getFuncSig(transaction.Data()),
			nullZeroAddress(receipt.ContractAddress),
			receipt.CumulativeGasUsed,
			receipt.GasUsed,
			getCopy(compress(receipt.LogsBloom)),
			receipt.Status,
			transaction.Type(),
			compress(accessListRLP),
			trimPrefix(transaction.GasFeeCap().Bytes()),
			trimPrefix(transaction.GasTipCap().Bytes()),
		))
		if indexer.hasMempool {
			statements = append(statements, ApplyParameters(
				"DELETE FROM mempool.transactions WHERE sender = %v AND nonce = %v AND (sender, nonce) IN (SELECT sender, nonce FROM transactions.transactions INDEXED BY senderNonce)",
				sender,
				transaction.Nonce(),
			))
		}
	}
	return statements, nil
}
