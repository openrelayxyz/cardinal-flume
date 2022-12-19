package indexer

import (
	"database/sql"

	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-evm/rlp"
	evm "github.com/openrelayxyz/cardinal-evm/types"
	"github.com/openrelayxyz/cardinal-types"
	"strings"
	"time"
)

func mempool_dropLowestPrice(db *sql.DB, mempoolSlots int, txDedup map[types.Hash]struct{}) {
	var txCount int
	db.QueryRow("SELECT count(*) FROM mempool.transactions;").Scan(&txCount)
	if txCount > mempoolSlots {
		pstart := time.Now()
		if _, err := db.Exec("DELETE FROM mempool.transactions WHERE gasPrice < (SELECT gasPrice FROM mempool.transactions ORDER BY gasPrice DESC LIMIT 1 OFFSET ?);", mempoolSlots); err != nil {
			log.Error("Error pruning", "err", err.Error())
		}
		log.Debug("Pruned transactions from mempool", "transaction count", (txCount - mempoolSlots), "time", time.Since(pstart))
	}
}

func mempool_indexer(db *sql.DB, mempoolSlots int, txDedup map[types.Hash]struct{}, tx *evm.Transaction) []string {
	txHash := tx.Hash()
	if _, ok := txDedup[txHash]; ok {
		return []string{}
	}
	var signer evm.Signer
	var accessListRLP []byte
	gasPrice := tx.GasPrice().Uint64()
	switch {
	case tx.Type() == evm.AccessListTxType:
		accessListRLP, _ = rlp.EncodeToBytes(tx.AccessList())
		signer = evm.NewEIP2930Signer(tx.ChainId())
	case tx.Type() == evm.DynamicFeeTxType:
		signer = evm.NewLondonSigner(tx.ChainId())
		accessListRLP, _ = rlp.EncodeToBytes(tx.AccessList())
		gasPrice = tx.GasFeeCap().Uint64()
	default:
		signer = evm.NewEIP155Signer(tx.ChainId())
	}
	sender, _ := evm.Sender(signer, tx)
	var to []byte
	if tx.To() != nil {
		to = trimPrefix(tx.To().Bytes())
	}
	v, r, s := tx.RawSignatureValues()
	statements := []string{}
	// If this is a replacement transaction, delete any it might be replacing
	statements = append(statements, ApplyParameters(
		"DELETE FROM mempool.transactions WHERE sender = %v AND nonce = %v",
		sender,
		tx.Nonce(),
	))
	// Insert the transaction
	statements = append(statements, ApplyParameters(
		"INSERT INTO mempool.transactions(gas, gasPrice, hash, input, nonce, recipient, `value`, v, r, s, sender, `type`, access_list, gasFeeCap, gasTipCap) VALUES (%v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v)",
		tx.Gas(),
		gasPrice,
		txHash,
		getCopy(compress(tx.Data())),
		tx.Nonce(),
		to,
		trimPrefix(tx.Value().Bytes()),
		v.Int64(),
		r,
		s,
		sender,
		tx.Type(),
		compress(accessListRLP),
		trimPrefix(tx.GasFeeCap().Bytes()),
		trimPrefix(tx.GasTipCap().Bytes()),
	))
	// Delete the transaction we just inserted if the confirmed transactions
	// pool has a conflicting entry
	statements = append(statements, ApplyParameters(
		"DELETE FROM mempool.transactions WHERE sender = %v AND nonce = %v AND (sender, nonce) IN (SELECT sender, nonce FROM transactions)",
		sender,
		tx.Nonce(),
	))
	if _, err := db.Exec(strings.Join(statements, " ; ") + ";"); err != nil {
		log.Error("Error on insert:", strings.Join(statements, " ; "), "err", err.Error())
		return []string{}
	}
	txDedup[txHash] = struct{}{}

	return statements
}
