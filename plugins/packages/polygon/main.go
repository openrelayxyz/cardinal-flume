package main

import (
	"database/sql"

	"io"

	"golang.org/x/crypto/sha3"

	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-evm/crypto"
	"github.com/openrelayxyz/cardinal-evm/rlp"
	"github.com/openrelayxyz/cardinal-evm/common"
	"github.com/openrelayxyz/cardinal-types"
	evm "github.com/openrelayxyz/cardinal-evm/types"
	"github.com/openrelayxyz/cardinal-types/metrics"

	"github.com/openrelayxyz/flume/config"
	"github.com/openrelayxyz/flume/indexer"
	"github.com/openrelayxyz/flume/plugins"
	rpcTransports "github.com/openrelayxyz/cardinal-rpc/transports"
)

func Initialize(cfg *config.Config, pl *plugins.PluginLoader) {
	log.Info("Polygon plugin loaded")
}

func Indexer(cfg *config.Config) indexer.Indexer {
	return &PolygonIndexer{Chainid: cfg.Chainid}
}

func RegisterAPI(tm *rpcTransports.TransportManager, db *sql.DB, cfg *config.Config) error {
	tm.Register("eth", &PolygonEthService{
			db: db,
			cfg: cfg,
	})
	log.Info("PolygonService registered")
	tm.Register("bor", &PolygonBorService{
		db: db,
		cfg: cfg,
	})
	log.Info("PolygonBorService registered")
	return nil	
} 

func sealHash(header *evm.Header) (hash types.Hash) {
	hasher := sha3.NewLegacyKeccak256()
	encodeSigHeader(hasher, header)
	hasher.Sum(hash[:0])

	return hash
}

func encodeSigHeader(w io.Writer, header *evm.Header) {
	enc := []interface{}{
		header.ParentHash,
		header.UncleHash,
		header.Coinbase,
		header.Root,
		header.TxHash,
		header.ReceiptHash,
		header.Bloom,
		header.Difficulty,
		header.Number,
		header.GasLimit,
		header.GasUsed,
		header.Time,
		header.Extra[:len(header.Extra)-65], // Yes, this will panic if extra is too short
		header.MixDigest,
		header.Nonce,
	}

	// if c.IsJaipur(header.Number.Uint64()) {
	if header.BaseFee != nil {
		enc = append(enc, header.BaseFee)
	}
	// }

	if err := rlp.Encode(w, enc); err != nil {
		panic("can't encode: " + err.Error())
	}
}

func getBlockAuthor(header *evm.Header) (common.Address, error) {

	signature := header.Extra[len(header.Extra)-65:]

	pubkey, err := crypto.Ecrecover(sealHash(header).Bytes(), signature)
	if err != nil {
		log.Info("pubkey error", "err", err.Error())
	}

	var signer common.Address

	copy(signer[:], crypto.Keccak256(pubkey[1:])[12:])

	return signer, nil
}

func borBlockDataPresent(input interface{}, cfg *config.Config, db *sql.DB) bool {
	present := true
	switch input.(type) {
	case BlockNumber:
		if uint64(input.(BlockNumber).Int64()) < cfg.EarliestBlock {
			present = false
			return present
		}
	case types.Hash:
		blockHash := input.(types.Hash)
		var response int
		statement := "SELECT 1 FROM bor.bor_logs WHERE blockHash = ?;"
		db.QueryRow(statement, trimPrefix(blockHash.Bytes())).Scan(&response)
		if response == 0 {
			present = false
			return present
		}
	}
	return present
}

func borTxDataPresent(txHash types.Hash, cfg *config.Config, db *sql.DB) bool {
	present := true
	var response int
	txStatement := "SELECT 1 FROM bor.bor_logs WHERE transactionHash = ?;"
	db.QueryRow(txStatement, trimPrefix(txHash.Bytes())).Scan(&response)
	if response == 0 {
		present = false
		return present
	}
	mpStatement := "SELECT 1 FROM mempool.transactions WHERE hash = ?;"
	db.QueryRow(mpStatement, trimPrefix(txHash.Bytes())).Scan(&response)
	if response == 0 {
		present = false
		return present
	}
	return present
}


