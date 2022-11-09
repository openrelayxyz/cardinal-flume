package main

import (
	"database/sql"
	"io"
	"golang.org/x/crypto/sha3"
	"regexp"
	"github.com/openrelayxyz/cardinal-evm/crypto"
	"github.com/openrelayxyz/cardinal-evm/rlp"
	"github.com/openrelayxyz/cardinal-evm/common"
	"github.com/openrelayxyz/cardinal-types/metrics"
	"github.com/openrelayxyz/cardinal-types"
	evm "github.com/openrelayxyz/cardinal-evm/types"
	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/flume/config"
	"github.com/openrelayxyz/flume/indexer"
	"github.com/openrelayxyz/flume/plugins"
	rpcTransports "github.com/openrelayxyz/cardinal-rpc/transports"
)

var TrackedPrefixes = []*regexp.Regexp{
	regexp.MustCompile("c/[0-9a-z]+/b/[0-9a-z]+/br/"),
	regexp.MustCompile("c/[0-9a-z]+/b/[0-9a-z]+/bl/"),
	regexp.MustCompile("c/[0-9a-z]+/b/[0-9a-z]+/bs"),
}

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
		log.Error("pubkey error", "err", err.Error())
	}

	var signer common.Address

	copy(signer[:], crypto.Keccak256(pubkey[1:])[12:])

	return signer, nil
}

var (
	polygonHitMeter  = metrics.NewMajorMeter("/flume/polygon/hit")
	polygonMissMeter = metrics.NewMajorMeter("/flume/polygon/miss")
)

func borBlockDataPresent(input interface{}, cfg *config.Config, db *sql.DB) bool {
	present := true
	switch input.(type) {
	case plugins.BlockNumber:
		if uint64(input.(plugins.BlockNumber).Int64()) < cfg.EarliestBlock {
			present = false
			return present
		}
	case types.Hash:
		blockHash := input.(types.Hash)
		var response int
		statement := "SELECT 1 FROM blocks.blocks WHERE hash = ?;"
		db.QueryRow(statement, plugins.TrimPrefix(blockHash.Bytes())).Scan(&response)
		if response == 0 {
			present = false
			return present
		}
	}
	return present
}

func snapshotHashPresent(hash types.Hash, cfg *config.Config, db *sql.DB) plugins.BlockNumber {

	var currentBlock uint64 

	db.QueryRow("SELECT number FROM blocks.blocks WHERE hash = ?;", plugins.TrimPrefix(hash.Bytes())).Scan(&currentBlock)

	offset := currentBlock % 1024
	requiredSnapshot := currentBlock - offset

	return plugins.BlockNumber(requiredSnapshot)

}