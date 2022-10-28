package main

import (
	"database/sql"

	"io"

	"golang.org/x/crypto/sha3"
	"regexp"
	"github.com/openrelayxyz/cardinal-evm/crypto"
	"github.com/openrelayxyz/cardinal-evm/rlp"
	"github.com/openrelayxyz/cardinal-evm/common"
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
		log.Info("pubkey error", "err", err.Error())
	}

	var signer common.Address

	copy(signer[:], crypto.Keccak256(pubkey[1:])[12:])

	return signer, nil
}
