package main

import (
	"context"
	"encoding/json"
	
	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-evm/common"
	"github.com/openrelayxyz/cardinal-types/metrics"
	"github.com/openrelayxyz/cardinal-types/hexutil"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-flume/plugins"
	"github.com/openrelayxyz/cardinal-flume/heavy"
)

type Snapshot struct {
	Number       uint64                    `json:"number"`       // Block number where the snapshot was created
	Hash         types.Hash               `json:"hash"`         // Block hash where the snapshot was created
	ValidatorSet *ValidatorSet      `json:"validatorSet"` // Validator set at this moment
	Recents      map[uint64]common.Address `json:"recents"`      // Set of recent signers for spam protections
}

type ValidatorSet struct {
	Validators []*Validator `json:"validators"`
	Proposer   *Validator   `json:"proposer"`
	totalVotingPower int64
	validatorsMap    map[common.Address]int // address -> index
}

type Validator struct {
	ID               uint64         `json:"ID"`
	Address          common.Address `json:"signer"`
	VotingPower      int64          `json:"power"`
	ProposerPriority int64          `json:"accum"`
}

func (service *PolygonBorService) fetchSnapshot(ctx context.Context, blockNumber uint64) (*Snapshot, error) {
	var snapshotBytes []byte

	if err := service.db.QueryRowContext(context.Background(), "SELECT snapshot FROM bor.bor_snapshots WHERE block = ?;", blockNumber).Scan(&snapshotBytes);
	err != nil {
		log.Error("sql snapshot fetch error Snapshot()", "err", err.Error())
		return nil, err
	}

	ssb, err := plugins.Decompress(snapshotBytes)
	if err != nil {
		log.Error("sql snapshot decompress error Snapshot()", "err", err.Error())
		return nil, err
	}

	var snapshot *Snapshot

	json.Unmarshal(ssb, &snapshot)


	return snapshot, nil
}

func (service *PolygonBorService) getRecents(blockNumber uint64) (map[uint64]common.Address, error) {

	recents := make(map[uint64]common.Address)

	initialBlock := blockNumber - 63
	
	rows, _ := service.db.QueryContext(context.Background(), "SELECT coinbase FROM blocks.blocks WHERE number >= ? AND number <= ?;", initialBlock, blockNumber)
	defer rows.Close()
	
	index := initialBlock
	
	for rows.Next() {
		var signer common.Address
		err := rows.Scan(&signer)
		if err != nil {
			log.Error("getRecents scan error", "err", err.Error())
			return nil, err
		}
		recents[index] = signer

		index += 1
	}

	return recents, nil 

}

var (
	bgssHitMeter  = metrics.NewMinorMeter("/flume/polygon/bgss/hit")
	bgssMissMeter = metrics.NewMinorMeter("/flume/polygon/bgss/miss")
)

func (service *PolygonBorService) GetSnapshot(ctx context.Context, blockNrOrHash plugins.BlockNumberOrHash) (*Snapshot, error) {

	number, numOk := blockNrOrHash.Number()
	blockHash, hshOk := blockNrOrHash.Hash()

	var blockNumber uint64 

	switch {
		case numOk:
			blockNumber = uint64(number)

			if err := service.db.QueryRow("SELECT hash FROM blocks WHERE number = ?", blockNumber).Scan(&blockHash); err != nil {
				log.Error("Error deriving blockHashash from blockNumber, getSnapshot()", "number", blockNumber, "err", err.Error())
				return nil, nil
			}

			offset := blockNumber % 1024
			
			requiredSnapshot := blockNumber - offset

			if len(service.cfg.HeavyServer) > 0 && requiredSnapshot < service.cfg.EarliestBlock {
				log.Debug("bor_getSnapshot sent to flume heavy")
				polygonMissMeter.Mark(1)
				bgssMissMeter.Mark(1)
				response, err := heavy.CallHeavy[*Snapshot](ctx, service.cfg.HeavyServer, "bor_getSnapshot", hexutil.Uint64(blockNumber))
				if err != nil {
					log.Error("Error calling to heavy server, getSnapshot()", "blockNumber", blockNumber, "err", err.Error())
					return nil, nil
				}
				return *response, nil
			}

			if len(service.cfg.HeavyServer) > 0 {
				log.Debug("bor_getSnapshot served from flume light")
				polygonHitMeter.Mark(1)
				bgssHitMeter.Mark(1)
			}

		case hshOk:
			var present int
			service.db.QueryRow("SELECT 1 FROM blocks WHERE hash = ?;", plugins.TrimPrefix(blockHash.Bytes())).Scan(&present)
			
			if len(service.cfg.HeavyServer) > 0 && present == 0 {
				log.Debug("bor_getSnapshot sent to flume heavy")
				polygonMissMeter.Mark(1)
				bgssMissMeter.Mark(1)
				response, err := heavy.CallHeavy[*Snapshot](ctx, service.cfg.HeavyServer, "bor_getSnapshot", blockHash)
				if err != nil {
					log.Error("Error calling to heavy server, getSnapshot()", "blockHash", blockHash, "err", err.Error())
					return nil, nil
				}
				return *response, nil
			}

			if err := service.db.QueryRow("SELECT number FROM blocks WHERE hash = ?;", plugins.TrimPrefix(blockHash.Bytes())).Scan(&blockNumber); err != nil {
				log.Error("Error deriving blockNumber from blockHash, getSnapshot()", "hash", blockHash, "err", err.Error())
				return nil, nil
			}
			
			if len(service.cfg.HeavyServer) > 0 {
				log.Debug("bor_getSnapshot served from flume light")
				polygonHitMeter.Mark(1)
				bgssHitMeter.Mark(1)
			}

		default:
			log.Error("Error deriving input, getSnapshot")
			return nil, nil
	}

	log.Debug("getSnapshot() intial block value", "blockNumber", blockNumber)

	recents, err := service.getRecents(blockNumber)
	if err != nil {
		log.Error("Error getting recents get_snapshot()", "err", err.Error())
	}

	if blockNumber % 64 == 0 {
		snap := &Snapshot{}
		snap, err = service.fetchSnapshot(ctx, blockNumber)	
		if err != nil {
			log.Error("Error fetching snapshot get_snapshot(), mod 64 condition", "err", err.Error())
		}
		return snap, nil
	} else {
		snap := &Snapshot{}
		previousSnapshot := blockNumber - (blockNumber % 64)
		snap, _ = service.fetchSnapshot(ctx, previousSnapshot)
		if err != nil {
			log.Error("Error fetching snapshot get_snapshot() mod 64 != 0 condition", "err", err.Error())
		}
		snap.Number = blockNumber
		snap.Hash = blockHash
		snap.Recents = recents
		return snap, nil
	}

}
