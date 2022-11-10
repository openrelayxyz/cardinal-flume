package main

import (
	"context"
	"encoding/json"
	"math/big"
	"errors"

	
	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-evm/common"
	"github.com/openrelayxyz/cardinal-types/metrics"
	"github.com/openrelayxyz/cardinal-types/hexutil"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/flume/plugins"
	"github.com/openrelayxyz/flume/heavy"
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

var (
	extraVanity = 32 // Fixed number of extra-data prefix bytes reserved for signer vanity
	extraSeal   = 65 // Fixed number of extra-data suffix bytes reserved for signer seal
)

func NewValidator(address common.Address, votingPower int64) *Validator {
	return &Validator{
		Address:          address,
		VotingPower:      votingPower,
		ProposerPriority: 0,
	}
}

func (service *PolygonBorService) fetchSnapshot(ctx context.Context, blockNumber uint64) (*Snapshot, error) {
	var snapshotBytes []byte

	if err := service.db.QueryRowContext(context.Background(), "SELECT snapshot FROM bor.bor_snapshots WHERE block = ?;", blockNumber).Scan(&snapshotBytes);
	err != nil {
		log.Error("sql snapshot fetch error Snapshot()", "err", err)
		return nil, err
	}

	ssb, err := plugins.Decompress(snapshotBytes)
	if err != nil {
		log.Error("sql snapshot decompress error Snapshot()", "err", err)
		return nil, err
	}

	var snapshot *Snapshot

	json.Unmarshal(ssb, &snapshot)


	return snapshot, nil
}

func (service *PolygonBorService) getPreviousSnapshot(blockNumber uint64) (*Snapshot, error) { 
	var lastSnapBlock uint64

	if err := service.db.QueryRowContext(context.Background(), "SELECT block FROM bor_snapshots WHERE block < ? ORDER BY block DESC LIMIT 1;", blockNumber).Scan(&lastSnapBlock);
	err != nil {
		log.Error("sql previous snapshot fetch error", "err", err)
		return nil, err
	}

	snap, err := service.fetchSnapshot(context.Background(), lastSnapBlock)
	if err != nil {
		log.Error("error fetching previous snapshot", "number", snap.Number)
		return nil, err
	}

	return snap, nil
}

func (service *PolygonBorService) getSubsequentSnapshot(blockNumber uint64) (*Snapshot, error) { 
	var nextSnapBlock uint64

	if err := service.db.QueryRowContext(context.Background(), "SELECT block FROM bor_snapshots WHERE block > ? ORDER BY block ASC LIMIT 1;", blockNumber).Scan(&nextSnapBlock);
	err != nil {
		log.Error("sql subsequent snapshot fetch error", "err", err)
		return nil, err
	}

	snap, err := service.fetchSnapshot(context.Background(), nextSnapBlock)
	if err != nil {
		log.Error("error fetching subsequent snapshot", "number", snap.Number)
		return nil, err
	}

	return snap, nil
}

func (service *PolygonBorService) getKeyFrame(blockNumber uint64, degree int64) (*Snapshot, error) {

	frames := make([]*Snapshot, degree)

	vals, err := service.getVals(blockNumber)
	if err != nil {
		log.Error("getKeyFrame, getVals error", "err", err.Error())
		return nil, err
	}
	
	for i := 0; i < int(degree); i++ {
		snap := &Snapshot{}
		vs := &ValidatorSet{
			Validators: vals,
		}
		snap.ValidatorSet = vs
		frames[i] = snap
	}
	
	updatedFrames := service.updateValidators(blockNumber, frames)
	
	return updatedFrames[len(updatedFrames) - 1], nil
}

func (service *PolygonBorService) getVals(number uint64) ([]*Validator, error) { 
	
	var extra []byte 

	if err := service.db.QueryRowContext(context.Background(), "SELECT extra FROM blocks.blocks WHERE number = ?;", number).Scan(&extra);
	err != nil {
		log.Error("sql keyframe extra fetch error", "err", err)
		return nil, err
	}

	validatorBytes := extra[extraVanity : len(extra)-extraSeal]

	newVals, err := parseValidators(validatorBytes)
	if err != nil {
		log.Error("getVals parse validators error", "err", err)
		return nil, err
	}

	var vals []*Validator

	vals = newVals

	return vals, nil
	
}

func parseValidators(validatorsBytes []byte) ([]*Validator, error) {
	if len(validatorsBytes)%40 != 0 {
		log.Error("Invalid validator bytes")
		return nil, errors.New("Invalid validators bytes")
	}

	result := make([]*Validator, len(validatorsBytes)/40)

	for i := 0; i < len(validatorsBytes); i += 40 {
		address := make([]byte, 20)
		power := make([]byte, 20)

		copy(address, validatorsBytes[i:i+20])
		copy(power, validatorsBytes[i+20:i+40])

		result[i/40] = NewValidator(plugins.BytesToAddress(address), big.NewInt(0).SetBytes(power).Int64())
	}

	return result, nil
}

func (service *PolygonBorService) updateValidators(blockNumber uint64, snaps []*Snapshot) []*Snapshot {

	previousSnap, _ := service.getPreviousSnapshot(blockNumber)
	
	expandedFrames := []*Snapshot{previousSnap}

	expandedFrames = append(expandedFrames, snaps[:]...)

	for i := 1; i <= len(snaps); i++ {

		votingPowerSum := int64(0)

		maxPriority := int64(0)

		var maxPriorityIndex int

		for j, val := range expandedFrames[i].ValidatorSet.Validators {

			votingPowerSum += val.VotingPower

			previousVal := expandedFrames[i - 1].ValidatorSet.Validators[j]
			
			val.ProposerPriority = previousVal.VotingPower + previousVal.ProposerPriority 
			
			if val.ProposerPriority > maxPriority {
				maxPriority = val.ProposerPriority
				maxPriorityIndex = j
			}
		}

		expandedFrames[i].ValidatorSet.Proposer = expandedFrames[i].ValidatorSet.Validators[maxPriorityIndex]
		expandedFrames[i].ValidatorSet.Proposer.ProposerPriority = expandedFrames[i].ValidatorSet.Proposer.ProposerPriority - votingPowerSum
	}

	return expandedFrames
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
				log.Error("Error deriving blockHashash from blockNumber, getSnapshot()", "number", blockNumber)
			}

			offset := blockNumber % 1024
			
			requiredSnapshot := blockNumber - offset

			if len(service.cfg.HeavyServer) > 0 && requiredSnapshot < service.cfg.EarliestBlock {
				log.Debug("bor_getSnapshot sent to flume heavy")
				polygonMissMeter.Mark(1)
				bgssMissMeter.Mark(1)
				response, err := heavy.CallHeavy[*Snapshot](ctx, service.cfg.HeavyServer, "bor_getSnapshot", hexutil.Uint64(blockNumber))
				if err != nil {
					return nil, err
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
					return nil, err
				}
				return *response, nil
			}

			if err := service.db.QueryRow("SELECT number FROM blocks WHERE hash = ?;", plugins.TrimPrefix(blockHash.Bytes())).Scan(&blockNumber); err != nil {
				log.Error("Error deriving blockNumber from blockHash, getSnapshot()", "hash", blockHash)
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

    if blockNumber % 1024 == 0 {
		snap := &Snapshot{}
		snap, _ = service.fetchSnapshot(ctx, blockNumber)	
		return snap, nil
	}

	recents, _ := service.getRecents(blockNumber)

	switch {

		case (blockNumber + 1) % 64 == 0:
			var snap *Snapshot
			var err error

			degree := ((blockNumber / 64) % 16 ) + 1

			origin := blockNumber - (64 * (degree - 1))

			if degree == 16 {
				snap, _ := service.getSubsequentSnapshot(blockNumber)

				snap.Number = blockNumber
				snap.Hash = blockHash
				snap.Recents = recents
					
				return snap, nil
			}

			snap, err = service.getKeyFrame(origin, int64(degree))
			if err != nil {
				log.Error("GetSnapshot keyframe case error")
				return nil, err
			}

			snap.Number = blockNumber
			snap.Hash = blockHash
			snap.Recents = recents


			return snap, nil

	case (blockNumber + 1) % 64 != 0:
		var snap *Snapshot
		var err error

		previousKeyframe := ((blockNumber + 1) - ((blockNumber + 1) % 64)) - 1
		degree := ((blockNumber / 64) % 16 )
		
		if degree == 0 {
			snap, _ := service.getPreviousSnapshot(blockNumber)
			
			snap.Number = blockNumber
			snap.Hash = blockHash
			snap.Recents = recents
			return snap, nil
		}
		
		origin := previousKeyframe - (64 * (degree -1))
		snap, err = service.getKeyFrame(origin, int64(degree))
		if err != nil {
			log.Error("GetSnapshot non-keyframe case error")
			return nil, err
		}

		snap.Number = blockNumber
		snap.Hash = blockHash
		snap.Recents = recents

		return snap, nil

	default:
		var err error
		err = errors.New("invalid input")
		log.Error("Cannot generate snapshot", "err", err)
		return nil, err

	}

}