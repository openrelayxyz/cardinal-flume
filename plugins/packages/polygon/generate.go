package main

import (
	// "bytes"
	"context"
	// "encoding/json"
	// "fmt"
	// "math"
	"math/big"
	"errors"
	// "sort"

	
	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-evm/common"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/flume/plugins"
)

type Snapshot struct {
	// config   *params.BorConfig // Consensus engine parameters to fine tune behavior
	// sigcache *lru.ARCCache     // Cache of recent block signatures to speed up ecrecover

	Number       uint64                    `json:"number"`       // Block number where the snapshot was created
	Hash         types.Hash               `json:"hash"`         // Block hash where the snapshot was created
	ValidatorSet *ValidatorSet      `json:"validatorSet"` // Validator set at this moment
	Recents      map[uint64]common.Address `json:"recents"`      // Set of recent signers for spam protections
}

type ValidatorSet struct {
	Validators []*Validator `json:"validators"`
	Proposer   *Validator   `json:"proposer"`

	// cached (unexported)
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

func (service *PolygonBorService) getPreviousSnapshot(blockNumber uint64) (*Snapshot, error) { 
	var lastSnapBlock uint64

	if err := service.db.QueryRowContext(context.Background(), "SELECT block FROM bor_snapshots WHERE block < ? ORDER BY block DESC LIMIT 1;", blockNumber).Scan(&lastSnapBlock);
	err != nil {
		log.Error("sql previous snapshot fetch error", "err", err)
		return nil, err
	}

	log.Info("fetching previous snapshot", "")

	snap, err := service.snapshot(context.Background(), lastSnapBlock)
	if err != nil {
		log.Error("error fetching previous snapshot", "number", snap.Number)
		return nil, err
	}

	return snap, nil
}

func (service *PolygonBorService) getSubsequentSnapshot(blockNumber uint64) (*Snapshot, error) { 
	var nextSnapBlock uint64

	if err := service.db.QueryRowContext(context.Background(), "SELECT block FROM bor_snapshots WHERE block > ? ORDER BY block DESC LIMIT 1;", blockNumber).Scan(&nextSnapBlock);
	err != nil {
		log.Error("sql subsequent snapshot fetch error", "err", err)
		return nil, err
	}

	log.Info("fetching subsequent snapshot")

	snap, err := service.snapshot(context.Background(), nextSnapBlock)
	if err != nil {
		log.Error("error fetching subsequent snapshot", "number", snap.Number)
		return nil, err
	}

	return snap, nil
}

func ParseValidators(validatorsBytes []byte) ([]*Validator, error) {
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

func (service *PolygonBorService) GetVals(number uint64) ([]*Validator, error) { 
	
	var extra []byte 

	if err := service.db.QueryRowContext(context.Background(), "SELECT extra FROM blocks.blocks WHERE number = ?;", number).Scan(&extra);
	err != nil {
		log.Error("sql keyframe extra fetch error", "err", err)
		return nil, err
	}

	log.Info("found extra", "len", len(extra))

	validatorBytes := extra[extraVanity : len(extra)-extraSeal]

 	newVals, _ := ParseValidators(validatorBytes)

	log.Error("got newVals", "len", len(newVals))

	var vals []*Validator

	vals = newVals

	return vals, nil
	
}
	
func (service *PolygonBorService) getRecents(blockNumber uint64) (map[uint64]common.Address, error) {

	recents := make(map[uint64]common.Address)

	for i := blockNumber - 63; i <= blockNumber; i++ {
		var signer common.Address
		if err := service.db.QueryRowContext(context.Background(), "SELECT coinbase FROM blocks.blocks WHERE number = ?;", i).Scan(&signer); 
		err != nil {
			log.Error("getRecents() error fetching signers", "err", err.Error())
			return nil, err
		}
		recents[i] = signer
	}

	return recents, nil 


}

func (service *PolygonBorService) getArmature(blockNumber uint64) (*Snapshot, error) {

	var  hashBytes []byte

	log.Info("getArmature", "blockNumber", blockNumber)

	if err := service.db.QueryRowContext(context.Background(), "SELECT hash FROM blocks.blocks WHERE number = ?;", blockNumber).Scan(&hashBytes);
	err != nil {
		log.Error("getArmature fetch hash error", "err", err)
		return nil, err
	}

	snap := &Snapshot{}

	recents, err := service.getRecents(blockNumber)
	if err != nil {
		log.Error("GetTestSnapshot fetch recents error", "err", err)
		return nil, err
	}

	vals, err := service.GetVals(blockNumber)
	log.Error("got vals in armature function", "len", len(vals))
	if err != nil {
		log.Error("getArmature fetch vals error", "err", err.Error())
		return nil, err
	}

	vs := &ValidatorSet{
		Validators: vals,
	}

	snap.Hash = plugins.BytesToHash(hashBytes)
	snap.Number = uint64(blockNumber)
	snap.Recents = recents
	snap.ValidatorSet = vs

	return snap, nil	

}

func getDegree(number int64) int64 {
	degree := ((number/ 64) % 16 ) + 1
	return degree
}

func (service *PolygonBorService) getFrames(blockNumber uint64, degree int64) []*Snapshot {

	frames := make([]*Snapshot, degree)

	// log.Warn("inside get frames, len frames", "len", len(frames), "pre loop degree", degree)

	for i := 0; i < int(degree); i++ {
		// log.Error("inside get frames loop", "i", i)
		snap := &Snapshot{}
		snap, _ = service.getArmature((blockNumber) + (uint64(64) * uint64(i)))
		frames[i] = snap
	}

	// log.Warn("inside getFrames, len frames", "len", len(frames), "post loop degree", degree)

	updatedFrames := service.UpdateValidators(blockNumber, frames)



	return updatedFrames
}

func (service *PolygonBorService) UpdateValidators(blockNumber uint64, snaps []*Snapshot) []*Snapshot {

	previousSnap, _ := service.getPreviousSnapshot(blockNumber)
	
	expandedFrames := []*Snapshot{previousSnap}

	expandedFrames = append(expandedFrames, snaps[:]...)

	log.Warn("inside update expanded frames", "len", len(expandedFrames))

	for i := 1; i <= len(snaps); i++ {

		log.Info("inside snaps loop UV", "i", i)

		votingPowerSum := int64(0)

		maxPriority := int64(0)

		var maxPriorityIndex int

		for j, val := range expandedFrames[i].ValidatorSet.Validators {

			votingPowerSum += val.VotingPower

			previousVal := expandedFrames[i - 1].ValidatorSet.Validators[j]
			
			val.ProposerPriority = previousVal.VotingPower + previousVal.ProposerPriority 
			
			log.Info("inside updateValidator loop", "new_accum", val.ProposerPriority, "degree", i)
			
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

func (service *PolygonBorService) GetTestSnapshot(ctx context.Context, blockNumber plugins.BlockNumber) (interface{}, error) {
	
	log.Warn("intial block value", "blockNumber", blockNumber)

	switch {
	case blockNumber.Int64() % 1024 == 0:
		log.Info("is a snapshot")
		snap := &Snapshot{}
		snap, _ = service.snapshot(ctx, uint64(blockNumber.Int64()))	
		return snap, nil

	case (blockNumber.Int64() + 1) % 64 == 0:
		degree := getDegree(blockNumber.Int64())

		if degree == 16 {
			snap, _ := service.getSubsequentSnapshot(uint64(blockNumber.Int64()))
			var hashBytes []byte
			if err := service.db.QueryRowContext(context.Background(), "SELECT hash FROM blocks.blocks WHERE number = ?;", uint64(blockNumber.Int64())).Scan(&hashBytes);
			err != nil {
				log.Error("generate snapshot fetch hash error", "err", err)
				return nil, err
			}
			snap.Number = uint64(blockNumber.Int64())
			snap.Hash = plugins.BytesToHash(hashBytes)
			return snap, nil
		}

		origin := blockNumber.Int64() - (64 * (degree - 1))

		log.Info("GetTestSnapshot", "block contains a keyframe value of degree", degree)
		frames := service.getFrames(uint64(origin), degree)
		log.Info("retrieved snapshot frames", "len", len(frames), "degree", degree)
		return frames[len(frames) - 1], nil

	case (blockNumber.Int64() + 1) % 64 != 0:
		number := ((blockNumber.Int64() + 1) - ((blockNumber.Int64() + 1) % 64)) - 1
		degree := getDegree(number)

		if degree == 16 {
			snap, _ := service.getSubsequentSnapshot(uint64(blockNumber.Int64()))
			var hashBytes []byte
			if err := service.db.QueryRowContext(context.Background(), "SELECT hash FROM blocks.blocks WHERE number = ?;", uint64(blockNumber.Int64())).Scan(&hashBytes);
			err != nil {
				log.Error("generate snapshot fetch hash error", "err", err)
				return nil, err
			}
			snap.Number = uint64(blockNumber.Int64())
			snap.Hash = plugins.BytesToHash(hashBytes)
			return snap, nil
		}

		origin := number - (64 * (degree - 1))
		log.Info("GetTestSnapshot", "generated a keyframe value of degree", degree)
		frames := service.getFrames(uint64(origin), degree)
		log.Info("generated snapshot frames", "len", len(frames), "degree", degree)

		var hashBytes []byte

		if err := service.db.QueryRowContext(context.Background(), "SELECT hash FROM blocks.blocks WHERE number = ?;", uint64(blockNumber.Int64())).Scan(&hashBytes);
		err != nil {
			log.Error("generate snapshot fetch hash error", "err", err)
			return nil, err
		}

		frame := frames[len(frames) - 1]

		recents, _ := service.getRecents(uint64(blockNumber.Int64()))

		frame.Number = uint64(blockNumber.Int64())
		frame.Hash = plugins.BytesToHash(hashBytes)
		frame.Recents = recents

		return frame, nil

	default:
		return "this is not a snapshot or a keyframe", nil

	}

}

func (service *PolygonBorService) Wtf(ctx context.Context, number int64) int64 {
	val := (number + 1) - ((number + 1) % 64)
	return val
}