package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	// "errors"
	"sort"

	
	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-evm/common"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/flume/plugins"
)

// func (service *PolygonBorService) InspectSnapshot(ctx context.Context, blockNumber uint64) (interface{}, error) {
// 	var snapshotBytes []byte
	
// 	if err := service.db.QueryRowContext(context.Background(), "SELECT snapsfunc (service *PolygonBorService) getArmature(blockNumber uint64) (*Snapshot, error) {

// 	var  hashBytes []byte

// 	if err := service.db.QueryRowContext(context.Background(), "SELECT hash FROM blocks.blocks WHERE number = ?;", blockNumber).Scan(&hashBytes);
// 		err != nil {
// 			log.Error("GetTestSnapshot fetch hash error", "err", err)
// 			return nil, err
// 		}
	
// 	snap := &Snapshot{}

// 	recents, err := service.getRecents(blockNumber)
// 	if err != nil {
// 		log.Error("GetTestSnapshot fetch recents error", "err", err)
// 		return nil, err
// 	}

// 	snap.Hash = plugins.BytesToHash(hashBytes)
// 	snap.Number = uint64(blockNumber)
// 	snap.Recents = recents

// 	return snap, nil	


// }hot FROM bor.bor_snapshots WHERE block = ?;", blockNumber).Scan(&snapshotBytes);
// 	err != nil {
// 		log.Error("sql snapshot fetch error", "err", err)
// 		return nil, err
// 	}
	
	
// 	ssb, err := plugins.Decompress(snapshotBytes)
// 	if err != nil {
// 		log.Error("sql snapshot decompress error", "err", err)
// 		return nil, err
// 	}
	
// 	var snapshot interface{}

// 	json.Unmarshal(ssb, &snapshot)


// 	return snapshot, nil
// }

// type Snapshot struct {
// 	// config   *params.BorConfig // Consensus engine parameters to fine tune behavior
// 	// sigcache *lru.ARCCache     // Cache of recent block signatures to speed up ecrecover

// 	Number       uint64                    `json:"number"`       // Block number where the snapshot was created
// 	Hash         types.Hash               `json:"hash"`         // Block hash where the snapshot was created
// 	ValidatorSet *ValidatorSet      `json:"validatorSet"` // Validator set at this moment
// 	Recents      map[uint64]common.Address `json:"recents"`      // Set of recent signers for spam protections
// }

// type ValidatorSet struct {
// 	Validators []*Validator `json:"validators"`
// 	Proposer   *Validator   `json:"proposer"`

// 	// cached (unexported)
// 	totalVotingPower int64
// 	validatorsMap    map[common.Address]int // address -> index
// }

// type Validator struct {
// 	ID               uint64         `json:"ID"`
// 	Address          common.Address `json:"signer"`
// 	VotingPower      int64          `json:"power"`
// 	ProposerPriority int64          `json:"accum"`
// }

// func (service *PolygonBorService) getPreviousSnapshot(blockNumber uint64) (*Snapshot, error) { 
// 	var lastSnapBlock uint64

// 	if err := service.db.QueryRowContext(context.Background(), "SELECT block FROM bor_snapshots WHERE block < ? ORDER BY block DESC LIMIT 1;", blockNumber).Scan(&lastSnapBlock);
// 	err != nil {
// 		log.Error("sql previous snapshot fetch error", "err", err)
// 		return nil, err
// 	}

// 	log.Info("fetching previous snapshot")

// 	snap, err := service.snapshot(context.Background(), lastSnapBlock)
// 	if err != nil {
// 		log.Error("error fetching previous snapshot")
// 		return nil, err
// 	}

// 	return snap, nil
// }

// func (service *PolygonBorService) GetTestSnapshot(ctx context.Context, blockNumber plugins.BlockNumber) (interface{}, error) {

	
	
// 	if blockNumber.Int64() % 1024 == 0 {
// 		snap := &Snapshot{}
// 		snap, _ = service.snapshot(ctx, uint64(blockNumber.Int64()))	
// 		return snap, nil
// 	}



// 	if (blockNumber.Int64() + 1) % 64 == 0 {
// 		degree := ((blockNumber.Int64() / 64) % 16 ) + 1
// 		frames := make([]*Snapshot, degree, degree)
// 		for i := 0; i < int(degree); i++ {
// 			snap := &Snapshot{}
// 			snap, _ = service.getKeyFrame(uint64(blockNumber.Int64()) + (uint64(64) * uint64(i)))
// 			frames[i] = snap
// 		}
// 		return frames, nil
// 	}

// 	return nil, nil

// 	// check if keyframe (number +1 % 64 == 0)
// 	// if is keyframe check how far from last master key, fetch objects and pull values through

// 	//if is not keyframe calculate last keyframe number and return starting from that position. 

// }

// func (service *PolygonBorService) getArmature(blockNumber uint64) (*Snapshot, error) {

// 	var  hashBytes []byte

// 	if err := service.db.QueryRowContext(context.Background(), "SELECT hash FROM blocks.blocks WHERE number = ?;", blockNumber).Scan(&hashBytes);
// 		err != nil {
// 			log.Error("GetTestSnapshot fetch hash error", "err", err)
// 			return nil, err
// 		}
	
// 	snap := &Snapshot{}

// 	recents, err := service.getRecents(blockNumber)
// 	if err != nil {
// 		log.Error("GetTestSnapshot fetch recents error", "err", err)
// 		return nil, err
// 	}

// 	snap.Hash = plugins.BytesToHash(hashBytes)
// 	snap.Number = uint64(blockNumber)
// 	snap.Recents = recents

// 	return snap, nil	


// }

// func (service *PolygonBorService) getKeyFrame(blockNumber uint64) (*Snapshot, error) {

// 	snap, _ := service.getArmature(blockNumber)


// 	return snap, nil

// }

// func (service *PolygonBorService) getRecents(blockNumber uint64) (map[uint64]common.Address, error) {

// 	recents := make(map[uint64]common.Address)

// 	for i := blockNumber - 64; i <= blockNumber; i++ {
// 		var signer common.Address
// 		if err := service.db.QueryRowContext(context.Background(), "SELECT coinbase FROM blocks.blocks WHERE number = ?;", i).Scan(&signer); 
// 		err != nil {
// 			log.Error("getRecents() error fetching signers", "err", err.Error())
// 			return nil, err
// 		}
// 		recents[i] = signer
// 	}

// 	return recents, nil 


// }

// func (service *PolygonBorService) GetVals(start, end uint64) ([][]*Validator, error) { 

// 	numbers := make([]uint64, 0, 16)
// 	for i := start; i < end + 1; i += 64 {
// 		numbers = append(numbers, i)
// 	}

// 	length := end - start

// 	vals := make([][]*Validator, 0, length / 64)

// 	for _, number := range numbers {
	
// 		var extra []byte 

// 		if err := service.db.QueryRowContext(context.Background(), "SELECT extra FROM blocks.blocks WHERE number = ?;", number).Scan(&extra);
// 		err != nil {
// 			log.Error("sql keyframe extra fetch error", "err", err)
// 			return nil, err
// 		}

// 		validatorBytes := extra[extraVanity : len(extra)-extraSeal]

//  		newVals, _ := ParseValidators(validatorBytes)

// 		vals = append(vals, newVals)

// 	}

// 	return vals, nil
	
// 	}

// 	return snap, nil
// }

// func (service *PolygonBorService) testSnapshot(ctx context.Context, blockNumber plugins.BlockNumber) (*Snapshot, error) {

// 	var hashBytes []byte

// 	if err := service.db.QueryRowContext(context.Background(), "SELECT hash FROM blocks.blocks WHERE number = ?;", blockNumber.Int64()).Scan(&hashBytes); 
// 	err != nil {
// 		log.Error("Sql blockHash fetch error, InstpectSnapshot()", "err", err.Error())
// 		return nil, err
// 	}

// 	snap := &Snapshot{}

// 	if snapshot, err := service.snapshot(context.Background(), uint64(blockNumber.Int64())); err == nil {
// 		log.Info("got a snapshot from db")
// 		snap = snapshot
// 		return snap, nil
// 	} else {

		
// 		blockRange := make([]uint64, 0, 64)
// 		recents := make(map[uint64]common.Address)

// 		for i := uint64(blockNumber.Int64()) - 63; i <= uint64(blockNumber.Int64()); i++ {
// 			blockRange = append(blockRange, i)
// 		}

		
// 		for _, block := range blockRange {
// 			var signer common.Address
// 			if err := service.db.QueryRowContext(context.Background(), "SELECT coinbase FROM blocks.blocks WHERE number = ?;", block).Scan(&signer); 
// 			err != nil {
// 				return nil, err
// 			}
// 			recents[block] = signer
// 		}
		
// 		var extra []byte
		
// 		previousSnap, psNumber, _ := service.getPreviousSnapshot(uint64(blockNumber.Int64()))

// 		keyFrameSnap, _ := service.getKeySnapshot(blockNumber)

// 	}

// func (service *PolygonBorService) InspectSnapshot(ctx context.Context, blockNumber plugins.BlockNumber) (*Snapshot, error) {

// 	var hashBytes []byte

// 	if err := service.db.QueryRowContext(context.Background(), "SELECT hash FROM blocks.blocks WHERE number = ?;", blockNumber.Int64()).Scan(&hashBytes); 
// 	err != nil {
// 		log.Error("Sql blockHash fetch error, InstpectSnapshot()", "err", err.Error())
// 		return nil, err
// 	}

// 	snap := &Snapshot{}
	
// 	if snapshot, err := service.snapshot(context.Background(), uint64(blockNumber.Int64())); err == nil {
// 		log.Info("got a snapshot from db", "snapshot", snapshot)
// 		snap = snapshot
// 		return snap, nil
// 	} else {

// 		previousSnap, _ := service.getPreviousSnapshot(uint64(blockNumber.Int64()))

// 		var blockRange []uint64
// 		recents := make(map[uint64]common.Address)

// 		for i := uint64(blockNumber.Int64()) - 63; i <= uint64(blockNumber.Int64()); i++ {
// 			blockRange = append(blockRange, i)
// 		}

// 		var extra []byte

// 		if err := service.db.QueryRowContext(context.Background(), "SELECT extra FROM blocks.blocks WHERE number = ?;", blockNumber).Scan(&extra); 
// 		err != nil {
// 			log.Error("Sql blocknumber fetch error", "err", err.Error())
// 			return nil, err
// 		}
		
// 		validatorBytes := extra[extraVanity : len(extra)-extraSeal]

// 		// validatorBytes := extra 
		
// 		newVals, _ := ParseValidators(validatorBytes)
// 		v := getUpdatedValidatorSet(previousSnap.ValidatorSet, newVals)
// 		v.IncrementProposerPriority(1)
// 		snap.ValidatorSet = v
		
// 		for _, block := range blockRange {
// 			var signer common.Address
// 			if err := service.db.QueryRowContext(context.Background(), "SELECT coinbase FROM blocks.blocks WHERE number = ?;", block).Scan(&signer); 
// 			err != nil {
// 				return nil, err
// 			}
// 			recents[block] = signer
// 		}

// 		snap.Number = uint64(blockNumber.Int64())
// 		snap.Hash = plugins.BytesToHash(hashBytes)
// 		snap.Recents = recents
		
// 		return snap, nil

// 	}
	
// }

func (service *PolygonBorService) snapshot(ctx context.Context, blockNumber uint64) (*Snapshot, error) {
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

func (service *PolygonBorService) GetSnapshotByNumber(ctx context.Context, blockNumber plugins.BlockNumber) (*Snapshot, error) {

	var hashBytes []byte

	if err := service.db.QueryRowContext(context.Background(), "SELECT hash FROM blocks.blocks WHERE number = ?;", uint64(blockNumber.Int64())).Scan(&hashBytes); 
	err != nil {
		log.Error("Sql blockHash fetch error", "err", err.Error())
		return nil, err
	}

	snap := &Snapshot{}
	
	if snapshot, err := service.snapshot(context.Background(), uint64(blockNumber.Int64())); err == nil {
		log.Info("got a snapshot from db", "snapshot", snapshot)
		snap = snapshot
		return snap, nil
	} else {

		previousSnap, _ := service.getPreviousSnapshot(uint64(blockNumber.Int64()))

		var blockRange []uint64
		recents := make(map[uint64]common.Address)

		for i := blockNumber - 63; i <= blockNumber; i++ {
			blockRange = append(blockRange, uint64(i))
		}

		var extra []byte

		if err := service.db.QueryRowContext(context.Background(), "SELECT extra FROM blocks.blocks WHERE number = ?;", blockNumber).Scan(&extra); 
		err != nil {
			log.Error("Sql blocknumber fetch error", "err", err.Error())
			return nil, err
		}
		log.Error("length of extra", "len", len(extra))

		// extraVanity = 32 
		// extraSeal   = 65
		// len(extra) = 97
		// 32: (97 - 65)
		// 32:32
		// len = 0
		
		validatorBytes := extra[extraVanity : len(extra)-extraSeal]

		// validatorBytes := extra

		log.Error("validatorBytes", "len", len(validatorBytes), "vb", validatorBytes)
		
		newVals, _ := ParseValidators(validatorBytes)
		log.Error("new validotor set", "newVals", newVals)
		v := getUpdatedValidatorSet(previousSnap.ValidatorSet.Copy(), newVals)
		log.Error("update validator set", "v", v)
		v.IncrementProposerPriority(1)
		snap.ValidatorSet = v
		
		for _, block := range blockRange {
			var signer common.Address
			if err := service.db.QueryRowContext(context.Background(), "SELECT coinbase FROM blocks.blocks WHERE number = ?;", block).Scan(&signer); 
			err != nil {
				return nil, err
			}
			recents[block] = signer
		}

		snap.Number = uint64(blockNumber.Int64())
		snap.Hash = plugins.BytesToHash(hashBytes)
		snap.Recents = recents
		
		return snap, nil

	}
	
}

func (service *PolygonBorService) GetSnapshot(ctx context.Context, blockHash types.Hash) (*Snapshot, error) {

	var blockNumber uint64

	if err := service.db.QueryRowContext(context.Background(), "SELECT number FROM blocks.blocks WHERE hash = ?;", blockHash).Scan(&blockNumber); 
	err != nil {
		log.Error("Sql blockHash fetch error", "err", err.Error())
		return nil, err
	}

	snap := &Snapshot{}
	
	if snapshot, err := service.snapshot(context.Background(), blockNumber); err == nil {
		log.Info("got a snapshot from db", "snapshot", snapshot)
		snap = snapshot
		return snap, nil
	} else {

		previousSnap, _ := service.getPreviousSnapshot(blockNumber)

		var blockRange []uint64
		recents := make(map[uint64]common.Address)

		for i := blockNumber - 63; i <= blockNumber; i++ {
			blockRange = append(blockRange, i)
		}

		var extra []byte

		if err := service.db.QueryRowContext(context.Background(), "SELECT extra FROM blocks.blocks WHERE number = ?;", blockNumber).Scan(&extra); 
		err != nil {
			log.Error("Sql blocknumber fetch error", "err", err.Error())
			return nil, err
		}
		log.Error("length of extra", "len", len(extra))

		// extraVanity = 32 
		// extraSeal   = 65
		// len(extra) = 97
		// 32: (97 - 65)
		// 32:32
		// len = 0
		
		validatorBytes := extra[extraVanity : len(extra)-extraSeal]

		// validatorBytes := extra

		log.Error("validatorBytes", "len", len(validatorBytes), "vb", validatorBytes)
		
		newVals, _ := ParseValidators(validatorBytes)
		log.Error("new validotor set", "newVals", newVals)
		v := getUpdatedValidatorSet(previousSnap.ValidatorSet.Copy(), newVals)
		log.Error("update validator set", "v", v)
		v.IncrementProposerPriority(1)
		snap.ValidatorSet = v
		
		for _, block := range blockRange {
			var signer common.Address
			if err := service.db.QueryRowContext(context.Background(), "SELECT coinbase FROM blocks.blocks WHERE number = ?;", block).Scan(&signer); 
			err != nil {
				return nil, err
			}
			recents[block] = signer
		}

		snap.Number = blockNumber
		snap.Hash = blockHash
		snap.Recents = recents
		
		return snap, nil

	}
	
}




func (v *Validator) Copy() *Validator {
	vCopy := *v
	return &vCopy
}

func (vals *ValidatorSet) Copy() *ValidatorSet {
	valCopy := validatorListCopy(vals.Validators)
	validatorsMap := make(map[common.Address]int, len(vals.Validators))

	for i, val := range valCopy {
		validatorsMap[val.Address] = i
	}

	return &ValidatorSet{
		Validators:       validatorListCopy(vals.Validators),
		Proposer:         vals.Proposer,
		totalVotingPower: vals.totalVotingPower,
		validatorsMap:    validatorsMap,
	}
}

// var (
// 	extraVanity = 32 // Fixed number of extra-data prefix bytes reserved for signer vanity
// 	extraSeal   = 65 // Fixed number of extra-data suffix bytes reserved for signer seal
// )

const (
	MaxTotalVotingPower      = int64(math.MaxInt64) / 8
	PriorityWindowSizeFactor = 2
)

type ValidatorsByAddress []*Validator

func NewValidator(address common.Address, votingPower int64) *Validator {
	return &Validator{
		Address:          address,
		VotingPower:      votingPower,
		ProposerPriority: 0,
	}
}

// func ParseValidators(validatorsBytes []byte) ([]*Validator, error) {
// 	if len(validatorsBytes)%40 != 0 {
// 		log.Error("Invalid validator bytes")
// 		return nil, errors.New("Invalid validators bytes")
// 	}

// 	result := make([]*Validator, len(validatorsBytes)/40)

// 	for i := 0; i < len(validatorsBytes); i += 40 {
// 		address := make([]byte, 20)
// 		power := make([]byte, 20)

// 		copy(address, validatorsBytes[i:i+20])
// 		copy(power, validatorsBytes[i+20:i+40])

// 		result[i/40] = NewValidator(plugins.BytesToAddress(address), big.NewInt(0).SetBytes(power).Int64())
// 	}

// 	return result, nil
// }

func getUpdatedValidatorSet(oldValidatorSet *ValidatorSet, newVals []*Validator) *ValidatorSet {
	v := oldValidatorSet
	oldVals := v.Validators

	changes := make([]*Validator, 0, len(oldVals))

	for _, ov := range oldVals {
		if f, ok := validatorContains(newVals, ov); ok {
			ov.VotingPower = f.VotingPower
		} else {
			ov.VotingPower = 0
		}

		changes = append(changes, ov)
	}

	for _, nv := range newVals {
		if _, ok := validatorContains(changes, nv); !ok {
			changes = append(changes, nv)
		}
	}

	if err := v.UpdateWithChangeSet(changes); err != nil {
		log.Error("Error while updating change set", "error", err)
	}

	return v
}

func validatorListCopy(valsList []*Validator) []*Validator {
	if valsList == nil {
		return nil
	}

	valsCopy := make([]*Validator, len(valsList))

	for i, val := range valsList {
		valsCopy[i] = val.Copy()
	}

	return valsCopy
}

func (vals *ValidatorSet) IncrementProposerPriority(times int) {
	if vals.IsNilOrEmpty() {
		panic("empty validator set")
	}

	if times <= 0 {
		panic("Cannot call IncrementProposerPriority with non-positive times")
	}

	// Cap the difference between priorities to be proportional to 2*totalPower by
	// re-normalizing priorities, i.e., rescale all priorities by multiplying with:
	//  2*totalVotingPower/(maxPriority - minPriority)
	diffMax := PriorityWindowSizeFactor * vals.TotalVotingPower()
	vals.RescalePriorities(diffMax)
	vals.shiftByAvgProposerPriority()

	var proposer *Validator
	// Call IncrementProposerPriority(1) times times.
	for i := 0; i < times; i++ {
		proposer = vals.incrementProposerPriority()
	}

	vals.Proposer = proposer
}

func validatorContains(a []*Validator, x *Validator) (*Validator, bool) {
	for _, n := range a {
		if n.Address == x.Address {
			return n, true
		}
	}

	return nil, false
}

func (vals *ValidatorSet) IsNilOrEmpty() bool {
	return vals == nil || len(vals.Validators) == 0
}

func (vals *ValidatorSet) TotalVotingPower() int64 {
	if vals.totalVotingPower == 0 {
		log.Info("invoking updateTotalVotingPower before returning it")

		if err := vals.UpdateTotalVotingPower(); err != nil {
			// Can/should we do better?
			panic(err)
		}
	}

	return vals.totalVotingPower
}


func (vals *ValidatorSet) shiftByAvgProposerPriority() {
	if vals.IsNilOrEmpty() {
		panic("empty validator set")
	}

	avgProposerPriority := vals.computeAvgProposerPriority()

	for _, val := range vals.Validators {
		val.ProposerPriority = safeSubClip(val.ProposerPriority, avgProposerPriority)
	}
}


func (vals *ValidatorSet) incrementProposerPriority() *Validator {
	for _, val := range vals.Validators {
		// Check for overflow for sum.
		newPrio := safeAddClip(val.ProposerPriority, val.VotingPower)
		val.ProposerPriority = newPrio
	}
	// Decrement the validator with most ProposerPriority.
	mostest := vals.getValWithMostPriority()
	// Mind the underflow.
	mostest.ProposerPriority = safeSubClip(mostest.ProposerPriority, vals.TotalVotingPower())

	return mostest
}

func safeSubClip(a, b int64) int64 {
	c, overflow := safeSub(a, b)
	if overflow {
		if b > 0 {
			return math.MinInt64
		}

		return math.MaxInt64
	}

	return c
}

func safeSub(a, b int64) (int64, bool) {
	if b > 0 && a < math.MinInt64+b {
		return -1, true
	} else if b < 0 && a > math.MaxInt64+b {
		return -1, true
	}

	return a - b, false
}

func safeAddClip(a, b int64) int64 {
	c, overflow := safeAdd(a, b)
	if overflow {
		if b < 0 {
			return math.MinInt64
		}

		return math.MaxInt64
	}

	return c
}

func safeAdd(a, b int64) (int64, bool) {
	if b > 0 && a > math.MaxInt64-b {
		return -1, true
	} else if b < 0 && a < math.MinInt64-b {
		return -1, true
	}

	return a + b, false
}

func (vals *ValidatorSet) RescalePriorities(diffMax int64) {
	if vals.IsNilOrEmpty() {
		panic("empty validator set")
	}
	// NOTE: This check is merely a sanity check which could be
	// removed if all tests would init. voting power appropriately;
	// i.e. diffMax should always be > 0
	if diffMax <= 0 {
		return
	}

	// Calculating ceil(diff/diffMax):
	// Re-normalization is performed by dividing by an integer for simplicity.
	// NOTE: This may make debugging priority issues easier as well.
	diff := computeMaxMinPriorityDiff(vals)
	ratio := (diff + diffMax - 1) / diffMax

	if diff > diffMax {
		for _, val := range vals.Validators {
			val.ProposerPriority = val.ProposerPriority / ratio
		}
	}
}

func computeMaxMinPriorityDiff(vals *ValidatorSet) int64 {
	if vals.IsNilOrEmpty() {
		panic("empty validator set")
	}

	max := int64(math.MinInt64)
	min := int64(math.MaxInt64)

	for _, v := range vals.Validators {
		if v.ProposerPriority < min {
			min = v.ProposerPriority
		}

		if v.ProposerPriority > max {
			max = v.ProposerPriority
		}
	}

	diff := max - min

	if diff < 0 {
		return -1 * diff
	} else {
		return diff
	}
}

func (vals *ValidatorSet) getValWithMostPriority() *Validator {
	var res *Validator
	for _, val := range vals.Validators {
		res = res.Cmp(val)
	}

	return res
}

func (v *Validator) Cmp(other *Validator) *Validator {
	// if both of v and other are nil, nil will be returned and that could possibly lead to nil pointer dereference bubbling up the stack
	if v == nil {
		return other
	}

	if other == nil {
		return v
	}

	if v.ProposerPriority > other.ProposerPriority {
		return v
	}

	if v.ProposerPriority < other.ProposerPriority {
		return other
	}

	result := bytes.Compare(v.Address.Bytes(), other.Address.Bytes())

	if result == 0 {
		panic("Cannot compare identical validators")
	}

	if result < 0 {
		return v
	}

	// result > 0
	return other
}

func (vals *ValidatorSet) computeAvgProposerPriority() int64 {
	n := int64(len(vals.Validators))
	sum := big.NewInt(0)

	for _, val := range vals.Validators {
		sum.Add(sum, big.NewInt(val.ProposerPriority))
	}

	avg := sum.Div(sum, big.NewInt(n))

	if avg.IsInt64() {
		return avg.Int64()
	}

	// This should never happen: each val.ProposerPriority is in bounds of int64.
	panic(fmt.Sprintf("Cannot represent avg ProposerPriority as an int64 %v", avg))
}

func (vals *ValidatorSet) UpdateTotalVotingPower() error {
	sum := int64(0)
	for _, val := range vals.Validators {
		// mind overflow
		sum = safeAddClip(sum, val.VotingPower)
		if sum > MaxTotalVotingPower {
			return &TotalVotingPowerExceededError{sum, vals.Validators}
		}
	}

	vals.totalVotingPower = sum

	return nil
}

type TotalVotingPowerExceededError struct {
	Sum        int64
	Validators []*Validator
}

func (e *TotalVotingPowerExceededError) Error() string {
	return fmt.Sprintf(
		"Total voting power should be guarded to not exceed %v; got: %v; for validator set: %v",
		MaxTotalVotingPower,
		e.Sum,
		e.Validators,
	)
}

func (vals *ValidatorSet) UpdateWithChangeSet(changes []*Validator) error {
	return vals.updateWithChangeSet(changes, true)
}


func (vals *ValidatorSet) updateWithChangeSet(changes []*Validator, allowDeletes bool) error {
	if len(changes) <= 0 {
		return nil
	}

	// Check for duplicates within changes, split in 'updates' and 'deletes' lists (sorted).
	updates, deletes, err := processChanges(changes)
	if err != nil {
		return err
	}

	if !allowDeletes && len(deletes) != 0 {
		return fmt.Errorf("cannot process validators with voting power 0: %v", deletes)
	}

	// Verify that applying the 'deletes' against 'vals' will not result in error.
	if err := verifyRemovals(deletes, vals); err != nil {
		return err
	}

	// Verify that applying the 'updates' against 'vals' will not result in error.
	updatedTotalVotingPower, numNewValidators, err := verifyUpdates(updates, vals)
	if err != nil {
		return err
	}

	// Check that the resulting set will not be empty.
	if numNewValidators == 0 && len(vals.Validators) == len(deletes) {
		return fmt.Errorf("applying the validator changes would result in empty set")
	}

	// Compute the priorities for updates.
	computeNewPriorities(updates, vals, updatedTotalVotingPower)

	// Apply updates and removals.
	vals.updateValidators(updates, deletes)

	if err := vals.UpdateTotalVotingPower(); err != nil {
		return err
	}

	// Scale and center.
	vals.RescalePriorities(PriorityWindowSizeFactor * vals.TotalVotingPower())
	vals.shiftByAvgProposerPriority()

	return nil
}

func processChanges(origChanges []*Validator) (updates, removals []*Validator, err error) {
	// Make a deep copy of the changes and sort by address.
	changes := validatorListCopy(origChanges)
	sort.Sort(ValidatorsByAddress(changes))

	sliceCap := len(changes) / 2
	if sliceCap == 0 {
		sliceCap = 1
	}

	removals = make([]*Validator, 0, sliceCap)
	updates = make([]*Validator, 0, sliceCap)

	var prevAddr common.Address

	// Scan changes by address and append valid validators to updates or removals lists.
	for _, valUpdate := range changes {
		if valUpdate.Address == prevAddr {
			err = fmt.Errorf("duplicate entry %v in %v", valUpdate, changes)
			return nil, nil, err
		}

		if valUpdate.VotingPower < 0 {
			err = fmt.Errorf("voting power can't be negative: %v", valUpdate)
			return nil, nil, err
		}

		if valUpdate.VotingPower > MaxTotalVotingPower {
			err = fmt.Errorf("to prevent clipping/ overflow, voting power can't be higher than %v: %v ",
				MaxTotalVotingPower, valUpdate)
			return nil, nil, err
		}

		if valUpdate.VotingPower == 0 {
			removals = append(removals, valUpdate)
		} else {
			updates = append(updates, valUpdate)
		}

		prevAddr = valUpdate.Address
	}

	return updates, removals, err
}

func verifyRemovals(deletes []*Validator, vals *ValidatorSet) error {
	for _, valUpdate := range deletes {
		address := valUpdate.Address
		_, val := vals.GetByAddress(address)

		if val == nil {
			return fmt.Errorf("failed to find validator %X to remove", address)
		}
	}

	if len(deletes) > len(vals.Validators) {
		panic("more deletes than validators")
	}

	return nil
}

func verifyUpdates(updates []*Validator, vals *ValidatorSet) (updatedTotalVotingPower int64, numNewValidators int, err error) {
	updatedTotalVotingPower = vals.TotalVotingPower()

	for _, valUpdate := range updates {
		address := valUpdate.Address
		_, val := vals.GetByAddress(address)

		if val == nil {
			// New validator, add its voting power the the total.
			updatedTotalVotingPower += valUpdate.VotingPower
			numNewValidators++
		} else {
			// Updated validator, add the difference in power to the total.
			updatedTotalVotingPower += valUpdate.VotingPower - val.VotingPower
		}

		overflow := updatedTotalVotingPower > MaxTotalVotingPower

		if overflow {
			err = fmt.Errorf(
				"failed to add/update validator %v, total voting power would exceed the max allowed %v",
				valUpdate, MaxTotalVotingPower)

			return 0, 0, err
		}
	}

	return updatedTotalVotingPower, numNewValidators, nil
}

func computeNewPriorities(updates []*Validator, vals *ValidatorSet, updatedTotalVotingPower int64) {
	for _, valUpdate := range updates {
		address := valUpdate.Address
		_, val := vals.GetByAddress(address)

		if val == nil {
			// add val
			// Set ProposerPriority to -C*totalVotingPower (with C ~= 1.125) to make sure validators can't
			// un-bond and then re-bond to reset their (potentially previously negative) ProposerPriority to zero.
			//
			// Contract: updatedVotingPower < MaxTotalVotingPower to ensure ProposerPriority does
			// not exceed the bounds of int64.
			//
			// Compute ProposerPriority = -1.125*totalVotingPower == -(updatedVotingPower + (updatedVotingPower >> 3)).
			valUpdate.ProposerPriority = -(updatedTotalVotingPower + (updatedTotalVotingPower >> 3))
		} else {
			valUpdate.ProposerPriority = val.ProposerPriority
		}
	}
}

func (vals *ValidatorSet) GetByAddress(address common.Address) (index int, val *Validator) {
	idx, ok := vals.validatorsMap[address]
	if ok {
		return idx, vals.Validators[idx].Copy()
	}

	return -1, nil
}

func (vals *ValidatorSet) updateValidators(updates []*Validator, deletes []*Validator) {
	vals.applyUpdates(updates)
	vals.applyRemovals(deletes)

	vals.UpdateValidatorMap()
}

func (vals *ValidatorSet) applyUpdates(updates []*Validator) {
	existing := vals.Validators
	merged := make([]*Validator, len(existing)+len(updates))
	i := 0

	for len(existing) > 0 && len(updates) > 0 {
		if bytes.Compare(existing[0].Address.Bytes(), updates[0].Address.Bytes()) < 0 { // unchanged validator
			merged[i] = existing[0]
			existing = existing[1:]
		} else {
			// Apply add or update.
			merged[i] = updates[0]
			if existing[0].Address == updates[0].Address {
				// Validator is present in both, advance existing.
				existing = existing[1:]
			}

			updates = updates[1:]
		}
		i++
	}

	// Add the elements which are left.
	for j := 0; j < len(existing); j++ {
		merged[i] = existing[j]
		i++
	}

	// OR add updates which are left.
	for j := 0; j < len(updates); j++ {
		merged[i] = updates[j]
		i++
	}

	vals.Validators = merged[:i]
}

func (vals *ValidatorSet) applyRemovals(deletes []*Validator) {
	existing := vals.Validators

	merged := make([]*Validator, len(existing)-len(deletes))
	i := 0

	// Loop over deletes until we removed all of them.
	for len(deletes) > 0 {
		if existing[0].Address == deletes[0].Address {
			deletes = deletes[1:]
		} else { // Leave it in the resulting slice.
			merged[i] = existing[0]
			i++
		}

		existing = existing[1:]
	}

	// Add the elements which are left.
	for j := 0; j < len(existing); j++ {
		merged[i] = existing[j]
		i++
	}

	vals.Validators = merged[:i]
}

func (vals *ValidatorSet) UpdateValidatorMap() {
	vals.validatorsMap = make(map[common.Address]int, len(vals.Validators))

	for i, val := range vals.Validators {
		vals.validatorsMap[val.Address] = i
	}
}

func (valz ValidatorsByAddress) Len() int {
	return len(valz)
}

func (valz ValidatorsByAddress) Less(i, j int) bool {
	return bytes.Compare(valz[i].Address.Bytes(), valz[j].Address.Bytes()) == -1
}

func (valz ValidatorsByAddress) Swap(i, j int) {
	it := valz[i]
	valz[i] = valz[j]
	valz[j] = it
}