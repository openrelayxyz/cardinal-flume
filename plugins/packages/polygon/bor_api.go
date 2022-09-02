package main

import (
	"database/sql"
	"context"

	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-evm/common"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/flume/plugins"
	"github.com/openrelayxyz/flume/config"

)

type PolygonBorService struct {
	db *sql.DB
	cfg *config.Config
}

func (service *PolygonBorService) GetRootHash(ctx context.Context, starBlockNr uint64, endBlockNr uint64) (string, error) {
	return "goodbye horses", nil
}

func (service *PolygonBorService) GetSignersAtHash(ctx context.Context, hash types.Hash) ([]common.Address, error) {

	var result []common.Address

	snap, err := service.GetSnapshot(context.Background(), hash)
	if err != nil {
		log.Error("Error fetching snapshot GetSignersAtHash", "err", err.Error())
		return nil, err
	}

	for _, validator := range snap.ValidatorSet.Validators {
		result = append(result, validator.Address)
	}

	return result, nil 

}

func (service *PolygonBorService) GetCurrentValidators(ctx context.Context) ([]*Validator, error) {

	var blockNumber int64
	var hash []byte
	err := service.db.QueryRowContext(ctx, "SELECT max(number), hash FROM blocks.blocks;").Scan(&blockNumber, &hash)

	var result []*Validator

	snap, err := service.GetSnapshot(context.Background(), plugins.BytesToHash(hash))
	if err != nil {
		log.Error("Error fetching snapshot GetCurrentValidators", "err", err.Error())
		return nil, err
	}

	for _, validator := range snap.ValidatorSet.Validators {
		result = append(result, validator)
	}

	return result, nil 

}

func (service *PolygonBorService) GetCurrentProposer(ctx context.Context) (*common.Address, error) {

	var result *common.Address

	var blockNumber int64
	var hash []byte
	err := service.db.QueryRowContext(ctx, "SELECT max(number), hash FROM blocks.blocks;").Scan(&blockNumber, &hash)

	snap, err := service.GetSnapshot(context.Background(), plugins.BytesToHash(hash))
	if err != nil {
		log.Error("Error fetching snapshot GetCurrentProposer", "err", err.Error())
		return nil, err
	}

	result = &snap.ValidatorSet.Proposer.Address

	return result, nil 

}