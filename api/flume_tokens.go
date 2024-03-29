package api

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-evm/common"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-flume/config"
	"github.com/openrelayxyz/cardinal-flume/heavy"
	"github.com/openrelayxyz/cardinal-flume/plugins"
)

type FlumeTokensAPI struct {
	db      *sql.DB
	network uint64
	pl      *plugins.PluginLoader
	cfg     *config.Config
}

func NewFlumeTokensAPI(db *sql.DB, network uint64, pl *plugins.PluginLoader, cfg *config.Config) *FlumeTokensAPI {
	return &FlumeTokensAPI{
		db:      db,
		network: network,
		pl:      pl,
		cfg:     cfg,
	}
}

func (api *FlumeTokensAPI) Erc20ByAccount(ctx context.Context, addr common.Address, offset *int) (*paginator[common.Address], error) {
	
	if offset == nil {
		offset = new(int)
	}

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("flume_erc20ByAccount sent to flume heavy by default")
		missMeter.Mark(1)
		address, err := heavy.CallHeavy[*paginator[common.Address]](ctx, api.cfg.HeavyServer, "flume_erc20ByAccount", addr, offset)
		if err != nil {
			return nil, err
		}
		return *address, nil
	}

	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	topic0 := types.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
	rows, err := api.db.QueryContext(tctx, `SELECT distinct(address) FROM event_logs INDEXED BY topic2_partial WHERE topic0 = ? AND topic2 = ? AND topic3 IS NULL LIMIT 1000 OFFSET ?;`, trimPrefix(topic0.Bytes()), trimPrefix(addr.Bytes()), offset)
	if err != nil {
		log.Error("Error getting account addresses", "err", err.Error())
		return nil, err
	}
	defer rows.Close()
	addresses := []common.Address{}
	for rows.Next() {
		var addrBytes []byte
		err := rows.Scan(&addrBytes)
		if err != nil {
			log.Error("Query Error", "err", err.Error())
			return nil, err
		}
		addresses = append(addresses, bytesToAddress(addrBytes))
	}
	if err := rows.Err(); err != nil {
		log.Error("Query Error", "err", err.Error())
		return nil, err
	}
	result := paginator[common.Address]{Items: addresses}
	if len(addresses) == 1000 {
		result.Token = *offset + len(addresses)
	}
	return &result, nil
}

func (api *FlumeTokensAPI) Erc20Holders(ctx context.Context, addr common.Address, offset *int) (*paginator[common.Address], error) {
	
	if offset == nil {
		offset = new(int)
	}
	
	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("flume_erc20Holders sent to flume heavy by default")
		missMeter.Mark(1)
		address, err := heavy.CallHeavy[*paginator[common.Address]](ctx, api.cfg.HeavyServer, "flume_erc20Holders", addr, offset)
		if err != nil {
			return nil, err
		}
		return *address, nil
	}

	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()


	topic0 := types.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
	// topic0 must match ERC20, topic3 must be empty (to exclude ERC721) and topic2 is the recipient address
	rows, err := api.db.QueryContext(tctx, `SELECT distinct(topic2) FROM logs.event_logs INDEXED BY address_compound WHERE topic0 = ? AND address = ? AND topic3 IS NULL LIMIT 1000 OFFSET ?;`, trimPrefix(topic0.Bytes()), trimPrefix(addr.Bytes()), offset)
	if err != nil {
		log.Error("Error getting account addresses", "err", err.Error())
		return nil, err
	}
	defer rows.Close()
	addresses := []common.Address{}
	for rows.Next() {
		var addrBytes []byte
		err := rows.Scan(&addrBytes)
		if err != nil {
			log.Error("Query Error", "err", err.Error())
			return nil, err
		}
		addresses = append(addresses, bytesToAddress(addrBytes))
	}
	if err := rows.Err(); err != nil {
		log.Error("Query Error", "err", err.Error())
		return nil, fmt.Errorf("database error")
	}
	result := paginator[common.Address]{Items: addresses}
	if len(addresses) == 1000 {
		result.Token = *offset + len(addresses)
	}
	return &result, nil
}
