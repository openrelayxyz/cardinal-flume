package api

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-evm/common"
	"github.com/openrelayxyz/cardinal-rpc"
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

func (api *FlumeTokensAPI) Erc20ByAccount(ctx *rpc.CallContext, addr common.Address, offset *int) (*paginator[common.Address], error) {
	
	if offset == nil {
		offset = new(int)
	}

	heavyResult := make(chan *paginator[common.Address])
	errChan := make(chan error)

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("flume_erc20ByAccount sent to flume heavy by default")
		missMeter.Mark(1)
		go func(offset *int) {
			address, err := heavy.CallHeavyDiscrete[*paginator[common.Address]](ctx.Context(), api.cfg.HeavyServer, api.cfg.EarliestBlock, "flume_erc20ByAccount", addr, offset)
			if err != nil {
				log.Error("Error processing request in flume_erc20ByAccount", "err", err) 
				errChan <- err
			}
			if address != nil {
				heavyResult <- *address
			}
		}(offset)
	} else {
		close(heavyResult)
	}

	tctx, cancel := context.WithTimeout(ctx.Context(), 5*time.Second)
	defer cancel()

	topic0 := types.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

	var rows *sql.Rows
	var err error

	if ctx.Latest > 0 {
		rows, err = api.db.QueryContext(tctx, `SELECT distinct(address) FROM event_logs INDEXED BY topic2_partial WHERE topic0 = ? AND topic2 = ? AND topic3 IS NULL AND block < ? LIMIT 1000 OFFSET ?;`, trimPrefix(topic0.Bytes()), trimPrefix(addr.Bytes()), ctx.Latest, offset)
	} else {
		rows, err = api.db.QueryContext(tctx, `SELECT distinct(address) FROM event_logs INDEXED BY topic2_partial WHERE topic0 = ? AND topic2 = ? AND topic3 IS NULL LIMIT 1000 OFFSET ?;`, trimPrefix(topic0.Bytes()), trimPrefix(addr.Bytes()), offset)
	}
	if err != nil {
		exhaustChannels[*paginator[common.Address]](heavyResult, errChan)
		log.Error("Error getting account addresses erc20ByAccount", "err", err)
		return nil, fmt.Errorf("database error")
	}
	defer rows.Close()
	addresses := []common.Address{}
	for rows.Next() {
		var addrBytes []byte
		err := rows.Scan(&addrBytes)
		if err != nil {
			exhaustChannels[*paginator[common.Address]](heavyResult, errChan)
			log.Error("scan error erc20ByAccount", "err", err)
			return nil, fmt.Errorf("database error")
		}
		addresses = append(addresses, bytesToAddress(addrBytes))
	}
	if err := rows.Err(); err != nil {
		exhaustChannels[*paginator[common.Address]](heavyResult, errChan)
		log.Error("Row error erc20ByAccount", "err", err)
		return nil, fmt.Errorf("database error")
	}
	result := paginator[common.Address]{Items: addresses}
	if len(addresses) == 1000 {
		result.Token = *offset + len(addresses)
	}

	select {
		case hr, ok := <- heavyResult:
			if ok {
				result.Items = dedup[common.Address](hr.Items, result.Items)
				if result.Token == nil {
					result.Token = hr.Token
				}
			}
		case err := <- errChan:
			return nil, err
	}

	return &result, nil
}

func (api *FlumeTokensAPI) Erc20Holders(ctx *rpc.CallContext, addr common.Address, offset *int) (*paginator[common.Address], error) {
	
	if offset == nil {
		offset = new(int)
	}

	heavyResult := make(chan *paginator[common.Address])
	errChan := make(chan error)
	
	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("flume_erc20Holders sent to flume heavy by default")
		missMeter.Mark(1)
		go func(offset *int) {
			address, err := heavy.CallHeavyDiscrete[*paginator[common.Address]](ctx.Context(), api.cfg.HeavyServer, api.cfg.EarliestBlock, "flume_erc20Holders", addr, offset)
			if err != nil {
				log.Error("Error processing request in flume_erc20Holders", "err", err) 
				errChan <- err
			}
			if address != nil {
				heavyResult <- *address
			}
		}(offset)
	} else {
		close(heavyResult)
	}

	tctx, cancel := context.WithTimeout(ctx.Context(), 5*time.Second)
	defer cancel()


	topic0 := types.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
	// topic0 must match ERC20, topic3 must be empty (to exclude ERC721) and topic2 is the recipient address
	
	var rows *sql.Rows
	var err error

	if ctx.Latest > 0 {
		rows, err = api.db.QueryContext(tctx, `SELECT distinct(topic2) FROM logs.event_logs INDEXED BY address_compound WHERE topic0 = ? AND address = ? AND block < ? AND topic3 IS NULL LIMIT 1000 OFFSET ?;`, trimPrefix(topic0.Bytes()), trimPrefix(addr.Bytes()), ctx.Latest, offset)
	} else {
		rows, err = api.db.QueryContext(tctx, `SELECT distinct(topic2) FROM logs.event_logs INDEXED BY address_compound WHERE topic0 = ? AND address = ? AND topic3 IS NULL LIMIT 1000 OFFSET ?;`, trimPrefix(topic0.Bytes()), trimPrefix(addr.Bytes()), offset)
	}
	if err != nil {
		exhaustChannels[*paginator[common.Address]](heavyResult, errChan)
		log.Error("Error getting account addresses erc20Holders", "err", err)
		return nil, fmt.Errorf("database error")
	}
	defer rows.Close()
	addresses := []common.Address{}
	for rows.Next() {
		var addrBytes []byte
		err := rows.Scan(&addrBytes)
		if err != nil {
			exhaustChannels[*paginator[common.Address]](heavyResult, errChan)
			log.Error("scan error erc20Holders", "err", err)
			return nil, fmt.Errorf("database error")
		}
		addresses = append(addresses, bytesToAddress(addrBytes))
	}
	if err := rows.Err(); err != nil {
		exhaustChannels[*paginator[common.Address]](heavyResult, errChan)
		log.Error("row error erc20Holders", "err", err)
		return nil, fmt.Errorf("database error")
	}
	result := paginator[common.Address]{Items: addresses}
	if len(addresses) == 1000 {
		result.Token = *offset + len(addresses)
	}

	select {
		case hr, ok := <- heavyResult:
			if ok {
				result.Items = dedup[common.Address](hr.Items, result.Items)
				if result.Token == nil {
					result.Token = hr.Token
				}
			}
		case err := <- errChan:
			return nil, err
	}

	return &result, nil
}
