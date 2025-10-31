package api

import (
	"fmt"
	"context"
	"database/sql"

	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-flume/config"
)

type CardinalAPI struct {
	db      *sql.DB
	network uint64
	cfg     *config.Config
}

func NewCardinalAPI(db *sql.DB, network uint64, cfg *config.Config) *CardinalAPI {
	return &CardinalAPI{
		db:      db,
		network: network,
		cfg:     cfg,
	}
}

func (api *CardinalAPI) ForkReady(ctx context.Context, forkname string) int {
	result := -1

	switch forkname {
		case "osaka":
			// for osaka our heuristic is based on implications from the blocks database, migrations, blocks, and blobSchedule tables
			var migration int
			migrationStatement := "SELECT version from blocks.migrations"
			api.db.QueryRow(migrationStatement).Scan(&migration)
			// migration 10 was made to support the osaka hardfork
			if migration >= 10 {
				var count int
				statement := "SELECT count(*) FROM blocks.blobSchedule WHERE updateFrac = ?;"
				api.db.QueryRow(statement, 5007716).Scan(&count)
				// this updateFrac value was introduced with prague, if migration is >= 10 but this value is not present in the database then the application is not configured to support the hardfork on this network
				if count > 1 {
					var initialized int
					initStatement := fmt.Sprint(`SELECT 1 FROM blocks.blobSchedule AS t1 JOIN blocks.blocks AS t2 ON t1.startTime >= t2.time
					WHERE t1.startTime = (SELECT startTime FROM blocks.blobSchedule WHERE updateFrac = ? ORDER BY endTime DESC LIMIT 1)
					AND t2.number = ?
					LIMIT 1;`)
					if err := api.db.QueryRow(initStatement, 5007716, api.cfg.LatestBlock).Scan(&initialized); err != nil {
						log.Error("error returned from initialized statement, cardinal forkReady", "err", err)
						return result
					}
					// if the greatest starttime value is greater than highest block's time then the application is configured for the hardfork but it has not occured yet
					// Also this updateFrac value exists for both prague and osaka and so the comparison needs to made to the osaka row
					if initialized > 0 {
						result = 1
					} else {
						result = 2
					}
				} else {
					result = 0
				}
			}
	}
	return result 
}