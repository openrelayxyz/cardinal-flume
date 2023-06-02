package api

import (
	"database/sql"
	"github.com/openrelayxyz/cardinal-evm/common"
	// log "github.com/inconshreveable/log15"
)

var (
	badAddressValues = map[common.Address]string{}
)

func LoadIndexHints(db *sql.DB) error {
	// rows, err := db.Query(`SELECT address FROM logs.address_hints;`)
	// if err != nil {
	// 	log.Error("Error getting hint addresses", "err", err.Error())
	// 	return err
	// }
	// defer rows.Close()
	// for rows.Next() {
	// 	var addrBytes []byte
	// 	if err := rows.Scan(&addrBytes); err != nil {
	// 		return err
	// 	}
	// 	badAddressValues[common.BytesToAddress(addrBytes)] = "+"
	// }
	return nil
}