package migrations

import (
	"database/sql"
	log "github.com/inconshreveable/log15"
)

const (
	maxInt = 9223372036854775807
)

func MigrateBlocks(db *sql.DB, chainid uint64) error {
	var tableName string
	db.QueryRow("SELECT name FROM blocks.sqlite_master WHERE type='table' and name='migrations';").Scan(&tableName)
	if tableName != "migrations" {
		db.Exec("CREATE TABLE blocks.migrations (version integer PRIMARY KEY);")

		db.Exec("INSERT INTO blocks.migrations(version) VALUES (0);")
	}
	var schemaVersion uint
	db.QueryRow("SELECT version FROM blocks.migrations;").Scan(&schemaVersion)
	if schemaVersion < 1 {
		log.Info("Applying blocks v1 migration")
		db.Exec(`CREATE TABLE blocks.blocks (
		      number      BIGINT PRIMARY KEY,
		      hash        varchar(32) UNIQUE,
		      parentHash  varchar(32),
		      uncleHash   varchar(32),
		      coinbase    varchar(20),
		      root        varchar(32),
		      txRoot      varchar(32),
		      receiptRoot varchar(32),
		      bloom       blob,
		      difficulty  varchar(32),
		      gasLimit    BIGINT,
		      gasUsed     BIGINT,
		      time        BIGINT,
		      extra       blob,
		      mixDigest   varchar(32),
		      nonce       BIGINT,
		      uncles      blob,
		      size        BIGINT,
		      td          varchar(32),
		      baseFee varchar(32))`)

		db.Exec(`CREATE TABLE blocks.cardinal_offsets (
			partition INT, 
			offset BIGINT, 
			topic STRING, 
			PRIMARY KEY (topic, partition))`)

		db.Exec(`CREATE TABLE blocks.issuance (
					startBlock     BIGINT,
					endBlock       BIGINT,
					value          BIGINT
					)`)
		if _, err := db.Exec(`CREATE INDEX blocks.coinbase ON blocks(coinbase)`); err != nil {
			log.Error("Migrate Blocks CREATE INDEX error", "err", err.Error())
			return nil
		}

		if _, err := db.Exec(`CREATE INDEX blocks.timestamp ON blocks(time)`); err != nil {
			log.Error("Migrate Blocks CREATE INDEX error", "err", err.Error())
			return nil
		}
		db.Exec(`UPDATE blocks.migrations SET version = 1;`)

	} 
	if schemaVersion < 2 {
		log.Info("Applying blocks v2 migration")

		if _, err := db.Exec(`CREATE INDEX blocks.bkHash ON blocks(hash);`); err != nil {
			log.Error("Migrate blocks CREATE INDEX bkHash On blocks error", "err", err.Error())
			return nil
		}
		db.Exec("UPDATE blocks.migrations SET version = 2;")
		log.Info("blocks migrations done")
	}

	log.Info("blocks migration up to date")
	return nil
}

func MigrateTransactions(db *sql.DB, chainid uint64) error {
	var tableName string
	db.QueryRow("SELECT name FROM transactions.sqlite_master WHERE type='table' and name='migrations';").Scan(&tableName)
	if tableName != "migrations" {
		db.Exec("CREATE TABLE transactions.migrations (version integer PRIMARY KEY);")

		db.Exec("INSERT INTO transactions.migrations(version) VALUES (0);")
	}
	var schemaVersion uint
	db.QueryRow("SELECT version FROM transactions.migrations;").Scan(&schemaVersion)
	if schemaVersion < 1 {
		log.Info("Applying transacitons v1 migration")
		db.Exec(`CREATE TABLE transactions.transactions (
		      id INTEGER PRIMARY KEY AUTOINCREMENT,
		      gas BIGINT,
		      gasPrice BIGINT,
		      hash varchar(32) UNIQUE,
		      input blob,
		      nonce BIGINT,
		      recipient varchar(20),
		      transactionIndex MEDIUMINT,
		      value varchar(32),
		      v SMALLINT,
		      r varchar(32),
		      s varchar(32),
		      sender varchar(20),
		      func varchar(4),
		      contractAddress varchar(20),
		      cumulativeGasUsed BIGINT,
		      gasUsed BIGINT,
		      logsBloom blob,
		      status TINYINT,
		      block BIGINT,
		      type TINYINT,
		      access_list blob,
		      gasFeeCap varchar(32),
		      gasTipCap varchar(32))`)

		if _, err := db.Exec(`CREATE INDEX transactions.txblock ON transactions(block)`); err != nil {
			log.Error("Migrate Transactions CREATE INDEX txblock error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`CREATE INDEX transactions.recipient_partial ON transactions(recipient) WHERE recipient IS NOT NULL`); err != nil {
			log.Error("Migrate Transactions CREATE INDEX receipient error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`CREATE INDEX transactions.contractAddress_partial ON transactions(contractAddress) WHERE contractAddress IS NOT NULL`); err != nil {
			log.Error("Migrate Transactions CREATE INDEX contractAddress_partial error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`CREATE INDEX transactions.senderNonce ON transactions(sender, nonce)`); err != nil {
			log.Error("Migrate Transactions CREATE INDEX senderNonce error", "err", err.Error())
			return nil
		}
		db.Exec(`UPDATE transactions.migrations SET version = 1;`)

	} 
	
	if schemaVersion < 2 {
		log.Info("Applying transactions v2 migration")

		if _, err := db.Exec(`CREATE INDEX transactions.txHash ON transactions(hash);`); err != nil {
			log.Error("Migrate transactions CREATE INDEX txHash On transactions error", "err", err.Error())
			return nil
		}
		db.Exec("UPDATE transactions.migrations SET version = 2;")
		log.Info("transacitons migrations done")
	}

	log.Info("transactions migrations up to date")
	return nil
}

func MigrateLogs(db *sql.DB, chainid uint64) error {
	var tableName string
	db.QueryRow("SELECT name FROM logs.sqlite_master WHERE type='table' and name='migrations';").Scan(&tableName)
	if tableName != "migrations" {
		db.Exec("CREATE TABLE logs.migrations (version integer PRIMARY KEY);")

		db.Exec("INSERT INTO logs.migrations(version) VALUES (0);")
	}
	var schemaVersion uint
	db.QueryRow("SELECT version FROM logs.migrations;").Scan(&schemaVersion)
	if schemaVersion < 1 {
		log.Info("Applying logs v1 migration")
		db.Exec(`CREATE TABLE logs.event_logs (
		      address varchar(20),
		      topic0 varchar(32),
		      topic1 varchar(32),
		      topic2 varchar(32),
		      topic3 varchar(32),
		      data blob,
		      block BIGINT,
		      logIndex MEDIUMINT,
		      transactionHash varchar(32),
		      transactionIndex varchar(32),
		      blockHash varchar(32),
		      PRIMARY KEY (block, logIndex)
		    )`)
		if _, err := db.Exec(`CREATE INDEX logs.address_compound ON event_logs(address, block)`); err != nil {
			log.Error("Migrate Logs CREATE INDEX address_compound error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`CREATE INDEX logs.topic0_compound ON event_logs(topic0, block)`); err != nil {
			log.Error("Migrate Logs CREATE INDEX topic0_compound error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`CREATE INDEX logs.topic1_partial ON event_logs(topic1, topic0, address, block) WHERE topic1 IS NOT NULL`); err != nil {
			log.Error("Migrate Logs CREATE INDEX topic1_compound error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`CREATE INDEX logs.topic2_partial ON event_logs(topic2, topic0, address, block) WHERE topic2 IS NOT NULL`); err != nil {
			log.Error("Migrate Logs CREATE INDEX topic2_compound error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`CREATE INDEX logs.topic3_partial ON event_logs(topic3, topic0, address, block) WHERE topic3 IS NOT NULL`); err != nil {
			log.Error("Migrate Logs CREATE INDEX topic3_compound error", "err", err.Error())
			return nil
		}

		db.Exec(`UPDATE logs.migrations SET version = 1;`)
		log.Info("logs migrations done")
	}

	log.Info("logs migrations up to date")
	return nil
}

func MigrateMempool(db *sql.DB, chainid uint64) error {
	var tableName string
	db.QueryRow("SELECT name FROM mempool.sqlite_master WHERE type='table' and name='migrations';").Scan(&tableName)
	if tableName != "migrations" {
		db.Exec("CREATE TABLE mempool.migrations (version integer PRIMARY KEY);")
		db.Exec("INSERT INTO mempool.migrations(version) VALUES (0);")
	}
	var schemaVersion uint
	db.QueryRow("SELECT version FROM mempool.migrations;").Scan(&schemaVersion)
	if schemaVersion < 1 {
		log.Info("Applying mempool v1 migration")
		db.Exec(`CREATE TABLE mempool.transactions (
			gas BIGINT,
			gasPrice BIGINT,
			gasFeeCap varchar(32),
			gasTipCap varchar(32),
			hash varchar(32) UNIQUE,
			input blob,
			nonce BIGINT,
			recipient varchar(20),
			value varchar(32),
			v SMALLINT,
			r varchar(32),
			s varchar(32),
			sender varchar(20),
			type TINYINT,
			access_list blob);`)

		if _, err := db.Exec(`CREATE INDEX mempool.sender ON transactions(sender, nonce);`); err != nil {
			log.Error("Migrate mempool CREATE INDEX sender error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`CREATE INDEX mempool.recipient ON transactions(recipient);`); err != nil {
			log.Error("Migrate Logs CREATE INDEX recipient rror", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`CREATE INDEX mempool.gasPrice ON transactions(gasPrice);`); err != nil {
			log.Error("Migrate Logs CREATE INDEX gasPrice error", "err", err.Error())
			return nil
		}
		db.Exec(`UPDATE mempool.migrations SET version = 1;`)
		log.Info("mempool migrations done")
	}

	log.Info("mempool migrations up to date")
	return nil
}
