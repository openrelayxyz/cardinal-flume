package migrations

import (
	"fmt"
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
		if _, err := db.Exec("CREATE TABLE blocks.migrations (version integer PRIMARY KEY);"); err != nil {
			log.Error("migrations CREATE TABLE blocks.migrations error", "err", err.Error())
		}
		if _, err := db.Exec("INSERT INTO blocks.migrations(version) VALUES (0);"); err != nil {
			log.Error("migrations INSERT INTO blocks.migrations error", "err", err.Error())
		}
	}
	var schemaVersion uint
	db.QueryRow("SELECT version FROM blocks.migrations;").Scan(&schemaVersion)
	if schemaVersion < 1 {
		log.Info("Applying blocks v1 migration")
		if _, err := db.Exec(`CREATE TABLE blocks.blocks (
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
		    baseFee varchar(32))`); err != nil {
			log.Error("migrations CREATE TABLE blocks.blocks error", "err", err.Error())
			return nil
			}
		if _, err := db.Exec(`CREATE TABLE blocks.cardinal_offsets (
			partition INT,
			offset BIGINT,
			topic STRING,
			PRIMARY KEY (topic, partition))`) ; err != nil {
			log.Error("migrations CREATE TABLE blocks.cardinal_offsets error", "err", err.Error())
			return nil
			}
		if _, err := db.Exec(`CREATE TABLE blocks.issuance (
					startBlock     BIGINT,
					endBlock       BIGINT,
					value          BIGINT
					)`); err != nil {
			log.Error("migrations CREATE TABLE blocks.issuance error", "err", err.Error())
			return nil
			}
		if _, err := db.Exec(`CREATE INDEX blocks.coinbase ON blocks(coinbase)`); err != nil {
			log.Error("migrations CREATE INDEX blocks.coinbase error", "err", err.Error())
			return nil
		}

		if _, err := db.Exec(`CREATE INDEX blocks.timestamp ON blocks(time)`); err != nil {
			log.Error("migrations CREATE INDEX blocks.timestamp error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`UPDATE blocks.migrations SET version = 1;`); err != nil {
			log.Error("migrations UPDATE blocks.migrations v1 error", "err", err.Error())
		}
		log.Info("blocks v1 migrations done")

	}
	if schemaVersion < 2 {
		log.Info("Applying blocks v2 migration")

		if _, err := db.Exec(`CREATE INDEX blocks.bkHash ON blocks(hash);`); err != nil {
			log.Error("migrations CREATE INDEX blocks.bkHash error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec("UPDATE blocks.migrations SET version = 2;"); err != nil {
			log.Error("migrations UPDATE blocks.migrations v2 error", "err", err.Error())
		}
		log.Info("blocks v2 migrations done")
	}
	if schemaVersion < 3 {
		log.Info("Applying blocks v3 migration")
		if _, err := db.Exec(`CREATE INDEX blocks.coinbaseCompound ON blocks(coinbase, number)`); err != nil {
			log.Error("migrations CREATE INDEX blocks.coinbaseCompound error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`DROP INDEX blocks.coinbase;`); err != nil {
			log.Error("migrations DROP INDEX blocks.coinbase error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec("UPDATE blocks.migrations SET version = 3;"); err != nil {
			log.Error("migrations UPDATE blocks.migrations v3 error", "err", err.Error())
		}
		log.Info("blocks v3 migrations done")
	}
	if schemaVersion < 4 {
		log.Info("Applying blocks v4 migration")
		if _, err := db.Exec(`ALTER TABLE blocks.blocks ADD COLUMN withdrawalHash VARCHAR(32)`); err != nil {
				log.Error("migrations ALTER TABLE blocks.blocks ADD COLUMN withdrawalHash error", "err", err.Error())
				return nil
			}
		if _, err := db.Exec(`CREATE TABLE blocks.withdrawals (
				wtdrlIndex MEDIUMINT,
				vldtrIndex MEDIUMINT,
				address VARCHAR(20),
				amount    MEDIUMINT,
				block     BIGINT,
				blockHash VARCHAR(32),
				PRIMARY KEY (block, wtdrlIndex))`); err != nil {
				log.Error("migrations CREATE TABLE blocks.withdrawals", "err", err.Error())
				return nil
				}
		if _, err := db.Exec(`CREATE INDEX blocks.addressBlock ON withdrawals(address, block)`); err != nil {
			log.Error("migrations CREATE INDEX blocks.addressBlock error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`CREATE INDEX blocks.blockHash ON withdrawals(blockHash)`); err != nil {
			log.Error("migrations CREATE INDEX blocks.blockHash error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec("UPDATE blocks.migrations SET version = 4;"); err != nil {
			log.Error("migrations UPDATE blocks.migrations v4 error", "err", err.Error())
			return nil
		}
		log.Info("blocks v4 migrations done")
	}
	if schemaVersion < 5 {
		log.Info("Applying blocks v5 migration")
		if _, err := db.Exec(`ALTER TABLE blocks.blocks ADD COLUMN blobGasUsed BIGINT`); err != nil {
			log.Error("migrations ALTER TABLE blocks.blocks blobGasUsed error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`ALTER TABLE blocks.blocks ADD COLUMN excessBlobGas BIGINT`); err != nil {
			log.Error("migrations ALTER TABLE blocks.blocks excessBlobGas error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`ALTER TABLE blocks.blocks ADD COLUMN parentBeaconRoot varchar(32)`); err != nil {
			log.Error("migrations ALTER TABLE blocks.blocks parentBeaconRoot error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec("UPDATE blocks.migrations SET version = 5;"); err != nil {
			log.Error("migrations UPDATE blocks.migrations v5 error", "err", err.Error())
			return nil
		}
		log.Info("blocks v5 migrations done")
	}
	if schemaVersion < 6 {
		log.Info("Applying blocks v6 migration")
		if _, err := db.Exec(`ALTER TABLE blocks.blocks ADD COLUMN requestsHash varchar(32)`); err != nil {
			log.Error("migrations ALTER TABLE blocks.blocks requestsHash error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec("UPDATE blocks.migrations SET version = 6;"); err != nil {
			log.Error("migrations UPDATE blocks.migrations v6 error", "err", err.Error())
			return nil
		}
		log.Info("blocks v6 migrations done")
	}
	if schemaVersion < 7 {
		log.Info("Applying blocks v7 migration")
		if _, err := db.Exec(`CREATE TABLE blocks.blobSchedule (
			startTime     BIGINT,
			endTime       BIGINT,
			target        INT,
			max           INT,
			updateFrac    BIGINT
			)`); err != nil {
			log.Error("migrations CREATE TABLE blocks.blobSchedule error", "err", err.Error())
			return nil
		}
		switch chainid {
		case 1:
			if _, err := db.Exec(fmt.Sprintf(`INSERT INTO blocks.blobSchedule(startTime, endTime, target, max, updateFrac) VALUES (%v, %v, %v, %v, %v)`, 1710338135, 9223372036854775807, 3, 6, 3338477)); err != nil {
				log.Error("migrations mainnet INSERT INTO blocks.blobSchedule v1 error", "err", err.Error())
				return nil
			}
			if _, err := db.Exec(fmt.Sprintf(`INSERT INTO blocks.blobSchedule(startTime, endTime, target, max, updateFrac) VALUES (%v, %v, %v, %v, %v)`, maxInt, maxInt, 6, 9, 5007716)); err != nil {
				log.Error("migrations mainnet INSERT INTO blocks.blobSchedule v2 error", "err", err.Error())
				return nil
			}
		case 11155111:
			if _, err := db.Exec(fmt.Sprintf(`INSERT INTO blocks.blobSchedule(startTime, endTime, target, max, updateFrac) VALUES (%v, %v, %v, %v, %v)`, 1706655072, 1741159775, 3, 6, 3338477)); err != nil {
				log.Error("migrations sepolia INSERT INTO blocks.blobSchedule v1 error", "err", err.Error())
				return nil
			}
			if _, err := db.Exec(fmt.Sprintf(`INSERT INTO blocks.blobSchedule(startTime, endTime, target, max, updateFrac) VALUES (%v, %v, %v, %v, %v)`, 1741159776, maxInt, 6, 9, 5007716)); err != nil {
				log.Error("migrations sepolia INSERT INTO blocks.blobSchedule  v2 error", "err", err.Error())
				return nil
			}
		case 17000:
			if _, err := db.Exec(fmt.Sprintf(`INSERT INTO blocks.blobSchedule(startTime, endTime, target, max, updateFrac) VALUES (%v, %v, %v, %v, %v)`, 1707305664, 1740434111, 3, 6, 3338477)); err != nil {
				log.Error("migrations holesky INSERT INTO blocks.blobSchedule v1 error", "err", err.Error())
				return nil
			}
			if _, err := db.Exec(fmt.Sprintf(`INSERT INTO blocks.blobSchedule(startTime, endTime, target, max, updateFrac) VALUES (%v, %v, %v, %v, %v)`, 1740434112, maxInt, 6, 9, 5007716)); err != nil {
				log.Error("migrations holesky INSERT INTO blocks.blobSchedule v2 error", "err", err.Error())
				return nil
			}
		default:
			if _, err := db.Exec(fmt.Sprintf(`INSERT INTO blocks.blobSchedule(startTime, endTime, target, max, updateFrac) VALUES (%v, %v, %v, %v, %v)`, maxInt, maxInt, 1, 1, 1)); err != nil {
				log.Error("migrations default INSERT INTO blocks.blobSchedule v1 error", "err", err.Error())
				return nil
			}
			if _, err := db.Exec(fmt.Sprintf(`INSERT INTO blocks.blobSchedule(startTime, endTime, target, max, updateFrac) VALUES (%v, %v, %v, %v, %v)`, maxInt, maxInt, 1, 1, 1)); err != nil {
				log.Error("migrations default INSERT INTO blocks.blobSchedule v2 error", "err", err.Error())
				return nil
			}
		}
		if _, err := db.Exec("UPDATE blocks.migrations SET version = 7;"); err != nil {
			log.Error("migrations UPDATE blocks.migrations v7 error", "err", err.Error())
			return nil
		}
		log.Info("blocks v7 migrations done")
	}

	log.Info("blocks migration up to date")
	return nil
}

func MigrateTransactions(db *sql.DB, chainid uint64) error {
	var tableName string
	db.QueryRow("SELECT name FROM transactions.sqlite_master WHERE type='table' and name='migrations';").Scan(&tableName)
	if tableName != "migrations" {
		if _, err := db.Exec("CREATE TABLE transactions.migrations (version integer PRIMARY KEY);"); err != nil {
			log.Error("migrations CREATE TABLE transactions.migrations error", "err", err.Error())
		}

		if _, err := db.Exec("INSERT INTO transactions.migrations(version) VALUES (0);"); err != nil {
			log.Error("migrations INSERT INTO transactions.migrations v1 error", "err", err.Error())
		}
	}
	var schemaVersion uint
	db.QueryRow("SELECT version FROM transactions.migrations;").Scan(&schemaVersion)
	if schemaVersion < 1 {
		log.Info("Applying transacitons v1 migration")
		// See note 1 *below
		if _, err := db.Exec(`CREATE TABLE transactions.transactions (
		    id INTEGER PRIMARY KEY AUTOINCREMENT,
		    gas BIGINT,
		    gasPrice BIGINT,
		    hash varchar(32),
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
		    gasTipCap varchar(32))`); err != nil {
			log.Error("migrations CREATE TABLE transactions.transactions error", "err", err.Error())
			}

		if _, err := db.Exec(`CREATE INDEX transactions.txblock ON transactions(block)`); err != nil {
			log.Error("migrations CREATE INDEX transactions.txblock error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`CREATE INDEX transactions.recipient_partial ON transactions(recipient) WHERE recipient IS NOT NULL`); err != nil {
			log.Error("migrations CREATE INDEX transactions.receipient error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`CREATE INDEX transactions.contractAddress_partial ON transactions(contractAddress) WHERE contractAddress IS NOT NULL`); err != nil {
			log.Error("migrations CREATE INDEX transactions.contractAddress_partial error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`CREATE INDEX transactions.senderNonce ON transactions(sender, nonce)`); err != nil {
			log.Error("migrations CREATE INDEX transactions.senderNonce error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`UPDATE transactions.migrations SET version = 1;`); err != nil {
			log.Error("migrations UPDATE transactions.migrations v1 error", "err", err.Error())
		}
		log.Info("transacitons migrations v1 done")
	}

	if schemaVersion < 2 {
		log.Info("Applying transactions v2 migration")

		if _, err := db.Exec(`CREATE INDEX transactions.txHash ON transactions(hash);`); err != nil {
			log.Error("migrations CREATE INDEX transactions.txHash error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec("UPDATE transactions.migrations SET version = 2;"); err != nil {
			log.Error("migrations UPDATE transactions.migrations v2 error", "err", err.Error())
		}
		log.Info("transacitons migrations v2 done")
	}
	if schemaVersion < 3 {
		log.Info("Applying transactions v3 migration")
		if _, err := db.Exec(`ALTER TABLE transactions.transactions ADD COLUMN maxFeePerBlobGas varchar(32)`); err != nil {
			log.Error("migrations ALTER TABLE transactions.transactions maxFeePerBlobGas error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`ALTER TABLE transactions.transactions ADD COLUMN blobVersionedHashes blob`); err != nil {
			log.Error("migrations ALTER TABLE transactions.transactions blobVersionedHashes error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec("UPDATE transactions.migrations SET version = 3;"); err != nil {
			log.Error("migrations UPDATE transactions.migrations v3 error", "err", err.Error())
		}
		log.Info("transacitons migrations v3 done")
	}
	if schemaVersion < 4 {
		log.Info("Applying transactions v4 migration")
		if _, err := db.Exec(`ALTER TABLE transactions.transactions ADD COLUMN authListBytes blob`); err != nil {
			log.Error("migrations ALTER TABLE transactions.transactions authListBytes error", "err", err.Error())
			return nil
		}
		//YOU ARE HERE PHILIP!!!!!
		if _, err := db.Exec("UPDATE transactions.migrations SET version = 4;"); err != nil {
			log.Error("migrations UPDATE transactions.migrations v4 error", "err", err.Error())
		}
		log.Info("transacitons migrations v4 done")
	}
	
	log.Info("transactions migrations up to date")
	return nil
}

func MigrateLogs(db *sql.DB, chainid uint64) error {
	var tableName string
	db.QueryRow("SELECT name FROM logs.sqlite_master WHERE type='table' and name='migrations';").Scan(&tableName)
	if tableName != "migrations" {
		if _, err := db.Exec("CREATE TABLE logs.migrations (version integer PRIMARY KEY);"); err != nil {
			log.Error("migrations CREATE TABLE logs.migrations error", "err", err.Error())
		}

		if _, err := db.Exec("INSERT INTO logs.migrations(version) VALUES (0);"); err != nil {
			log.Error("migrations INSERT INTO log.migraions error", "err", err.Error())
		}
	}
	var schemaVersion uint
	db.QueryRow("SELECT version FROM logs.migrations;").Scan(&schemaVersion)
	if schemaVersion < 1 {
		log.Info("Applying logs v1 migration")
		if _, err := db.Exec(`CREATE TABLE logs.event_logs (
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
		    )`); err != nil {
			log.Error("migrations CREATE TABLE logs.event_logs error", "err", err.Error())
			}
		if _, err := db.Exec(`CREATE INDEX logs.address_compound ON event_logs(address, block)`); err != nil {
			log.Error("migrations CREATE INDEX logs.address_compound error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`CREATE INDEX logs.topic0_compound ON event_logs(topic0, block)`); err != nil {
			log.Error("migrations CREATE INDEX logs.topic0_compound error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`CREATE INDEX logs.topic1_partial ON event_logs(topic1, topic0, address, block) WHERE topic1 IS NOT NULL`); err != nil {
			log.Error("migrations CREATE INDEX logs.topic1_compound error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`CREATE INDEX logs.topic2_partial ON event_logs(topic2, topic0, address, block) WHERE topic2 IS NOT NULL`); err != nil {
			log.Error("migrations CREATE INDEX logs.topic2_compound error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`CREATE INDEX logs.topic3_partial ON event_logs(topic3, topic0, address, block) WHERE topic3 IS NOT NULL`); err != nil {
			log.Error("migrations CREATE INDEX logs.topic3_compound error", "err", err.Error())
			return nil
		}

		if _, err := db.Exec(`UPDATE logs.migrations SET version = 1;`); err != nil {
			log.Error("migrations UPDATE log.migrations error", "err", err.Error())
		}
		log.Info("logs migrations v1 done")
	}
	if schemaVersion < 2 {
		if _, err := db.Exec(`CREATE TABLE logs.address_hints (
			address varchar(20)
		);`); err != nil {
			log.Error("Migrate Logs CREATE ADDRESS address_hints error", "err", err.Error())
			return nil
		}
		db.Exec(`UPDATE logs.migrations SET version = 2;`)
		log.Info("logs migrations v2 done")
	}
	if schemaVersion < 3 {
		if _, err := db.Exec(`CREATE INDEX logs.address_topic0_compound ON event_logs(address, topic0, block);`); err != nil {
			log.Error("Migrate Logs CREATE INDEX logs.addresstopic0_compound error", "err", err.Error())
			return nil
		}
		db.Exec(`UPDATE logs.migrations SET version = 3;`)
		log.Info("logs migrations v3 done")
	}
	if schemaVersion < 4 {
		if _, err := db.Exec(`ANALYZE migrations;`); err != nil {
			log.Error("Migrate Logs ANALYZE migrations error;", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`INSERT INTO "sqlite_stat1" VALUES('event_logs','address_topic0_compound','937459904 73 22 11');`); err != nil {
			log.Error("INSERT sqlite_stat1, address_topic0_compound error;", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`INSERT INTO "sqlite_stat1" VALUES('event_logs','topic3_partial','946568192 500001 4976 540 2');`); err != nil {
			log.Error("INSERT sqlite_stat1, topic3_partial error;", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`INSERT INTO "sqlite_stat1" VALUES('event_logs','topic2_partial','414026412 500001 2545 62 2');`); err != nil {
			log.Error("INSERT sqlite_stat1, topic2_partial error;", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`INSERT INTO "sqlite_stat1" VALUES('event_logs','topic1_partial','3472777216 500001 3876 30 2');`); err != nil {
			log.Error("INSERT sqlite_stat1, topic1_partial error;", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`INSERT INTO "sqlite_stat1" VALUES('event_logs','topic0_compound','812632744 166667 3 noskipscan');`); err != nil {
			log.Error("INSERT sqlite_stat1, topic0_compound error;", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`INSERT INTO "sqlite_stat1" VALUES('event_logs','address_compound','596825792 73 23');`); err != nil {
			log.Error("INSERT sqlite_stat1, address_compound error;", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`INSERT INTO "sqlite_stat1" VALUES('event_logs','sqlite_autoindex_event_logs_1','1698316288 601 1');`); err != nil {
			log.Error("INSERT sqlite_stat1, sqlite_autoindex_event_logs_1 error;", "err", err.Error())
			return nil
		}
		db.Exec(`UPDATE logs.migrations SET version = 4;`)
		log.Info("logs migrations v4 done")
	}

	log.Info("logs migrations up to date")
	return nil
}

func MigrateMempool(db *sql.DB, chainid uint64) error {
	var tableName string
	db.QueryRow("SELECT name FROM mempool.sqlite_master WHERE type='table' and name='migrations';").Scan(&tableName)
	if tableName != "migrations" {
		if _, err := db.Exec("CREATE TABLE mempool.migrations (version integer PRIMARY KEY);"); err != nil {
			log.Error("migrations CREATE TABLE mempoool.migrations error", "err", err.Error())
		}
		if _, err := db.Exec("INSERT INTO mempool.migrations(version) VALUES (0);"); err != nil {
			log.Error("migrations INSERT INTO mempool.migrations error", "err", err.Error())
		}
	}
	var schemaVersion uint
	db.QueryRow("SELECT version FROM mempool.migrations;").Scan(&schemaVersion)
	if schemaVersion < 1 {
		log.Info("Applying mempool v1 migration")
		if _, err := db.Exec(`CREATE TABLE mempool.transactions (
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
			access_list blob);`); err != nil {
			log.Error("migrationsCREATE TABLE mempool.transactions error", "err", err.Error())
			}

		if _, err := db.Exec(`CREATE INDEX mempool.sender ON transactions(sender, nonce);`); err != nil {
			log.Error("migrations CREATE INDEX mempool.sender error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`CREATE INDEX mempool.recipient ON transactions(recipient);`); err != nil {
			log.Error("migrations CREATE INDEX mempool.recipient rror", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`CREATE INDEX mempool.gasPrice ON transactions(gasPrice);`); err != nil {
			log.Error("migrations CREATE INDEX mempool.gasPrice error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`UPDATE mempool.migrations SET version = 1;`); err != nil {
			log.Error("migrationsUPDATE mempool.migraitons error", "err", err.Error())
		}
		log.Info("mempool migrations v1 done")
	}

	if schemaVersion < 2 {
		log.Info("Applying mempool v2 migration")

		if _, err := db.Exec(`ALTER TABLE mempool.transactions ADD time BIGINT;`); err != nil {
			log.Error("migrations, mempool ALTER TABLE ADD time error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`CREATE INDEX mempool.time ON transactions(time);`); err != nil {
			log.Error("migrations CREATE INDEX mempool.time error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec("UPDATE mempool.migrations SET version = 2;"); err != nil {
			log.Error("migrations UPDATE mempool.migrations v2 error", "err", err.Error())
		}
		log.Info("mempool v2 migrations done")
	}

	if schemaVersion < 3 {
		log.Info("Applying mempool v3 migration")
		if _, err := db.Exec(`ALTER TABLE mempool.transactions ADD COLUMN maxFeePerBlobGas varchar(32)`); err != nil {
			log.Error("migrations ALTER TABLE mempool.transactions maxFeePerBlobGas error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec(`ALTER TABLE mempool.transactions ADD COLUMN blobVersionedHashes blob`); err != nil {
			log.Error("migrations ALTER TABLE mempool.transactions blobVersionedHashes error", "err", err.Error())
			return nil
		}
		if _, err := db.Exec("UPDATE mempool.migrations SET version = 3;"); err != nil {
			log.Error("migrations UPDATE mempool.migrations v3 error", "err", err.Error())
		}
		log.Info("mempool migrations v3 done")
	}

	log.Info("mempool migrations up to date")
	return nil
}


// *1: Previous versions of this migration had a UINIQUE constriant put on transaction hash. We found that this was redundant in practice when considered
// alongside the txHash index, which is added below, and added considerable lag to block uptake. As of tag v1.3.0-removing-uinique-txHash0 all newer databases will have the schema 
// below while previously existing databases will retain the UNIQUE constraint but will be missing the txHash index.
