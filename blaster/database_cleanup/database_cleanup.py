import sqlite3
from tqdm import tqdm
import os
import gzip
import argparse
import sys
from config import parse_yaml

migration_statements = {"blocks" : ["CREATE TABLE migrations (version integer PRIMARY KEY);", 
    "CREATE TABLE cardinal_offsets (partition INT, offset BIGINT, topic STRING, PRIMARY KEY (topic, partition));",
    "CREATE TABLE issuance (startBlock BIGINT, endBlock BIGINT, value BIGINT);",
    "CREATE INDEX timestamp ON blocks(time);",
    "CREATE INDEX bkHash ON blocks(hash);",
    "CREATE INDEX coinbaseCompound ON blocks(coinbase, number);",
    "CREATE INDEX addressBlock ON withdrawals(address, block);",
    "CREATE INDEX blockHash ON withdrawals(blockHash);",
    "INSERT INTO migrations(version) VALUES(4);"],

"transactions" : ["CREATE TABLE migrations (version integer PRIMARY KEY);",
    "CREATE INDEX txblock ON transactions(block)",
    "CREATE INDEX recipient_partial ON transactions(recipient) WHERE recipient IS NOT NULL",
    "CREATE INDEX contractAddress_partial ON transactions(contractAddress) WHERE contractAddress IS NOT NULL",
    "CREATE INDEX senderNonce ON transactions(sender, nonce)",
    "CREATE INDEX txHash ON transactions(hash);",
    "INSERT INTO migrations(version) VALUES(2);"],

"logs" : ["CREATE TABLE migrations (version integer PRIMARY KEY);",
    "CREATE INDEX address_compound ON event_logs(address, block)",
    "CREATE INDEX topic0_compound ON event_logs(topic0, block)",
    "CREATE INDEX topic1_partial ON event_logs(topic1, topic0, address, block) WHERE topic1 IS NOT NULL",
    "CREATE INDEX topic2_partial ON event_logs(topic2, topic0, address, block) WHERE topic2 IS NOT NULL",
    "CREATE INDEX topic3_partial ON event_logs(topic3, topic0, address, block) WHERE topic3 IS NOT NULL",
    "CREATE TABLE address_hints (address varchar(20));",
    "CREATE INDEX address_topic0_compound ON event_logs(address, topic0, block);",
    "INSERT INTO migrations(version) VALUES(3);"]
}

def migrate_database(db_file, data_type, tmp_dir):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    try:
        cursor.execute(f'PRAGMA temp_store_directory = "{tmp_dir}"')
        conn.commit()
    except Exception as e:
        print(f"Error setting temporary directory in migrate_database(), for {data_type}, in {db_file}: {e}")
        raise


    statements = migration_statements[data_type]

    for statement in tqdm(statements, desc=f"executing migration statements for {data_type}"):
        try:
            cursor.execute(statement)
        except Exception as e:
            print(f"Error executing migration statements, error {e}, statement {statement}")

    conn.commit()

    cursor.close()
    conn.close()

def copy_table(withdrawals_db, blocks_db, tmp_dir):
    source_conn = sqlite3.connect(withdrawals_db)
    source_cursor = source_conn.cursor()
    
    target_conn = sqlite3.connect(blocks_db)
    target_cursor = target_conn.cursor()

    target_conn.execute(f'PRAGMA temp_store_directory = "{tmp_dir};"')
    try:
        target_cursor.execute(f'PRAGMA temp_store_directory = "{tmp_dir}"')
        target_conn.commit()
    except Exception as e:
        print(f"Error setting temporary directory in copy_table: {e}")
        raise

    print("copying withdrawals table initiated")
    target_cursor.execute(f"ATTACH DATABASE '{withdrawals_db}' AS source")
    target_cursor.execute("CREATE TABLE withdrawals(block, wtdrlIndex, vldtrIndex, address, amount, blockHash, PRIMARY KEY (block, wtdrlIndex)) WITHOUT ROWID;")
    target_cursor.execute(f"INSERT INTO withdrawals SELECT * FROM source.withdrawals;")
    target_conn.commit()

    source_cursor.close()
    target_cursor.close()

    source_conn.close()
    target_conn.close()

    print("copying migrations table completed")

def inject_missing_statements(database, statements, tmp_dir):
    conn = sqlite3.connect(database)
    cursor = conn.cursor()

    try:
        cursor.execute(f'PRAGMA temp_store_directory = "{tmp_dir}"')
        conn.commit()
    except Exception as e:
        print(f"Error setting temporary directory in inject_missing_statements(), for {statements}, in {database}: {e}")
        raise


    with open(statements, 'r') as sql_file:
        sql_commands = sql_file.read().split(';')[:-1]
        for command in tqdm(sql_commands, desc=f"injecting missing statments into {database}"):
            cursor.execute(command)

    conn.commit()
    conn.close()


# at this point this function is inactive, it appears the os library has a problem identifying the files
def gunzip_statements(statements):
    base_name = os.path.splitext(os.path.basename(statements))[0]

    try:
        with gzip.open(statements, 'rb') as f_in:
            sql_commands = f_in.read()

            print(base_name, sql_commands)
    except Exception as e:
        print(f"error encountered in test. Error: {e}")

def main(args, config=None):

    if config == None:
        print("No config file found")
        return

    if args.mode == 'all':

        for stmnt_file in os.listdir(config['missing_statements']):
            if stmnt_file[0:2] == 'lo':
                log_statements = os.path.join(config['missing_statements'], stmnt_file)
            if stmnt_file[0:2] == 'tx':
                tx_statements = os.path.join(config['missing_statements'], stmnt_file)

        inject_missing_statements(config['tx_db'], tx_statements)

        inject_missing_statements(config['logs_db'], log_statements)

        copy_table(config['wdls_db'], config['blocks_db'], config['tmp_dir'])

        migrate_database(config['blocks_db'], 'blocks', config['tmp_dir'])

        migrate_database(config['tx_db'], 'transactions', config['tmp_dir'])

        migrate_database(config['logs_db'], 'logs', config['tmp_dir'])

    if args.mode == 'inject':

        for stmnt_file in os.listdir(config['missing_statements']):
            if stmnt_file[0:2] == 'lo':
                log_statements = os.path.join(config['missing_statements'], stmnt_file)
            if stmnt_file[0:2] == 'tx':
                tx_statements = os.path.join(config['missing_statements'], stmnt_file)

        inject_missing_statements(config['tx_db'], tx_statements, config['tmp_dir'])

        inject_missing_statements(config['logs_db'], log_statements, config['tmp_dir'])

    if args.mode == 'copy':

        copy_table(config['wdls_db'], config['blocks_db'], config['tmp_dir'])

    if args.mode == 'migrate':

        migrate_database(config['blocks_db'], 'blocks', config['tmp_dir'])

        migrate_database(config['tx_db'], 'transactions', config['tmp_dir'])

        migrate_database(config['logs_db'], 'logs', config['tmp_dir'])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Flume blaster, post indexing database transformation")

    parser.add_argument('mode', choices=['all', 'inject', 'copy', 'migrate'], help='specify action to perform')

    parser.add_argument('config', help="config file")

    args = parser.parse_args()

    config = parse_yaml(args.config)

    main(args, config)