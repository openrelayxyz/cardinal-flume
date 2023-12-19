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

def migrate_database(db_file, data_type):
    conn = sqlite3.connect(db_file)

    # conn.execute(f"PRAGMA temp_store_directory = {tmp_dir};")
    
    cursor = conn.cursor()

    statements = migration_statements[data_type]

    for statement in tqdm(statements, desc=f"executing migration statements for {data_type}"):
        try:
            cursor.execute(statement)
        except Exception as e:
            print(f"Error executing migration statements, error {e}, statement {statement}")

    conn.commit()

    cursor.close()
    conn.close()

def copy_table(source_db, target_db):
    source_conn = sqlite3.connect(source_db)
    target_conn = sqlite3.connect(target_db)
    # target_conn.execute(f"PRAGMA temp_store_directory = {tmp_dir};")

    source_cursor = source_conn.cursor()
    target_cursor = target_conn.cursor()

    print("copying migrations table initiated")
    source_cursor.execute(f"ATTACH DATABASE '{target_db}' AS target")
    source_cursor.execute("CREATE TABLE withdrawals(block, wtdrlIndex, vldtrIndex, address, amount, blockHash, PRIMARY KEY (block, wtdrlIndex,)) WITHOUT ROWID;")
    source_cursor(f"INSERT INTO withdrawls SELECT * FROM target.withdrawals;")
    # source_cursor.execute(f"CREATE TABLE target.withdrawals AS SELECT * FROM withdrawals")
    # needs t be edited such that the primary key is block. withdrawl index
    source_conn.commit()

    source_cursor.close()
    target_cursor.close()
    source_conn.close()
    target_conn.close()

    print("copying migrations table completed")

def inject_missing_statements(database, statements):

    conn = sqlite3.connect(database)

    with open(statements, 'r') as sql_file:
        sql_commands = sql_file.read().split(';')[:-1]
        for command in tqdm(sql_commands, desc=f"injecting missing statments into {database}"):
            conn.execute(command)

    conn.commit()
    conn.close()

def gzip_test(statements):
    base_name = os.path.splitext(os.path.basename(statements))[0]

    try:
        with gzip.open(statements, 'rb') as f_in:
            sql_commands = f_in.read()

            print(base_name, sql_commands)
    except Exception as e:
        print(f"error encountered in test. Error: {e}")

def test():
    print("success")

# def tmp_dir_check(arg):
#     print("checking")
#     if args.tmp_dir == "":
#         print("empty")
#     else:
#         print(f"{args.tmp_dir}")

def main(args):

    config = parse_yaml(args.config)

    if args.mode == 'all':
        # print(config)
        # print(config['missing_statements'])
    #     if args.missing_dir == None:
    #       print("'all' argument requires missing statements location")
    #     if args.blocks_db == None:
    #         print("'all' argument requires blocks database location")
    #     if args.wdls_db == None:
    #         print("'all' argument requires missing statements location")
    #     if args.tx_db == None:
    #         print("'all' argument requires transactios database location")
    #     if args.logs_db == None:
    #         print("'all' argument requires logs database location")
    #     # if args.tmp_dir == None:
    #     #     print("'all' argument requires pragma tmp dir location")
    #     else:
        
        for stmnt_file in os.listdir(config['missing_statements']):
            if stmnt_file[0:2] == 'lo':
                log_statements = os.path.join(config['missing_statements'], stmnt_file)
            if stmnt_file[0:2] == 'tx':
                tx_statements = os.path.join(config['missing_statements'], stmnt_file)

        # print(f"logs: {log_statements}, tx: {tx_statements}")
        # print(config['tx_db'])

        inject_missing_statements(config['tx_db'], tx_statements)

        inject_missing_statements(config['logs_db'], log_statements)

        copy_table(config['wdls_db'], config['blocks_db'])

        migrate_database(config['blocks_db'], 'blocks')

        migrate_database(config['tx_db'], 'transactions')

        migrate_database(config['logs_db'], 'logs')

    # if args.mode == 'wdls':
    #     # tmp_dir_check(args)
    #     # if args.tmp_dir == None:
    #     #     print("'withdrawals' argument requires pragma tmp dir location")
    #     if args.blocks_db == None:
    #         print("'withdrawals' argument requires blocks database location")
    #     if args.wdls_db == None:
    #         print("'withdrawals' argument requires missing statements location")
    #     else:
    #         # tm_dir = tmp_dir_check(args.tmp_dir)
    #         copy_table(args.wdls_db, args.blocks_db)

    # if args.injection:
    #     if args.tx_db and args.logs_db == None:
    #         print("'injection' argument requires either the logs or transactions database location")
    #     if args.temp_dir == None:
    #         print("'injection' argument requires pragma temp dir location")
    #     else:
    #         for file in [args.tx_db, args.logs_db]:
    #             if file != None:
    #                 db = file
                    
    #     for stmnt_file in os.listdir(args.statements_dir):
    #             if stmnt_file[0:2] == 'lo' and db == args.logs_db:
    #                 statements = os.path.join(args.statements_dir, stmnt_file)
    #             if stmnt_file[0:2] == 'tx' and db == args.tx_db:
    #                 statements = os.path.join(args.statements_dir, stmnt_file)

    #     # if db == args.tx_db 
        
    #     inject_missing_statements(db, statements)
        
        
            


    # if args.migrate:
    #     migrate(args)





# if __name__ == "__main__":
# #     main(sys.argv[1], sys.argv[2])
#     # main(sys.argv[1], sys.argv[2])
#     # test(sys.argv[0])
#     # inject_missing_statements(sys.argv[1], sys.argv[2])
#     # copy_table(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
#     migrate_database(sys.argv[1], sys.argv[2])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Flume blaster, post indexing database transformation")

    parser.add_argument('mode', choices=['all', 'wdls'], help='specify action to perform')

    parser.add_argument('--config', help="config file")
    # parser.add_argument('--blocks_db', help="location of blocks database")
    # parser.add_argument('--wdls_db', help="location of withdrawals database")
    # parser.add_argument('--tx_db', help="location of transactions database")
    # parser.add_argument('--logs_db', help="location of logs database")
    # parser.add_argument('--tmp_dir', help="location of pragma temporary directory")

    args = parser.parse_args()

    main(args)