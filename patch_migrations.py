import sys
import sqlite3
from contextlib import contextmanager

blocks_columns = [
	'hash',
	'parentHash',
	'uncleHash',
	'coinbase',
	'root',
	'txRoot',
	'receiptRoot',
	'mixDigest',
	'td',
	'baseFee',
	'withdrawalHash',
]

withdrawals_columns = [
	'address',
	'blockHash'
]

transactions_columns = [
	'hash',
	'r',
	's',
	'sender',
	'func'
]

event_logs_colums = [
	'address',
	'transactionHash',
	# 'transactionIndex', transaction index needs a different fix
	'blockHash'
]

columns = {
    'blocks': [blocks_columns, ('number')],
    'withdrawals': [withdrawals_columns, ('block', 'wtdrlIndex')],
    'transactions': [transactions_columns, ('id')],
    'event_logs': [event_logs_colums, ('block', 'logIndex')],
}


class DatabaseManager:
    def __init__(self, db_name, table_name):
        self.db = db_name
        self.table = table_name

    @contextmanager
    def connect(self):
        try:
            conn = sqlite3.connect(self.db)
            yield conn
        except:
            raise
        finally:
            conn.close()

    def execute(self, query):
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params or ())
            conn.commit()
            return cursor.rowcount

    def fetchall(self, query, params=None):
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params or ())
            return cursor.fetchall()

    def fetchone(self, query, params=None):
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params or ())
            return cursor.fetchone()

class DataManipulator:
    def __init__(self, connection, table):
        self.conn = connection
        self.tbl = table
        self.prmry = columns.get(table)[1]
        self.initial = {}

    def get_initial_results(self):
        primary = self.prmry
        table = self.tbl

        if isinstance(primary, str):
            partial = f', {primary} FROM {table};'
        elif isinstance(primary, tuple):
            partial = f', {primary[0]}, {primary[1]} FROM {table};'

        for column in columns.get(table)[0]:
            q = f'SELECT hex({column})' + partial
            rows = self.conn.fetchall(q)
            zeros = find_zeros(rows)
            prepared = remove_zeros(zeros)
            
            if len(prepared) > 0:
                self.initial[column] = prepared

        return self.initial

    def alter_table(self):
        initial_results = self.get_initial_results()

        primary = self.prmry
        table = self.tbl

        if isinstance(primary, str):
            tmp = 'UPDATE {} SET {} = {} WHERE {} = {}'
        elif isinstance(primary, tuple):
            tmp = 'UPDATE {} SET {} = {} WHERE {} = {} AND {} = {}'
        
        for k, v in initial_results.items():
            print(f"processing column: {k} on table: {table} with {len(v)} statements to process")
            for item in v:
                if isinstance(primary, str):
                    s = f'UPDATE {table} SET {k} = {item[0]} WHERE {primary} = {item[1]}'
                elif isinstance(primary, tuple):
                    s = f'UPDATE {table} SET {k} = {item[0]} WHERE {primary[0]} = {item[1]} AND {primary[1]} = {item[2]}'
                print(s)
                # self.conn.execute(s)

def find_zeros(rows):
    results = []
    for row in rows:
        if row[0][0] == '0':
            results.append(row)
    return results

def remove_zeros(rows):
    results = []
    for row in rows:
        zeros = 0
        for char in row[0]:
            if char == '0':
                zeros += 1
        results.append((row[0][zeros:], *row[1:]))
        zeros = 0
    return results

def main(file, table):
    db_connection = DatabaseManager(file, table)

    manipulator = DataManipulator(db_connection, table)
    
    manipulator.alter_table()
    

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])

# the program would work on one table at a time. And would be called by running: python3 file.sqlite name_of_table