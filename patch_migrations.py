import sys
import sqlite3
from contextlib import contextmanager
import json
import codecs

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
    'transactions': [transactions_columns, ('block', 'transactionIndex')],
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

        if table == 'blocks':
            condition = ' WHERE number <= 600000;'
        else: 
            condition = ' WHERE block <= 600000;'

        if isinstance(primary, str):
            partial = f', {primary} FROM {table}'
        elif isinstance(primary, tuple):
            partial = f', {primary[0]}, {primary[1]} FROM {table}'

        
        for column in columns.get(table)[0]:
            print(f"executing select statements on {column}, {table}, table")
            q = f'SELECT {column}' + partial + condition
            print(q)
            rows = self.conn.fetchall(q)
            print(f"this is pre deliver row 0 {rows[0]}")
            zeros = find_zeros(rows)
            print(f'length of zeros for {column} on {table} {len(zeros)}')
            prepared = remove_zeros(zeros)
            print(f'length of prepared for {column} on {table} {len(prepared)}')
            
            if len(prepared) > 0:
                self.initial[column] = prepared

        return self.initial

    def extract_statements(self):
        initial_results = self.get_initial_results()

        primary = self.prmry
        table = self.tbl

        output = open(f'{table}-statements.txt', "w")

        for k, v in initial_results.items():
            print(f'writing update statements from column {k} on table {table}')
            for item in v:
                if isinstance(primary, str):
                    s = f"UPDATE {table} SET {k} = X'{codecs.decode(codecs.encode(item[0], 'hex'), 'latin1')}' WHERE {primary} = {item[1]};"
                elif isinstance(primary, tuple):
                    s = f"UPDATE {table} SET {k} = X'{codecs.decode(codecs.encode(item[0], 'hex'), 'latin1')}' WHERE {primary[0]} = {item[1]} AND {primary[1]} = {item[2]};"
                output.write(s + '\n')
       
        output.close()

def find_zeros(rows):
    results = []
    if isinstance(rows, list):
        if len(rows) > 0:
            print(f'this is row zero from find zeros {rows[0]}')
            for row in rows:
                if isinstance(row, tuple):
                    if row[0]:
                        if row[0][0] == 0:
                            results.append(row)
                else:
                    print(f"item was not a tuple, item is a {type(row[0])}, r0 {row[0]}")
        else:
            print("list was empty")
    else: 
        print("argument was not a list")
    return results

def remove_zeros(rows):
    results = []
    for row in rows:
        zeros = 0
        for char in row[0]:
            if char == 0:
                zeros += 1
            else:
                break
        results.append((row[0][zeros:], *row[1:]))
        zeros = 0
    return results

# I think the above functions are doing what we want now. The byte representation in my shell of 0 is b'/x00' but, as you
# remarked they compare to 0 the integer. I am having trouble testing though becuase my test data is fairly limited and I
# cant find any cases of acutal leading zeros. 

def main(file, table):
    db_connection = DatabaseManager(file, table)

    manipulator = DataManipulator(db_connection, table)
    
    manipulator.extract_statements()
    

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])

# the program would work on one table at a time. And would be called by running: python3 file.sqlite name_of_table
