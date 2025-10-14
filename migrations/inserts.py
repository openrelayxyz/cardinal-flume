import sqlite3 
import sys

config = [
    #   "cancun": {
    #     "time": 0,
    #     "target": 3,
    #     "max": 6,
    #     "baseFeeUpdateFraction": 3338477
    #   },
    { #"prague":
    "start_time": 0,
    "end_time": 1757611104,
    "target": 6,
    "max": 9,
    "baseFeeUpdateFraction": 5007716
    }, 
    { #"osaka":
    "start_time": 1757611104,
    "end_time": 1757709408,
    "target": 6,
    "max": 9,
    "baseFeeUpdateFraction": 5007716
    },
    { #"bpo1":
    "start_time": 1757709408,
    "end_time": 1757807712,
    "target": 10,
    "max": 15,
    "baseFeeUpdateFraction": 8346193
    },
    { #"bpo2":
    "start_time": 1757807712,
    "end_time": 1757906016,
    "target": 14,
    "max": 21,
    "baseFeeUpdateFraction": 11684671
    },
    { #"bpo3":
    "start_time": 1757906016,
    "end_time": 1758004320,
    "target": 22,
    "max": 33,
    "baseFeeUpdateFraction": 18361626
    },
    { #"bpo4":
    "start_time": 1758004320,
    "end_time": 1758102624,
    "target": 32,
    "max": 48,
    "baseFeeUpdateFraction": 26707819
    },
    { #"bpo5": 
    "start_time": 1758102624,
    "end_time": 9223372036854775807,
    "target": 48,
    "max": 72,
    "baseFeeUpdateFraction": 40061729
    }
]

def insert(blocks_path):
    connection = sqlite3.connect(f'{blocks_path}')
    cursor = connection.cursor()

    for i, schedule in enumerate(config):
        try:
            cursor.execute(
                """
                INSERT INTO blobSchedule(startTime, endTime, target, max, updateFrac) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    schedule['start_time'],
                    schedule['end_time'],
                    schedule['target'],
                    schedule['max'],
                    schedule['baseFeeUpdateFraction']
                )
            )
        except Exception as e:
            print(f"error encoutered when executing insert statment. Error: {e}, index:{i}")

    connection.commit()
    connection.close()

def main(path):
    insert(path)

if __name__ == "__main__":
    main(sys.argv[1])
