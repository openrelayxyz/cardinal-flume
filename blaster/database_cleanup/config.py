import yaml
import sys
import os

def parse_yaml(file):

    with open(file, 'r') as f:
        try:
            data = yaml.safe_load(f)

            dbs = ['blocks', 'tx', 'logs', 'wdls']

            for db in dbs:
                if f'{db}_db' not in data:
                    print(f"config missing required database {db}")
                    return 
                elif 'missing_statements' not in data:
                    print("config missing required missing statements directory")
                    return
                elif 'tmp_dir' not in data:
                    print("config missing required pragma tmp dir location")
                    return

        except yaml.YAMLError as e:
            print("Error parsing YAML file:", e)

    # config = {
    #     'blocks_db': data['blocks_db'],
    #     'tx_db': os.path.dirname(data['transactions_db']),
    #     'logs_db': os.path.dirname(data['logs_db']),
    #     'ws_db': os.path.dirname(data['withdrawals_db']),
    #     'missing_dir': os.path.dirname(data['missing_statements']),
    # }

    return data
    

    # print(blocks_dir, tx_dir, logs_dir, ws_dir, data)


