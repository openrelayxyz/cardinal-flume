import requests
import json
import argparse

class RPCClient:
    def __init__(self, rpc_url):
        self.rpc_url = rpc_url
        self.session = requests.Session()

        self.block_hash = ""
        self.tx_hashes = []
        self.senders = []
        self.recipients = []

    def set_vars(self, block_data):
        transaction_hashes = [tx['hash'] for tx in block_data.get('transactions', [])]
        recepients = [tx['to'] for tx in block_data.get('transactions', [])]
        senders = [tx['from'] for tx in block_data.get('transactions', [])]

        self.block_hash = block_data.get('hash')
        self.tx_hashes.extend(transaction_hashes)
        self.recipients.extend(recepients)
        self.senders.extend(senders)

    def call_rpc(self, method, params=None):
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params or [],
            "id": 1
        }
        
        response = self.session.post(self.rpc_url, json=payload)

        if response.status_code == 200:
            return response.json().get("result")
        else:
            print(f"error calling {method}: {response.text}")
            return None

    # blocks API

    def get_latest_block(self):
        return self.call_rpc("eth_blockNumber", [])

    def get_block_by_number(self, block_number):
        return self.call_rpc("eth_getBlockByNumber", [hex(block_number), True])

    def get_block_by_hash(self, block_hash):
        return self.call_rpc("eth_getBlockByHash", [block_hash, True])

    def get_block_transaction_count_by_number(self, block_number):
        return self.call_rpc("eth_getBlockTransactionCountByNumber", [hex(block_number)])

    def get_block_transaction_count_by_hash(self, block_hash):
        return self.call_rpc("eth_getBlockTransactionCountByHash", [block_hash])

    def get_uncle_count_by_number(self, block_number):
        return self.call_rpc("eth_getUncleCountByBlockNumber", [hex(block_number)])

    def get_uncle_count_by_hash(self, block_hash):
        return self.call_rpc("eth_getUncleCountByBlockHash", [block_hash])

    def get_block_receipts(self, block_number_or_hash):
        return self.call_rpc("eth_getBlockReceipts", [block_number_or_hash])

    # tx API

    def get_transaction_by_hash(self, tx_hash): 
        return self.call_rpc("eth_getTransactionByHash", [tx_hash])

    def get_transaction_by_hash_index(self, block_hash, dex): 
        return self.call_rpc("eth_getTransactionByBlockHashAndIndex", [block_hash, dex])

    def get_transaction_by_number_index(self, block_number, dex): 
        return self.call_rpc("eth_getTransactionByBlockNumberAndIndex", [hex(block_number), dex])

    def get_transaction_receipt(self, tx_hash): 
        return self.call_rpc("eth_getTransactionReceipt", [tx_hash])

    def get_transaction_count(self, addr, block_number): 
        return self.call_rpc("eth_getTransactionCount", [addr, hex(block_number)])

    # logs API

    def get_logs(self, filter_query):
        return self.call_rpc("eth_getLogs", [filter_query])

    # gas API

    def fee_history(self, block_count, terminal_block, reward_percentiles):
        return self.call_rpc("eth_feeHistory", [hex(block_count), hex(terminal_block), reward_percentiles])


def aggregate_data(args):
    port = args.port
    file_name = args.filename
    latest_block = int(args.latestblock)
    number_of_blocks = args.blockrange

    client = RPCClient(f'http://localhost:{port}')
        
    results = {
        'blocks': {'by_number':[],'by_hash':[],'tx_ct_by_num':[],'tx_ct_by_hsh':[],'ucl_ct_by_num':[],'ucl_ct_by_hsh':[]},
        'txns': {'by_hash':[],'hash_dex':[],'num_dex':[],'receipt':[],'counts':[]},
        'receipts': [],
        'logs': [],
        'fees': []
    }

    for i, block_number in enumerate(range(latest_block, latest_block - number_of_blocks, -1)):

        if block_number % 100 == 0:
            print(f"inside collection loop, block: {block_number}")

        prms = block_number
        block_data = client.get_block_by_number(prms)
        results['blocks']['by_number'].append({'arg':prms,'resp':block_data})
        client.set_vars(block_data)

        if len(client.tx_hashes) > 0:
            prms = client.tx_hashes[0]
            tx_data = client.get_transaction_by_hash(prms)
            results['txns']['by_hash'].append({'arg':prms,'resp':tx_data})
            prms = client.tx_hashes[-1]
            tx_data = client.get_transaction_by_hash(prms)
            results['txns']['by_hash'].append({'arg':prms,'resp':tx_data})

        if i % 2 == 0:
            prms = hex(block_number)
            receipt_data = client.get_block_receipts(prms)
        else:
            prms = client.block_hash
            receipt_data = client.get_block_receipts(prms)
        results['receipts'].append({'arg':prms,'resp':receipt_data})
        

        if i == 1 or i % 10 == 0:
            prms = block_number
            tx_ct_by_num = client.get_block_transaction_count_by_number(prms)
            results['blocks']['tx_ct_by_num'].append({'arg':prms,'resp':tx_ct_by_num})
            prms = client.block_hash
            tx_ct_by_hsh = client.get_block_transaction_count_by_hash(prms)
            results['blocks']['tx_ct_by_hsh'].append({'arg':prms,'resp':tx_ct_by_hsh})
            prms = block_number
            ucl_ct_by_num = client.get_uncle_count_by_number(prms)
            results['blocks']['ucl_ct_by_num'].append({'arg':prms,'resp':tx_ct_by_num})
            prms = client.block_hash
            ucl_ct_by_hsh = client.get_uncle_count_by_hash(prms)
            results['blocks']['ucl_ct_by_hsh'].append({'arg':prms,'resp':tx_ct_by_hsh})
            prms = client.block_hash
            block_by_hash = client.get_block_by_hash(prms)
            results['blocks']['by_hash'].append({'arg':prms,'resp':block_by_hash})

            if len(client.tx_hashes) > 0:
                prms = (client.block_hash, 0)
                tx_data = client.get_transaction_by_hash_index(*prms)
                results['txns']['hash_dex'].append({'arg':prms,'resp':tx_data})
                prms = (block_number, len(client.tx_hashes) -1)
                tx_data = client.get_transaction_by_number_index(*prms)
                results['txns']['num_dex'].append({'arg':prms,'resp':tx_data})

                prms = client.tx_hashes[0]
                rcpt_data = client.get_transaction_receipt(prms)
                results['txns']['receipt'].append({'arg':prms,'resp':rcpt_data})
                prms = client.tx_hashes[-1]
                rcpt_data = client.get_transaction_receipt(prms)
                results['txns']['receipt'].append({'arg':prms,'resp':rcpt_data})

                # NOTE at this point we are only testing transactionCount using the old, partially accurate flume behavior

                prms = (client.senders[0], block_number)
                ct_data = client.get_transaction_count(*prms)
                results['txns']['counts'].append({'arg':prms,'resp':ct_data})

                prms = (client.senders[-1], block_number)
                ct_data = client.get_transaction_count(*prms)
                results['txns']['counts'].append({'arg':prms,'resp':ct_data})

            prms = {'fromBlock': hex(block_number - 1), 'toBlock': hex(block_number)}
            log_data = client.get_logs(prms)
            results['logs'].append({'arg':prms,'resp':log_data})

            prms = (27, block_number, [10, 50, 90])
            fee_data = client.fee_history(*prms)
            results['fees'].append({'arg':prms,'resp':fee_data})

    print(f"data aggregation complete, printing to file: {file_name}")
    with open(f"{file_name}.json", "w") as file:
        json.dump(results, file, indent=4)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Test accuracy of flume APIs against a Geth node')

    parser.add_argument('-p', '--port', default='8000')
    parser.add_argument('-f', '--filename', default='results')
    parser.add_argument('-b', '--latestblock', required=True, help="An integer that must be provided")
    parser.add_argument('-r', '--blockrange', type=int, default=20)

    args = parser.parse_args()
    aggregate_data(args)
