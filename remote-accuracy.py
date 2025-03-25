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

        if payload['method'] == 'eth_feeHistory':
            print(payload)

        if payload['method'] == 'eth_getLogs':
            print(payload)
        
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


def main(port, file_name, latest_block, number_of_blocks):

    client = RPCClient(f'http://localhost:{port}')
        
    results = {
        'blocks': {'by_number':[],'by_hash':[],'tx_ct_by_num':[],'tx_ct_by_hsh':[],'ucl_ct_by_num':[],'ucl_ct_by_hsh':[]},
        'txns': {'by_hash':[],'hash_dex':[],'num_dex':[],'receipt':[],'counts':[]},
        'receipts': [],
        'logs': [],
        'fees': []
    }

    for i, block_number in enumerate(range(latest_block, latest_block - number_of_blocks, -1)):
        
        block_data = client.get_block_by_number(block_number)
        results['blocks']['by_number'].append(block_data)
        client.set_vars(block_data)

        if len(client.tx_hashes) > 0:
            tx_data = client.get_transaction_by_hash(client.tx_hashes[0])
            results['txns']['by_hash'].append(tx_data)
            tx_data = client.get_transaction_by_hash(client.tx_hashes[-1])
            results['txns']['by_hash'].append(tx_data)

        if i % 2 == 0:
            receipt_data = client.get_block_receipts(hex(block_number))
        else:
            receipt_data = client.get_block_receipts(client.block_hash)
        results['receipts'].append(receipt_data)
        

        if i == 1 or i % 10 == 0:
            tx_ct_by_num = client.get_block_transaction_count_by_number(block_number)
            results['blocks']['tx_ct_by_num'].append(tx_ct_by_num)
            tx_ct_by_hsh = client.get_block_transaction_count_by_hash(client.block_hash)
            results['blocks']['tx_ct_by_hsh'].append(tx_ct_by_hsh)
            ucl_ct_by_num = client.get_uncle_count_by_number(block_number)
            results['blocks']['ucl_ct_by_num'].append(tx_ct_by_num)
            ucl_ct_by_hsh = client.get_uncle_count_by_hash(client.block_hash)
            results['blocks']['ucl_ct_by_hsh'].append(tx_ct_by_hsh)
            block_by_hash = client.get_block_by_hash(client.block_hash)
            results['blocks']['by_hash'].append(block_by_hash)

            if len(client.tx_hashes) > 0:
                tx_data = client.get_transaction_by_hash_index(client.block_hash, 0)
                results['txns']['hash_dex'].append(tx_data)
                tx_data = client.get_transaction_by_number_index(block_number, len(client.tx_hashes) -1)
                results['txns']['num_dex'].append(tx_data)

                rcpt_data = client.get_transaction_receipt(client.tx_hashes[0])
                results['txns']['receipt'].append(rcpt_data)
                rcpt_data = client.get_transaction_receipt(client.tx_hashes[-1])
                results['txns']['receipt'].append(rcpt_data)

                ct_data = client.get_transaction_count(client.recipients[0], block_number)
                results['txns']['counts'].append(ct_data)
                ct_data = client.get_transaction_count(client.recipients[-1], block_number)
                results['txns']['counts'].append(ct_data)
                ct_data = client.get_transaction_count(client.senders[0], block_number)
                results['txns']['counts'].append(ct_data)
                ct_data = client.get_transaction_count(client.senders[-1], block_number)
                results['txns']['counts'].append(ct_data)

           
            log_data = client.get_logs({"fromBlock": hex(block_number), "toBlock": hex(latest_block)})
            results['logs'].append(log_data)

            fee_data = client.fee_history(27, block_number, [10, 50, 90])
            results['fees'].append(fee_data)

    with open(f"{file_name}.json", "w") as file:
        json.dump(results, file, indent=4)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
                    prog='Flume accuracy test',
                    description='Test accuracy of flume APIs against a Geth node')

    parser.add_argument('-p', '--port', default='8000')
    parser.add_argument('-f', '--filename', default='results')
    parser.add_argument('-b', '--latestblock', type=int, default=None)
    parser.add_argument('-r', '--blockrange', type=int, default=20)
    
    args = parser.parse_args()

    main(args.port, args.filename, args.latestblock, args.blockrange)
