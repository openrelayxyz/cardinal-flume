import json
import requests

part_tx_curl = {"jsonrpc":"2.0", "method":"flume_getTransactionsByParticipant", "params":["0x3cd751e6b0078be393132286c442345e5dc49699"],"id":2}

reci_tx_curl = {"jsonrpc":"2.0", "method":"flume_getTransactionsByRecipient", "params":["0x7a250d5630b4cf539739df2c5dacb4c659f2488d"],"id":2}

send_tx_curl = {"jsonrpc":"2.0", "method":"flume_getTransactionsBySender", "params":["0x52bc44d5378309ee2abf1539bf71de1b7d7be3b5"],"id":2}

part_curl = {"jsonrpc":"2.0", "method":"flume_getTransactionReceiptsByParticipant", "params":["0x3cd751e6b0078be393132286c442345e5dc49699"],"id":2}

reci_curl = {"jsonrpc":"2.0", "method":"flume_getTransactionReceiptsByRecipient", "params":["0x7a250d5630b4cf539739df2c5dacb4c659f2488d"],"id":2}

send_curl = {"jsonrpc":"2.0", "method":"flume_getTransactionReceiptsBySender", "params":["0x52bc44d5378309ee2abf1539bf71de1b7d7be3b5"],"id":2}

card = 'http://127.0.0.1:9000'
flume = 'http://127.0.0.1:8000'

part_tx_card = requests.post(card, json=part_tx_curl).json()
part_tx_leg = requests.post(flume, json=part_tx_curl).json()

reci_tx_card = requests.post(card, json=reci_tx_curl).json() 
reci_tx_leg = requests.post(flume, json=reci_tx_curl).json()

send_tx_card = requests.post(card, json=send_tx_curl).json()
send_tx_leg = requests.post(flume, json=send_tx_curl).json()

part_card = requests.post(card, json=part_curl).json()
part_leg = requests.post(flume, json=part_curl).json()

reci_card = requests.post(card, json=reci_curl).json()
reci_leg = requests.post(flume, json=reci_curl).json()

send_card = requests.post(card, json=send_curl).json()
send_leg = requests.post(flume, json=send_curl).json()

tx_leg_responses = [part_tx_leg, reci_tx_leg, send_tx_leg]
tx_card_responses = [part_tx_card, reci_tx_card, send_tx_card]
rct_leg_responses = [part_leg, reci_leg, send_leg]
rct_card_responses = [part_card, reci_card, send_card] 

def tx_check():
    print("===== TX ======")
    for i, resp in enumerate(tx_leg_responses):
        print(f"leg tx resp {i}")
        for j, item in enumerate(resp['result']['items']):
            print(f"length of leg response {len(item)} idx {i} subidx {j}")
            for k, v in item.items():
                try:
                    if v != tx_card_responses[i]['result']['items'][j][k]:
                        print(f"tx leg check error {i}, {j}, {k}")
                except Exception as e:
                    print(e, i, j, k)

    for i, resp in enumerate(tx_card_responses):
        print(f"card tx resp {i}")
        for j, item in enumerate(resp['result']['items']):
            print(f"length of card response {len(item)}, idx {i}")
            if "timestamp" not in item.keys():
                print(f"ts not here {i}, {j}")
            for k, v in item.items():
                try:
                    if v != tx_leg_responses[i]['result']['items'][j][k]:
                        print(i, j, k)
                except Exception as e:
                    print(e, i, j, k)


def rct_check():
    print("+++++ RCPT ++++++")
    for i, resp in enumerate(rct_leg_responses):
        print(f"leg  rct resp {i}")
        for j, item in enumerate(resp['result']['items']):
            print(f"length of leg response {len(item)} idx {i} subidx {j}")
            for k, v in item.items():
                if v != rct_card_responses[i]['result']['items'][j][k]:
                    print(i, j, k)

    for i, resp in enumerate(rct_card_responses):
        print(f"card rct resp {i}")
        for j, item in enumerate(resp['result']['items']):
            print(f"length of card response {len(item)}, idx {i} subidx {j}")
            if "timestamp" not in item.keys():
                print(f"ts not here {i}, {j}")
            # print(f"{item.keys()}")
            for k, v in item.items():
                if k == 'timestamp':
                    continue
                if v != rct_leg_responses[i]['result']['items'][j][k]:
                    print(i, j, k)

def main():
    tx_check()
    rct_check()

if __name__ == "__main__":
    main()

