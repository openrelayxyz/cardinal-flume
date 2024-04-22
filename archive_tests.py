import requests
import json

import requests
import json

def get_all_by_address(url, address, meth):
    results = []
    s = requests.session()
    try:
        outer = s.post(url, json={"id":77,"method":f"flume_get{meth}","params":[address]}).json()
        res = outer['result']
    except Exception as e:
        print(f"error on stand alone attempt result, error: {e}")
        raise
        return
    while res.get("next"):
        results.append(res['items'])
        try:
            outer = s.post(url, json={"id":77,"method":f"flume_get{meth}","params":[address, res['next']]}).json()
            res = outer['result']
        except Exception as e:
            print(f"error on inner attemtp result, error: {e}")
            raise
            return 
    results.append(res['items'])
    # print(f"len of list {len(results[0])}")
    #return sorted(results, key=lambda x: int(x[0]['nonce'], 16))
    return sorted(results, key=lambda x: int(x[0]['blockNumber'], 16))

def compare_multiple(adrss, method):
    flume = 'http://localhost:8000'
    rivet = 'https://sepolia.rpc.rivet.cloud/2ebb6d9d2bbb42a3a9b1cf4d129e9609'
    for adr in adrss:
        try:
            r = get_all_by_address(rivet, adr, method)
            f = get_all_by_address(flume, adr, method)
        except Exception as e:
            print(f"error on address {adr}, exception {e}")
            raise
            return
        print(f"{adr}, {f == r}")
