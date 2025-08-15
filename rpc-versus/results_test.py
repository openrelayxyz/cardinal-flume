import json
import sys
import re
import pytest

# these are known cases where flume and standard geth APIs diverge
ignored_keys = [
    "chainId", 
    "totalDifficulty"
]

ignored_patterns = [
    re.compile(r"blocks\.by_number\[\d+\]\.resp"),
    re.compile(r"blocks\.by_hash\[\d+\]\.resp"),
    re.compile(r"blocks\.by_hash\[\d+\]\.resp\.transactions\[\d+\]"),
    re.compile(r"blocks\.by_number\[\d+\]\.resp\.transactions\[\d+\]"),
    re.compile(r"txns\.by_hash\[\d+\]\.resp"),
    re.compile(r"txns\.counts\[\d+\]\.resp") # needs to be taken out of the aggregator 
    # TODO the only nonces flume accounts for are senders not countracts. Need to address how addresses are aggregated. 
]


def compare_dicts(d1, d2, path=""):  
    if d1.keys() != d2.keys():
        missing_key = (set(d1.keys()) ^ set(d2.keys())).pop()
        if missing_key in ignored_keys and any(pattern.fullmatch(path) for pattern in ignored_patterns):
            return
        else:
            pytest.fail(f"Key mismatch at {path}: {set(d1.keys()) ^ set(d2.keys())}")
    
    for key in d1:
        new_path = f"{path}.{key}" if path else key

        if key not in d2:
            pytest.fail(f"Missing key '{key}' in test file at {new_path}")
            continue
        
        v1, v2 = d1[key], d2[key]
        compare_values(v1, v2, new_path)

def compare_lists(l1, l2, path=""):
    if len(l1) != len(l2):
        pytest.fail(f"List length mismatch at {path}: {len(l1)} vs {len(l2)}")
    
    for i, (item1, item2) in enumerate(zip(l1, l2)):
        new_path = f"{path}[{i}]"
        compare_values(item1, item2, new_path)

def compare_values(v1, v2, path=""):
    if type(v1) != type(v2):
        pytest.fail(f"Type mismatch at {path}: {type(v1).__name__} vs {type(v2).__name__}")
    elif isinstance(v1, dict):
        compare_dicts(v1, v2, path)
    elif isinstance(v1, list):
        compare_lists(v1, v2, path)
    elif v1 != v2:
        pytest.fail(f"Value mismatch at {path}: {v1} vs {v2}")

def test_main(control_path, test_path):

    with open(control_path, "r") as f1, open(test_path, "r") as f2:
        data1, data2 = json.load(f1), json.load(f2)
    
    compare_dicts(data1, data2)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])