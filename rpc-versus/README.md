### Flume RPC versus test:

This test is designed to exercise the flume RPC APIs agains a geth master in order to establish the accuracy of Cardinal-Flume's return values.

The test requires that two datasets be collected. One, a control, from a Geth master with http ports open and eth namespace exposed. Then another, from a cardianl flume with databases consistent with the block ranges specified. This can be accomplished by running the `collect.py` file from this directory against a Geth master endpoint and then a flume endpoint. 

Once the two files have been aggregated the test is run from the CLI like so:
```pytest --control-path=<path/to/control/data.json> --test-path=<path/to/test/data.json>``` 

**Note:** If ranging beyond blocks older than 128 from the head block the master will likely not have the state to process ```get_transactionCount```. There is a ```-s --range_beyond_state``` flag in the collection script to account for this. 