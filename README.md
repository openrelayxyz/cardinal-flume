# About 

At [Rivet](rivet.cloud) our mission is to make ETH nodes operationally manageable. Cardinal-flume, or just flume, forms part of the backbone of our Etherreum gateway servie. The project was initially designed to index Ethereum log data and make it more accessible than a conventional Ethereum node. Over time the project has grown and now indexes block and transaction data as well. Indexed data is stored in SQLite relational databases. The data is exposed through several APIs written to serve a subset of the ETH namespace as well as some custom RPC methods. 

# Building and running a cardinal-flume service. 

For the purposes of this document it is assumed that the user has installed go version 1.18 or later. Also flume will require access to a synced and healthy [Plugeth](https://github.com/openrelayxyz/plugeth) master with the [blockupdates](https://github.com/openrelayxyz/plugeth-plugins/tree/master/packages/blockupdates), [cardinal producer](https://github.com/openrelayxyz/cardinal-evm/tree/master/plugins/producer), and [cardinal merge](https://github.com/openrelayxyz/cardinal-evm/tree/master/plugins/merge) plugins loaded and a websocket port open on 8555. 

### Building from Source:

Clone the project. 

From the root of the project run. 

```
go get
```
```
go build
```
Once complete this will build a binary within the root of the project which will enable the user to start the service. 

# Config File

The service is controlled by a `config.yml` file. The parsing logic of the config is located in `config.go` in the config package of the project. 

There are serveral parameters that can be set from the config. Of those the most basic and necessary are:

### Network Name

Some network must be specified in order for flume to subsequently set further parameters. 

Currently supported networks are `mainnet`, `classic`, `rinkeby`, `georli`, and `sepolia`. 

`polygon` is also supported but will require additional configuration. *See below*  

### Brokers

Flume requires a data stream from which to sync from the network. For the purposes of this tutorial we will assume a websocket connection to a Plugeth master.

> **_NOTE:_**  Alternatively, `null://` can be provided in this field to hold flume at a static position without syncing new data.

### Databases

Flume requires that paths be provided to four seperate databases with four specific names:

`blocks.sqlite`, `transactions.sqlite`, `logs.sqlite`, `mempool.sqlite`

> **_NOTE:_**  If starting with empty databases a `--genesisIndex` flag must be provided in order to sink from block zero. *See flags discussion below*


A version of this basic implementation could look like this:

```yml
networkName: network
brokers: [
  {
   "url": ws://address:port
  }
]
databases:
  blocks: /path/to/directory/blocks.sqlite
  transactions: /path/to/directory/transactions.sqlite
  logs: /path/to/directory/logs.sqlite
  mempool: /path/to/directory/mempool.sqlite
```

The service can now be initialized with the following command, run from the directory containing the binary.

```
./cardinal-flume path/to/config.yml
```


Beyond this most simple implementation there are other parameters worth considering. 

### Port

Flume will serve through port 8000 by default. Other ports may be specified with the port field. 

### Minimum Safe Block

A minimum block can be set which will trigger the service to shutdown if the minimum indexed block is too high. This check is present to ensure that flume will not start with empty databases unless that is the desired behaviour. 

### Plugins

Flume, like Plugeth, has a plugin framework. Plugins can be developed to extend flume.

In order to load plugins there needs to be a plugin directory specified in the config. Plugins will also need to be named in a plugins list. The name of the plugin should be a string of file name of the plugin present in the plugins directory without the file extension. For example:

If these were the Plugin binaries located in the specified plugins directory: `myPlugin.iso` `anotherPlugin.iso`

Then this would be the Plugins list in the config: [`'myPlugin'`, `'anotherPlugin'`]

### Heavy vs Light

Due to their considerable combined size flume databases can be resource intensive to maintain. flume project was designed with idea of serving archive log data. As the project has grown other APIs have been developed to serve a variety of other requests. Some of the most common requests do not require historic data. And so, it is possible to run flume in a light and heavy configuration. That is: a heavy instance of the service is attached to and syncing full archive databases while another (or multiple) light instance is attached to smaller databases with only the most recent data available. 

Throughout flume there is logic which routes requests to the appropriate instance according to what data is requred to serve them. The location of the heavy instance is provided in the heavyserver field of the config. 

If no heavyserver address is provided in the config then flume will default to heavy behavior and all archive data will be required to serve all potential requests.

The idea is to deploy light servers on an as needed basis to handle the majority of requests and take pressure off of the heavy servers. The light servers are meant to begin syncing from current the moment they are turned on and continue syncing until they are not longer neccesary and can be discarded. 


A version of this more complete implementation of the config could look like this for a light instance:

```yml
networkName: network
brokers: [
  {
   "url": ws://address:port
  }
]
databases:
  blocks: /path/to/directory/blocks.sqlite
  transactions: /path/to/directory/transactions.sqlite
  logs: /path/to/directory/logs.sqlite
  mempool: /path/to/directory/mempool.sqlite
port: port
minSafeBlock: *current block*
pluginPath: /path/to/plugin-directory/
plugins:
  ['myPlugin','anotherPlugin']
heavyserver:
  'http://address:port'
```
> **_NOTE:_**   If running a heavy and light instance on the same machine the `healthcheck` field will need to be specified with two different ports for light and heavy. The default is 9999. At least one of the instances will need to open another unused port. 

# Flags

The behavior of flume can also be modified by the presence of flags provided upon start up. 

In order to start the service with appended flags use the following command template:

```
./cardinal-flume --flag(s) /path/to/config.yml
```

### Exit When Synced

`--shotdownSync` Once synced flume will begin serving requests. In some cases, such as when creating snapshots, serving requests is not required. If provided this flag will cause the service to shutdown once it has indexed the most recent block. 


### Genesis Index

`--genesisIndex` This flag must be provided if starting flume for the first time with the intention of indexing a network from the genesis block. If this flag is not present, starting the service with empty databases will cause cardianl-flume to begin syncing from the most recent blocks (which is the typical behavior of a light instance).

# Supported RPC Methods
Cardinal-Flume supports a subset of the `eth` namespace as well as extended `flume` namespace RPC API. 

#### ETH Methods
> A more complete discussion of the ETH RPC API can be found [here](https://rivet.cloud/docs/topics/api/rpc.html#json-rpc-api-reference).

- `eth_chainId`
- `eth_blockNumber`
- `eth_getBlockByNumber`
- `eth_getBlockByHash`
- `eth_getTransactionByHash`
- `eth_getLogs`
- `eth_getTransactionReceipt`
- `eth_getTransactionsBySender`
- `eth_getBlockTransactionCountByNumber`
- `eth_getBlockTransactionCountByHash`
- `eth_getTransactionByBlockNumberAndIndex`
- `eth_getTransactionByBlockHashAndIndex`
- `eth_getUncleCountByBlockNumber`
- `eth_getUncleCountByBlockHash`
- `eth_getTransactionCount`
- `eth_gasPrice`
- `eth_maxPriorityFeePerGas`
- `eth_feeHistory`

#### Extented Flume Namespace RPC Methods
> A more complete discussion of the flume RPC API can be found [here](https://rivet.cloud/docs/topics/api/rpc.html#flume).

- `flume_getTransactionReceiptsByBlockNumber` - Takes a hex incoded block number as an argument.
- `flume_getTransactionReceiptsByBlockHash` - Takes a block hash as an argument. 


- `flume_getTransactionsBySender` 
- `flume_getTransactionReceiptsBySender`
- `flume_getTransactionsByRecipient`
- `flume_getTransactionReceiptsByRecipient`
- `flume_getTransactionsByParticipant`
- `flume_getTransactionReceiptsByParticipant` - All Take an address and an optional offset as arguments. 


# Polygon

We have written a plugin to extend support to the polygon network. In order to utilize this extention a plugin must be built and placed in a designated plugins directory. An additional database must be provided as well. 

It is assumed that the user has access to a synced and healthy [Plugeth-Bor](https://github.com/openrelayxyz/plugeth/tree/feature/merge-bor-v0.3.0) master with the [blockupdates](https://github.com/openrelayxyz/plugeth-plugins/tree/master/packages/blockupdates) and [cardinal producer](https://github.com/openrelayxyz/cardinal-evm/tree/master/plugins/producer) plugins loaded and a websocket port open on 8555.

### Building the Plugin

From the root of the project navigate to `cardian-flume/plugins/packages/polygon/`. From within this directory run:

```
go build -buildmode=plugin
```
This will create a `polygon.so` artifact which will need to be moved to the plugins directory designated in the config. 

### Bor Database

A `bor.sqlite` will need to be provided to flume in order to store the indexed polygon data. 

A basic implementation of such a config could look like this:

```yml
networkName: polygon
brokers: [
  {
   "url": ws://address:port
  }
]
databases:
  blocks: /path/to/directory/blocks.sqlite
  transactions: /path/to/directory/transactions.sqlite
  logs: /path/to/directory/logs.sqlite
  mempool: /path/to/directory/mempool.sqlite
  bor: /path/to/directory/bor.sqlite
pluginPath: /path/to/plugin-directory/
plugins:
  ['polygon']
```
### Extended API

The Polygon plugin provides an extended API of Polygon specific RPC methods. 

- `eth_getBorBlockReceipt` - Takes a block hash as an argument. 
- `eth_getTransactionReceiptsByBlock` - Takes a hex encoded block number or block hash as an argument. 

- `bor_getSnapshot` - Takes a hex encoded block number or block hash as an argument.
- `bor_getGetAuthor` - Takes a hex encoded block number as an argument.
- `bor_getRootHash` - Takes from and to block integers as arguments.
- `bor_getSignersAtHash` - Takes a hex encoded block number or block hash as an argument.
- `bor_getCurrentValidators` - Takes no argument
- `bor_getCurrentPropser` - Takes no argument





