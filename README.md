## About 

inject language around rivet.cloud links

Our mission is to make ETH nodes operationally manageable. Cardinal-flume was initially designed to organize Ethereum log data and make it more accessible than a conventional Ethereum node. Over time the project has grown and now indexes block and transaction data as well. Indexed data is stored in SQLite relational databases. The data is exposed through several API's written to serve a subset of the ETH namespace as well as some custom RPC methods. 

# Building and running a cardinal-flume service. 

For the purposes of this document it is assumed that the user has installed go version 1.18 or later. Also cardianl-flume will require access to a synced and healthy [Plugeth](https://github.com/openrelayxyz/plugeth) master with the [blockupdates](https://github.com/openrelayxyz/plugeth-plugins/tree/master/packages/blockupdates), [cardinal producer](https://github.com/openrelayxyz/cardinal-evm/tree/master/plugins/producer), and [cardinal merge](https://github.com/openrelayxyz/cardinal-evm/tree/master/plugins/merge) plugins loaded and a websocket port open on 8555. 

###### Building from Source:

Clone the project. 

From the root of the project run. 

```
go get
```
```
go build
```

Once complete this will build a binary within the root of the project which will enable the user to start the service. 
### config.yml

The service is controlled by a `config.yml` file. The parsing logic of the config is located in `config.go` in the root 
of the project. 

There are serveral parameters that can be set from the config. Of those the most basic and necessary are:

#### networkName

Some network must be specified in order for cardinal-flume to subsequently set further parameters. 

*mainnet, georli, sepolia* etc.

#### brokers

cardinal-flume requires a data stream from which to sync from the network. For the purposes of this tutorial we will assume a websocket connection to a Plugeth master.

> **_NOTE:_**  Alternatively, `null://` can be provided in this field to hold cardinal-flume at a static position without syncing new data.

#### databases

cardinal-flume requires that paths be provided to four seperate databases with four specific names:

`blocks.sqlite`, `transactions.sqlite`, `logs.sqlite`, `mempool.sqlite`

> **_NOTE:_**  If starting with empty databases a `--genesisIndex` flag must be provided in order to sink from block zero. *See flags discussion below*


A version of this basic implemnetation could look like this:

```yml
networkName: mainnet
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

#### port

cardinal-flume will serve through port 8000 by default. Other ports may be specified with the port field. 

#### minSafeBlock

A minimum block can be set which will trigger the service to shutdown if the minimum indexed block is too high. This check is present to ensure that cardianl-flume will not start with empty databases unless that is the desired behaviour. 

> **_NOTE:_** This field is particularly important to note when running a light instance. *See heavyserver below*

#### plugins

cardinal-flume, like Plugeth, has a plugin framework. Plugins can be developed to extend cardinal-flume.

In order to load plugins there needs to be a plugin directory specified in the config. Plugins will also need to be named in a plugins list. The name of the plugin should be a string of file name of the plugin present in the plugins directory without the file extension. For example:

If these were the Plugin binaries located in the specified plugins directory: `myPlugin.iso` `anotherPlugin.iso`

Then this would be the Plugins list in the config: [`'myPlugin'`, `'anotherPlugin'`]

#### heavyserver

Due to their considerable combined size cardinal-flume databases can be resource intensive to maintain. The cardinal-flume project was designed with idea of serving archive log data. As the project has grown other APIs have been developed to serve a variety of other requests. Some of the most common requests do not require historic data. And so, it is possible to run cardinal-flume in a light and heavy configuration. That is: a heavy instance of the service is attached to and syncing full archive databases while another (or multiple) light instance is attached to smaller databases with only the most recent data available. 

Throughout cardinal-flume there is logic which routes requests to the appropriate instance according to what data is requred to serve them. The location of the heavy instance is provided in the heavyserver field of the config. 

If no heavyserver address is provided in the config then cardinal-flume will default to heavy behavior and all archive data will be required to serve all potential requests.

The idea is to deploy light servers on an as needed basis to handle the majority of requests and take pressure off of the heavy servers. The light servers are meant to begin syncing from current the moment they are turned on and continue syncing until they are not longer neccesary and can be discarded. 


A version of this more complete implementation of the config could look like this:

```yml
networkName: mainnet
brokers:
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

## Flags

The behavior of cardinal-flume can also be modified by the presence of flags provided upon start up. 

In order to start the service with appended flags use the following command template:

```
./cardinal-flume --flag(s) /path/to/config.yml
```


#### --exitWhenSynced

Once synced cardinal-flume will begin serving requests. In some cases, such as when creating snapshots, serving requests is not required. If provided this flag will cause the service to shutdown once it has indexed the most recent block. 


#### --genesisIndex

This flag must be provided if starting cardinal-flume for the first time with the intention of indexing a network from the genesis block. If this flag is not present, starting the service with empty databases will cause cardianl-flume to begin syncing from the most recent blocks (which is the typical behavior of a light instance).

## Supported RPC Methods

#### Extented Flume Namespace RPC Methods


-`flume_getTransactionReceiptsByBlockNumber` -Takes a hex incoded block number as an argument.
-`flume_getTransactionReceiptsByBlockHash` -Takes a block hash as an argument. 


-`flume_getTransactionsBySender` 
-`flume_getTransactionReceiptsBySender`
-`flume_getTransactionsByRecipient`
-`flume_getTransactionReceiptsByRecipient`
-`flume_getTransactionsByParticipant`
-`flume_getTransactionReceiptsByParticipant` - All Take an address and an optional offset as arguments. 

#### ETH Methods


