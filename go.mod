module github.com/openrelayxyz/flume

go 1.18

require (
	github.com/gorilla/websocket v1.4.2
	github.com/inconshreveable/log15 v0.0.0-20201112154412-8562bdadbbac
	github.com/klauspost/compress v1.15.9
	github.com/mattn/go-sqlite3 v2.0.3+incompatible
	github.com/openrelayxyz/cardinal-evm v1.1.3
	github.com/openrelayxyz/cardinal-rpc v1.0.0
	github.com/openrelayxyz/cardinal-streams v1.1.0-init-memory-leak-0
	github.com/openrelayxyz/cardinal-types v1.1.1
	github.com/xsleonard/go-merkle v1.1.0
	golang.org/x/crypto v0.0.0-20210921155107-089bfa567519
	gopkg.in/yaml.v2 v2.4.0
)

require (
	github.com/NYTimes/gziphandler v1.1.1 // indirect
	github.com/Shopify/sarama v1.29.0 // indirect
	github.com/aws/aws-sdk-go v1.44.36 // indirect
	github.com/btcsuite/btcd/btcec/v2 v2.2.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.0.1 // indirect
	github.com/eapache/go-resiliency v1.2.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20180814174437-776d5712da21 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/go-stack/stack v1.8.1 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/go-cmp v0.5.7 // indirect
	github.com/hamba/avro v1.6.6 // indirect
	github.com/hashicorp/go-uuid v1.0.2 // indirect
	github.com/hashicorp/golang-lru v0.5.5-0.20210104140557-80c98217689d // indirect
	github.com/holiman/uint256 v1.2.0 // indirect
	github.com/jcmturner/gofork v1.0.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/openrelayxyz/cardinal-storage v1.1.1 // indirect
	github.com/openrelayxyz/drumline v0.4.0 // indirect
	github.com/pierrec/lz4 v2.5.2+incompatible // indirect
	github.com/pubnub/go-metrics-statsd v0.0.0-20170124014003-7da61f429d6b // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	github.com/rs/cors v1.7.0 // indirect
	github.com/savaki/cloudmetrics v0.0.0-20160314183336-c82bfea3c09e // indirect
	github.com/xdg/scram v1.0.3 // indirect
	github.com/xdg/stringprep v1.0.3 // indirect
	golang.org/x/net v0.0.0-20220615171555-694bf12d69de // indirect
	golang.org/x/sys v0.0.0-20220520151302-bc2c85ada10a // indirect
	golang.org/x/text v0.3.7 // indirect
	gopkg.in/jcmturner/aescts.v1 v1.0.1 // indirect
	gopkg.in/jcmturner/dnsutils.v1 v1.0.1 // indirect
	gopkg.in/jcmturner/gokrb5.v7 v7.5.0 // indirect
	gopkg.in/jcmturner/rpc.v1 v1.1.0 // indirect
)

replace github.com/Shopify/sarama => github.com/openrelayxyz/sarama v0.0.0-20200619041629-a7760f73892f
