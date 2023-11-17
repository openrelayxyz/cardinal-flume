package config

import (
	"database/sql"
	"context"
	"errors"
	"fmt"
	"math/big"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	log "github.com/inconshreveable/log15"
	
	"github.com/openrelayxyz/cardinal-streams/transports"
	"github.com/openrelayxyz/cardinal-types"
)

type statsdOpts struct {
	Address  string `yaml:"address"`
	Port     string `yaml:"port"`
	Prefix   string `yaml:"prefix"`
	Interval int64  `yaml:"interval.sec"`
	Minor    bool   `yaml:"include.minor"`
}

type cloudwatchOpts struct {
	Namespace   string            `yaml:"namespace"`
	Dimensions  map[string]string `yaml:"dimensions"`
	Interval    int64             `yaml:"interval.sec"`
	Percentiles []float64         `yaml:"percentiles"`
	Minor       bool              `yaml:"include.minor"`
}

type broker struct {
	URL               string `yaml:"url"`
	DefaultTopic      string `yaml:"default.topic"`
	BlockTopic        string `yaml:"block.topic"`
	TransactionsTopic string `yaml:"transactions.topic"`
	LogsTopic         string `yaml:"logs.topic"`
	ReceiptTopic      string `yaml:"receipts.topic"`
	Rollback          int64  `yaml:"rollback"`
}

type Config struct {
	Port            int64             `yaml:"port"`
	PprofPort       int               `yaml:"pprofPort"`
	HealthcheckPort int64             `yaml:"healthcheck"`
	MinSafeBlock    int               `yaml:"minSafeBlock"`
	Network         string            `yaml:"networkName"`
	Chainid         uint64            `yaml:"chainid"`
	HomesteadBlock  uint64            `yaml:"homesteadBlock"`
	Eip155Block     uint64            `yaml:"eip155Block"`
	TxTopic         string            `yaml:"mempoolTopic"`
	WhitelistInternal map[uint64]string `yaml:"whitelist"`
	KafkaRollback   int64             `yaml:"kafkaRollback"`
	ReorgThreshold  int64             `yaml:"reorgThreshold"`
	Databases       map[string]string `yaml:"databases"`
	MempoolSlots    int               `yaml:"mempoolSize"`
	MemTxTimeThreshold int64          `yaml:"mempoolTxTime"` //mempool tx expiration in miuntes
	BlockWaitDuration int64           `yaml:"blockWaitDuration"` // number of miliseconds to wait for a block from charon
	Concurrency     int               `yaml:"concurrency"`
	LogLevel        string            `yaml:"loggingLevel"`
	Plugins         []string          `yaml:"plugins"`
	PluginDir       string            `yaml:"pluginPath"`
	Brokers         []broker          `yaml:"brokers"`
	BrokerParams    []transports.BrokerParams
	Statsd          *statsdOpts     `yaml:"statsd"`
	CloudWatch      *cloudwatchOpts `yaml:"cloudwatch"`
	HeavyServer   	string `yaml:"heavyserver"`
	EarliestBlock 	uint64 
	LatestBlock   	uint64
	BaseFeeChangeBlockHeight uint64
	LightSeed       int64
	ExtraConfig     map[string]map[string]string `yaml:extra`
	WhitelistExternal map[uint64]types.Hash
}

func LoadConfig(fname string) (*Config, error) {
	data, err := ioutil.ReadFile(fname)
	if err != nil {
		return nil, err
	}
	cfg := Config{}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	cfg.PluginDir = cfg.PluginDir + "plugins"

	switch cfg.Network {
	case "mainnet", "eth":
		cfg.HomesteadBlock = 1150000
		cfg.Eip155Block = 2675000
		cfg.Chainid = 1
	case "classic", "etc":
		cfg.HomesteadBlock = 1150000
		cfg.Eip155Block = 3000000
		cfg.Chainid = 61
	case "ropsten":
		cfg.HomesteadBlock = 0
		cfg.Eip155Block = 10
		cfg.Chainid = 3
	case "rinkeby":
		cfg.HomesteadBlock = 1
		cfg.Eip155Block = 3
		cfg.Chainid = 4
	case "goerli":
		cfg.HomesteadBlock = 0
		cfg.Eip155Block = 0
		cfg.Chainid = 5
	case "sepolia":
		cfg.Chainid = 11155111
	case "holesky":
		cfg.HomesteadBlock = 0
		cfg.Eip155Block = 0
		cfg.Chainid = 17000
	case "kiln":
		cfg.Chainid = 1337802
	case "polygon":
		cfg.HomesteadBlock = 0
		cfg.Eip155Block = 0
		cfg.Chainid = 137
		cfg.BaseFeeChangeBlockHeight = 38189056
	case "mumbai":
		cfg.HomesteadBlock = 0
		cfg.Eip155Block = 0
		cfg.Chainid = 80001
	case "":
		if cfg.Chainid == 0 {
			err := errors.New("Network name, eipp155Block, and homestead Block values must be set in configuration file")
			return nil, err
		} //if chainid is not zero we assume the other fields are valid
	default:
		err := errors.New("Unrecognized network name")
		return nil, err
	}

	if cfg.BaseFeeChangeBlockHeight == 0 {
		cfg.BaseFeeChangeBlockHeight = 1000000000
	}

	var logLvl log.Lvl
	switch cfg.LogLevel {
	case "debug":
		logLvl = log.LvlDebug
	case "info":
		logLvl = log.LvlInfo
	case "warn":
		logLvl = log.LvlWarn
	case "error":
		logLvl = log.LvlError
	default:
		logLvl = log.LvlInfo
	}

	log.Root().SetHandler(log.LvlFilterHandler(logLvl, log.Root().GetHandler()))

	if cfg.Port == 0 {
		cfg.Port = 8000
	}

	if cfg.PprofPort == 0 {
		cfg.PprofPort = 6969
	}
	if cfg.HealthcheckPort == 0 {
		cfg.HealthcheckPort = 9999
	}
	if cfg.MinSafeBlock == 0 {
		cfg.MinSafeBlock = 1000000
	}
	if cfg.KafkaRollback == 0 {
		cfg.KafkaRollback = 5000
	}
	if cfg.ReorgThreshold == 0 {
		cfg.ReorgThreshold = 128
	}
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 16
	}
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("Config must specify at least one broker")
	}
	cfg.BrokerParams = make([]transports.BrokerParams, len(cfg.Brokers))
	for i := range cfg.Brokers {
		if cfg.Brokers[i].DefaultTopic == "" {
			cfg.Brokers[i].DefaultTopic = fmt.Sprintf("cardinal-%v", cfg.Chainid)
		}
		if cfg.Brokers[i].BlockTopic == "" {
			cfg.Brokers[i].BlockTopic = fmt.Sprintf("%v-block", cfg.Brokers[i].DefaultTopic)
		}
		if cfg.Brokers[i].LogsTopic == "" {
			cfg.Brokers[i].LogsTopic = fmt.Sprintf("%v-logs", cfg.Brokers[i].DefaultTopic)
		}
		if cfg.Brokers[i].TransactionsTopic == "" {
			cfg.Brokers[i].TransactionsTopic = fmt.Sprintf("%v-tx", cfg.Brokers[i].DefaultTopic)
		}
		if cfg.Brokers[i].ReceiptTopic == "" {
			cfg.Brokers[i].ReceiptTopic = fmt.Sprintf("%v-receipt", cfg.Brokers[i].DefaultTopic)
		}
		if cfg.Brokers[i].Rollback == 0 {
			cfg.Brokers[i].Rollback = 500
		}
		cfg.BrokerParams[i] = transports.BrokerParams{
			URL:          cfg.Brokers[i].URL,
			DefaultTopic: cfg.Brokers[i].DefaultTopic,
			Topics: []string{
				cfg.Brokers[i].DefaultTopic,
				cfg.Brokers[i].BlockTopic,
				cfg.Brokers[i].LogsTopic,
				cfg.Brokers[i].TransactionsTopic,
				cfg.Brokers[i].ReceiptTopic,
			},
			Rollback: cfg.Brokers[i].Rollback,
		}
	}
	
	if cfg.CloudWatch != nil {
		if cfg.CloudWatch.Namespace == "" {
			cfg.CloudWatch.Namespace = "Flume"
		}
	}
	
	if cfg.MemTxTimeThreshold == 0 {
		cfg.MemTxTimeThreshold = 60
	}

	if cfg.BlockWaitDuration == 0 {
		cfg.BlockWaitDuration = 200
		// this value was calculated as roughly the 95th percentile of block processing times on flume light. Heavey instances
		// will need to be adjusted higher. 
	}

	cfg.WhitelistExternal = make(map[uint64]types.Hash)
	for k, v := range cfg.WhitelistInternal {
		cfg.WhitelistExternal[k] = types.HexToHash(v)
	}
	
	return &cfg, nil
}

var (
	preForkDenominator = big.NewInt(8)
	postForkDenominator = big.NewInt(16)
  )

func (cfg *Config) GetBaseFeeDenominator(db *sql.DB) *big.Int {

	var blockNumber uint64
	db.QueryRowContext(context.Background(), "SELECT max(number) FROM blocks.blocks;").Scan(&blockNumber)

	if blockNumber > cfg.BaseFeeChangeBlockHeight {
		return postForkDenominator
	}
	return preForkDenominator
}
