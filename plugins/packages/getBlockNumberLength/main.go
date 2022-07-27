package main

import (
	// "github.com/openrelayxyz/plugeth-utils/core"
	// "gopkg.in/urfave/cli.v1"
	log "github.com/inconshreveable/log15"
)

// var log core.Logger

// func Initialize(ctx *cli.Context, loader core.PluginLoader, logger core.Logger) {
// 	log = logger
// 	log.Info("loaded Get Block Number Length plugin")
// }

func GetRPCCalls(lenBlockNumber int) {

	log.Info("BlockNumber list length is", "length", lenBlockNumber)

}
