package plugins

import (
	// "plugin"
	log "github.com/inconshreveable/log15"
	// "os"

	"github.com/openrelayxyz/plugeth-utils/core"

	// "flag"
	// "fmt"
	"io/ioutil"
	"path"
	"plugin"
	// "reflect"
	"strings"
	"github.com/openrelayxyz/flume/config"

	// "github.com/ethereum/go-ethereum/event"
	// "github.com/ethereum/go-ethereum/log"
	// "gopkg.in/urfave/cli.v1"
)

type pluginDetails struct {
	p *plugin.Plugin
	name string
}

type PluginLoader struct {
	Plugins     []pluginDetails
	LookupCache map[string][]interface{}
}

var DefaultPluginLoader *PluginLoader


func Lookup(name string, validate func(interface{}) bool) []interface{} {
	if DefaultPluginLoader == nil {
		log.Warn("Lookup attempted, but PluginLoader is not initialized", "name", name)
		return []interface{}{}
	}
	return DefaultPluginLoader.Lookup(name, validate)
}

func (pl *PluginLoader) Lookup(name string, validate func(interface{}) bool) []interface{} {
	if v, ok := pl.LookupCache[name]; ok {
		return v
	}
	results := []interface{}{}
	for _, plugin := range pl.Plugins {
		if v, err := plugin.p.Lookup(name); err == nil {
			if validate(v) {
				results = append(results, v)
			} else {
				log.Warn("Plugin matches hook but not signature", "plugin", plugin.name, "hook", name)
			}
		}
	}
	pl.LookupCache[name] = results
	return results
}

func NewPluginLoader(target string) (*PluginLoader, error) {
	pl := &PluginLoader{
		Plugins:     []pluginDetails{},
		LookupCache: make(map[string][]interface{}),
	}
	files, err := ioutil.ReadDir(target)
	if err != nil {
		log.Warn("Could not load plugins directory. Skipping.", "path", target)
		return pl, nil
	}
	for _, file := range files {
		fpath := path.Join(target, file.Name())
		if !strings.HasSuffix(file.Name(), ".so") {
			log.Debug("File inplugin directory is not '.so' file. Skipping.", "file", fpath)
			continue
		}
		plug, err := plugin.Open(fpath)
		if err != nil {
			log.Warn("File in plugin directory could not be loaded: %v", "file", fpath, "error", err.Error())
			continue
		}

		pl.Plugins = append(pl.Plugins, pluginDetails{plug, fpath})
	}
	return pl, nil
}

func Initialize(target string, cfg *config.Config) (err error) {
	DefaultPluginLoader, err = NewPluginLoader(target)
	if err != nil {
		return err
	}
	DefaultPluginLoader.Initialize(cfg)
	return nil
}

func (pl *PluginLoader) Initialize(cfg *config.Config) {
	fns := pl.Lookup("Initialize", func(i interface{}) bool {
		_, ok := i.(func(core.PluginLoader, *config.Config))
		return ok
	})
	for _, fni := range fns {
		if fn, ok := fni.(func(core.PluginLoader, *config.Config)); ok {
			fn(pl, cfg)
		}
	}
}

func (pl *PluginLoader) GetFeed() core.Feed {
	return nil 
}

// type Decoder interface {
// 	Decode(int64)
// }


// func (pl *PluginLoader) InitializePlugin (mod string, blockNo int64) {

// plug, err := plugin.Open(mod)
// if err != nil {
// 	log.Error("plugin error", "err", err.Error())
// }

// // 2. look up a symbol (an exported function or variable)
// // in this case, variable Greeter
// symGreeter, err := plug.Lookup("Decoder")
// if err != nil {
// 	log.Error("plugin lookup error", symGreeter, err.Error())
// }

// // 3. Assert that loaded symbol is of a desired type
// // in this case interface type Greeter (defined above)
// var decoder Decoder
// decoder, ok := symGreeter.(Decoder)
// if !ok {
// 	log.Error("plugin load error", "err", err.Error())
// }

// // 4. use the module
// Decoder.Decode(decoder, blockNo)


// }