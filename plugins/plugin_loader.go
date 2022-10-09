package plugins

import (
	// "io/ioutil"
	"path"
	"plugin"
	// "strings"

	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/flume/config"
)

type pluginDetails struct {
	p    *plugin.Plugin
	Name string
}

type PluginLoader struct {
	Plugins     []pluginDetails
	LookupCache map[string][]interface{}
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
				log.Warn("Plugin matches hook but not signature", "plugin", plugin.Name, "hook", name)
			}
		}
	}
	pl.LookupCache[name] = results
	return results
}

func NewPluginLoader(cfg *config.Config) (*PluginLoader, error) {
	pl := &PluginLoader{
		Plugins:     []pluginDetails{},
		LookupCache: make(map[string][]interface{}),
	}

	pluginDirectory := cfg.PluginDir

	for _, name := range cfg.Plugins {
		file := name + ".so"
		fpath := path.Join(pluginDirectory, file)
		plug, err := plugin.Open(fpath)
		if err != nil {
			log.Error("File in plugin directory could not be loaded: %v", "file", fpath, "error", err.Error())
			panic(err)
		}

		pl.Plugins = append(pl.Plugins, pluginDetails{plug, fpath})
	}
	log.Info("all required plugins loaded", "plugins", cfg.Plugins)
	return pl, nil
}

func (pl *PluginLoader) Initialize(cfg *config.Config) {
	fns := pl.Lookup("Initialize", func(i interface{}) bool {
		_, ok := i.(func(*config.Config, *PluginLoader))
		return ok
	})
	for _, fni := range fns {
		if fn, ok := fni.(func(*config.Config, *PluginLoader)); ok {
			fn(cfg, pl)
		}
	}
}
