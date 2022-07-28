package plugins

import (
	"io/ioutil"
	"path"
	"plugin"
	"strings"

	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/flume/config"
)

type pluginDetails struct {
	p *plugin.Plugin
	name string
}

type PluginLoader struct {
	Plugins     []pluginDetails
	LookupCache map[string][]interface{}
}

// var DefaultPluginLoader *PluginLoader


// func Lookup(name string, validate func(interface{}) bool) []interface{} {
// 	if DefaultPluginLoader == nil {
// 		log.Warn("Lookup attempted, but PluginLoader is not initialized", "name", name)
// 		return []interface{}{}
// 	}
// 	return DefaultPluginLoader.Lookup(name, validate)
// }

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

// func Initialize(target string, cfg *config.Config) (err error) {
// 	DefaultPluginLoader, err = NewPluginLoader(target)
// 	if err != nil {
// 		return err
// 	}
// 	DefaultPluginLoader.Initialize(cfg)
// 	return nil
// }

func (pl *PluginLoader) Initialize(cfg *config.Config) {
	fns := pl.Lookup("Initialize", func(i interface{}) bool {
		_, ok := i.(func(PluginLoader, *config.Config))
		return ok
	})
	for _, fni := range fns {
		if fn, ok := fni.(func(*PluginLoader, *config.Config)); ok {
			fn(pl, cfg)
		}
	}
}
