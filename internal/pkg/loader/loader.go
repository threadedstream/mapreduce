package loader

import (
	"log"
	"plugin"

	"github.com/threadedstream/mapreduce/internal/mr"
)

// LoadPlugin loads `filename` plugin
func LoadPlugin(filename string) (mr.MapFn, mr.ReduceFn) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v, reason: %s", filename, err)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v, reason: %s", filename, err)
	}
	mapf := xmapf.(mr.MapFn)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v, reason: %s", filename, err)
	}
	reducef := xreducef.(mr.ReduceFn)

	return mapf, reducef
}
