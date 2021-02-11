package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"

	"github.com/goccy/go-json"
	"github.com/miku/skate"
	"github.com/miku/skate/parallel"
)

var (
	numWorkers = flag.Int("w", runtime.NumCPU(), "number of workers")
	batchSize  = flag.Int("b", 100000, "batch size")
	mode       = flag.String("m", "", "what to extract")

	// json = jsoniter.ConfigCompatibleWithStandardLibrary
)

type Func func([]byte) ([]byte, error)

func main() {
	flag.Parse()
	var f Func
	switch *mode {
	default:
		f = func(p []byte) ([]byte, error) {
			var cluster skate.Cluster
			if err := json.Unmarshal(p, &cluster); err != nil {
				return nil, nil
			}
			s := fmt.Sprintf("%d\t%s\n", len(cluster.Values), cluster.Key)
			return []byte(s), nil
		}
	}
	pp := parallel.NewProcessor(os.Stdin, os.Stdout, f)
	pp.NumWorkers = *numWorkers
	pp.BatchSize = *batchSize
	if err := pp.Run(); err != nil {
		log.Fatal(err)
	}
}
