package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"

	jsoniter "github.com/json-iterator/go"
	"github.com/miku/skate"
	"github.com/miku/skate/parallel"
)

var (
	numWorkers = flag.Int("w", runtime.NumCPU(), "number of workers")
	batchSize  = flag.Int("b", 100000, "batch size")
	bestEffort = flag.Bool("B", false, "best effort, log errors")
	mode       = flag.String("m", "", "what to extract")

	json = jsoniter.ConfigCompatibleWithStandardLibrary
)

type Func func([]byte) ([]byte, error)

func main() {
	flag.Parse()
	var f Func
	switch *mode {
	case "c":
		f = func(p []byte) ([]byte, error) {
			var cluster skate.Cluster
			if err := json.Unmarshal(p, &cluster); err != nil {
				if *bestEffort {
					log.Printf("%v", err)
					return nil, nil
				}
				log.Fatal(err)
			}
			var refs int
			for _, v := range cluster.Values {
				if v.Extra.Skate.Status == "ref" {
					refs++
				}
			}
			// total, refs, non-refs, key
			s := fmt.Sprintf("%d\t%d\t%d\t%s\n",
				len(cluster.Values), refs, len(cluster.Values)-refs, cluster.Key)
			return []byte(s), nil
		}
	default:
		f = func(p []byte) ([]byte, error) {
			var cluster skate.Cluster
			if err := json.Unmarshal(p, &cluster); err != nil {
				return nil, err
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
