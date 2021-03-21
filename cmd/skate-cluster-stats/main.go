package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"

	jsoniter "github.com/json-iterator/go"
	"git.archive.org/martin/cgraph/skate"
	"git.archive.org/martin/cgraph/skate/parallel"
)

var (
	numWorkers = flag.Int("w", runtime.NumCPU(), "number of workers")
	batchSize  = flag.Int("b", 100000, "batch size")
	bestEffort = flag.Bool("B", false, "best effort, log errors")
	// unmatched: clusters w/ refs only
	// count: number of entities in cluster (by type)
	// default: key and number of values
	mode = flag.String("m", "", "what to extract (unmatched, count, ...)")

	json         = jsoniter.ConfigCompatibleWithStandardLibrary
	bytesNewline = []byte("\n")
)

type Func func([]byte) ([]byte, error)

func main() {
	flag.Parse()
	var f Func
	switch *mode {
	case "unmatched":
		f = func(p []byte) ([]byte, error) {
			var cluster skate.ClusterResult
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
			if refs == len(cluster.Values) {
				return p, nil
			}
			return nil, nil
		}
	case "count":
		f = func(p []byte) ([]byte, error) {
			var cluster skate.ClusterResult
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
			var cluster skate.ClusterResult
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
