// skate-ref-to-release converts a "ref" document to a "release" document.
//
package main

import (
	"flag"
	"log"
	"os"
	"runtime"

	"github.com/miku/parallel"
	"github.com/miku/skate"

	json "github.com/goccy/go-json"
)

var (
	numWorkers = flag.Int("w", runtime.NumCPU(), "number of workers")
	batchSize  = flag.Int("b", 100000, "batch size")

	bytesNewline = []byte("\n")
)

func main() {
	flag.Parse()
	pp := parallel.NewProcessor(os.Stdin, os.Stdout, func(p []byte) ([]byte, error) {
		var ref skate.Ref
		if err := json.Unmarshal(p, &ref); err != nil {
			return nil, err
		}
		release, err := skate.RefToRelease(&ref)
		if err != nil {
			return nil, err
		}
		release.Extra.Skate.Status = "ref" // means: converted from ref
		b, err := json.Marshal(release)
		b = append(b, bytesNewline...)
		return b, err
	})
	pp.NumWorkers = *numWorkers
	pp.BatchSize = *batchSize
	if err := pp.Run(); err != nil {
		log.Fatal(err)
	}
}
