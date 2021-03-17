// skate-from-unstructured tries to parse various pieces of information from an
// unstrctured string.
package main

import (
	"flag"
	"log"
	"os"
	"runtime"

	jsoniter "github.com/json-iterator/go"
	"github.com/miku/skate"
	"github.com/miku/skate/parallel"
)

var (
	numWorkers   = flag.Int("w", runtime.NumCPU(), "number of workers")
	batchSize    = flag.Int("b", 100000, "batch size")
	json         = jsoniter.ConfigCompatibleWithStandardLibrary
	bytesNewline = []byte("\n")
)

func main() {
	pp := parallel.NewProcessor(os.Stdin, os.Stdout, func(p []byte) ([]byte, error) {
		var ref skate.Ref
		if err := json.Unmarshal(p, &ref); err != nil {
			return nil, err
		}
		b, err := json.Marshal(ref)
		if err != nil {
			return nil, err
		}
		b = append(b, bytesNewline...)
		return b, nil
	})
	pp.NumWorkers = *numWorkers
	pp.BatchSize = *batchSize
	if err := pp.Run(); err != nil {
		log.Fatal(err)
	}
}
