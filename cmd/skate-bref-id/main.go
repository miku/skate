// skate-bref-id is a temporary helper to generate an id for a bref doc.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/miku/skate"
	"github.com/miku/skate/parallel"
)

var (
	numWorkers = flag.Int("w", runtime.NumCPU(), "number of workers")
	batchSize  = flag.Int("b", 100000, "batch size")
	json       = jsoniter.ConfigCompatibleWithStandardLibrary

	newlineB = []byte("\n")
)

func main() {
	pp := parallel.NewProcessor(os.Stdin, os.Stdout, func(p []byte) ([]byte, error) {
		var bref skate.BiblioRef
		if err := json.Unmarshal(p, &bref); err != nil {
			return nil, err
		}
		bref.Key = fmt.Sprintf("%s_%d", bref.SourceReleaseIdent, bref.RefIndex)
		bref.UpdateTs = time.Now().Unix()
		b, err := json.Marshal(bref)
		b = append(b, newlineB...)
		return b, err
	})
	pp.NumWorkers = *numWorkers
	pp.BatchSize = *batchSize
	if err := pp.Run(); err != nil {
		log.Fatal(err)
	}
}
