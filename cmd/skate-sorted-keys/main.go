// skate-sorted-keys derives a key from a JSON document.
//
// This is a processing stage for clustering. Input is jsonlines of release
// docs, output is a TSV with id, key and the json doc, optionally sorted by
// key.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"

	"github.com/miku/parallel"
	"github.com/miku/skate"
)

var keyOpts = map[string]skate.IdentifierKeyFunc{
	"title": skate.KeyTitle,
	"tnorm": skate.KeyTitleNormalized,
	"tnysi": skate.KeyTitleNysiis,
	"tsand": skate.KeyTitleSandcrawler,
}

var (
	keyFuncName = flag.String("f", "tsand", "key function name")
	numWorkers  = flag.Int("w", runtime.NumCPU(), "number of workers")
	batchSize   = flag.Int("b", 100000, "batch size")

	wsReplacer = strings.NewReplacer("\t", "", "\n", "")
)

func main() {
	flag.Parse()
	keyFunc, ok := keyOpts[*keyFuncName]
	if !ok {
		log.Fatal("invalid key func")
	}
	pp := parallel.NewProcessor(os.Stdin, os.Stdout, func(p []byte) ([]byte, error) {
		ident, key, err := keyFunc(p)
		if err != nil {
			return nil, err
		}
		v := fmt.Sprintf("%s\t%s\t%s\n", ident, key, wsReplacer.Replace(string(p)))
		return []byte(v), nil
	})
	pp.NumWorkers = *numWorkers
	pp.BatchSize = *batchSize
	if err := pp.Run(); err != nil {
		log.Fatal(err)
	}
}
