// skate-derive-key derives a key from release entity JSON documents.
//
// $ skate-derive-key < release_entities.jsonlines > docs.tsv
//
// Result will be a three column TSV (ident, key, doc), LC_ALL=C sorted by key.
//
// ---- ident ---------------    ---- key ------------------------------     ---- doc ----------
//
// 4lzgf5wzljcptlebhyobccj7ru    2568diamagneticsusceptibilityofh8n2o10sr    {"abstracts":[],...
//
// After this step, a fast "itertools.groupby" or "skate-cluster" on key can yields clusters.
//
// Notes
//
// Using https://github.com/DataDog/zstd#stream-api, 3700 docs/s for key
// extraction only; using zstd -T0, we get 21K docs/s; about 13K docs/s; about
// 32h for 1.5B records.
//
// Default sort(1) buffer is 1K, but we'll need G's, e.g. -S35% of 48GB.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/miku/skate"
	"github.com/miku/skate/parallel"
)

var (
	keyFuncName = flag.String("f", "tsand", "key function name, other: title, tnorm, tnysi, tsand")
	numWorkers  = flag.Int("w", runtime.NumCPU(), "number of workers")
	batchSize   = flag.Int("b", 50000, "batch size")
	verbose     = flag.Bool("verbose", false, "show progress")
	bestEffort  = flag.Bool("B", false, "best effort")
	logFile     = flag.String("log", "", "log filename")

	wsReplacer = strings.NewReplacer("\t", "", "\n", "")
	keyOpts    = map[string]skate.IdentifierKeyFunc{
		"title": skate.KeyTitle,
		"tnorm": skate.KeyTitleNormalized,
		"tnysi": skate.KeyTitleNysiis,
		"tsand": skate.KeyTitleSandcrawler,
	}
	keyFunc skate.IdentifierKeyFunc
	ok      bool
)

func main() {
	flag.Parse()
	if keyFunc, ok = keyOpts[*keyFuncName]; !ok {
		log.Fatal("invalid key func")
	}
	if *logFile != "" {
		f, err := os.OpenFile(*logFile, os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		log.SetOutput(f)
	}
	started := time.Now()
	pp := parallel.NewProcessor(os.Stdin, os.Stdout, func(p []byte) ([]byte, error) {
		ident, key, err := keyFunc(p)
		if err != nil {
			if *bestEffort {
				log.Printf("keyFunc failed with %v: %v", err, string(p))
				return nil, nil
			}
			return nil, err
		}
		ident, key = strings.TrimSpace(ident), strings.TrimSpace(key)
		v := fmt.Sprintf("%s\t%s\t%s\n", ident, key, wsReplacer.Replace(string(p)))
		return []byte(v), nil
	})
	pp.NumWorkers = *numWorkers
	pp.BatchSize = *batchSize
	pp.Verbose = *verbose
	if err := pp.Run(); err != nil {
		log.Fatal(err)
	}
	log.Printf("took: %s", time.Since(started))
}
