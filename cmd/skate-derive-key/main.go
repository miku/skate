// skate-derive-key derives a key from JSON documents.
//
// $ zstdcat release_export_expanded.json.zst | skate-derive | zstd -c > keydocs.tsv
//
// Result will be a three column TSV (ident, key, doc), sorted (LC_ALL=C) by key.
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
// On a 16-core box. About 40K sustained extraction, w/o sorting; sort very low
// CPU, down to 5%, even on /fast disk. 10M entries takes very long to sort
// (did not finish after 30min). Trying 2-step, "extract-sort"; 24 workers; E:
// 6min; S: 10+min.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"sync/atomic"

	"github.com/miku/skate"
	"github.com/miku/skate/parallel"
)

var (
	keyFuncName = flag.String("f", "tsand", "key function name, other: title, tnorm, tnysi")
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
	keyFunc           skate.IdentifierKeyFunc
	ok                bool
	counterEmptyKey   uint64
	counterEmptyIdent uint64
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
	bw := bufio.NewWriter(os.Stdout)
	defer bw.Flush()
	pp := parallel.NewProcessor(os.Stdin, bw, func(p []byte) ([]byte, error) {
		ident, key, err := keyFunc(p)
		if err != nil {
			if *bestEffort {
				log.Printf("keyFunc failed with %v: %v", err, string(p))
				return nil, nil
			}
			return nil, err
		}
		ident, key = strings.TrimSpace(ident), strings.TrimSpace(key)
		if key == "" {
			atomic.AddUint64(&counterEmptyKey, 1)
			return nil, nil
		}
		if ident == "" {
			atomic.AddUint64(&counterEmptyIdent, 1)
			return nil, nil
		}
		v := fmt.Sprintf("%s\t%s\t%s\n", ident, key, wsReplacer.Replace(string(p)))
		return []byte(v), nil
	})
	pp.NumWorkers = *numWorkers
	pp.BatchSize = *batchSize
	pp.Verbose = *verbose
	pp.LogFunc = func() {
		log.Printf("empty keys/idents: %d/%d", counterEmptyKey, counterEmptyIdent)
	}
	if err := pp.Run(); err != nil {
		log.Fatal(err)
	}
	log.Printf("docs with empty keys skipped: %d", counterEmptyKey)
	log.Printf("docs with empty ident skipped: %d", counterEmptyIdent)
}
