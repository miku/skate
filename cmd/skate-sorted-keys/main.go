// skate-sorted-keys derives a key from JSON documents.
//
// $ zstdcat release_export_expanded.json.zst | skate-sorted-keys | zstd -c > keydocs.tsv
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
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"runtime"
	"strings"

	"github.com/miku/skate"
	"github.com/miku/skate/parallel"
)

var (
	keyFuncName     = flag.String("f", "tsand", "key function name, other: title, tnorm, tnysi")
	numWorkers      = flag.Int("w", runtime.NumCPU(), "number of workers")
	batchSize       = flag.Int("b", 50000, "batch size")
	compressProgram = flag.String("compress-program", "pzstd", "compress program name (only), passed to sort")
	sortBuffer      = flag.String("S", "30%", "sort -S")
	verbose         = flag.Bool("verbose", false, "show progress")
	tmpDir          = flag.String("T", os.TempDir(), "temp dir to use")
	skipSort        = flag.Bool("S", false, "skip sorting")

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
	if _, err := exec.LookPath("zstd"); err != nil {
		log.Fatal("zstd command line tool required")
	}
	if keyFunc, ok = keyOpts[*keyFuncName]; !ok {
		log.Fatal("invalid key func")
	}
	var (
		w    io.WriteCloser = os.Stdout
		err  error
		done = make(chan bool) // used for pipe
	)
	if !*skipSort {
		command := fmt.Sprintf("LC_ALL=C sort -S %s -k2,2 -T %q --compress-program %q", *sortBuffer, *tmpDir, *compressProgram)
		cmd := exec.Command("bash", "-c", command)
		if *verbose {
			log.Println(cmd.String())
		}
		cmd.Stderr = os.Stderr
		cmd.Stdout = os.Stdout
		w, err = cmd.StdinPipe() // Pipe in our release entities.
		if err != nil {
			log.Fatal(err)
		}
		done := make(chan bool)
		go func() {
			if err := cmd.Run(); err != nil {
				log.Fatal(err)
			}
			done <- true
		}()
	}
	pp := parallel.NewProcessor(os.Stdin, w, func(p []byte) ([]byte, error) {
		ident, key, err := keyFunc(p)
		if err != nil {
			return nil, err
		}
		v := fmt.Sprintf("%s\t%s\t%s\n", ident, key, wsReplacer.Replace(string(p)))
		return []byte(v), nil
	})
	pp.NumWorkers = *numWorkers
	pp.BatchSize = *batchSize
	pp.Verbose = *verbose
	if err := pp.Run(); err != nil {
		log.Fatal(err)
	}
	if !*skipSort {
		if err := w.Close(); err != nil {
			log.Fatal(err)
		}
		<-done
	}
}
