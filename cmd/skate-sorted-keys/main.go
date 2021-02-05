// skate-sorted-keys derives a key from JSON documents.
//
// $ zstdcat release_export_expanded.json.zst | skate-sorted-keys | zstd -c > keydocs.tsv
//
// Result will be a three column TSV (ident, key, doc), sorted (LC_ALL=C) by key.
//
// 4lzgf5wzljcptlebhyobccj7ru      2568diamagneticsusceptibilityofh8n2o10sr        {"abstracts":[],"refs":[],"contribs":[ ...
//
// After this step, an "itertools.groupby" on key can yield clusters.
//
// Notes
//
// Using https://github.com/DataDog/zstd#stream-api, 3700 docs/s for key
// extraction only; using zstd -T0, we get 21K docs/s; about 13K docs/s; about
// 32h for 1.5B records.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"runtime"
	"strings"

	"github.com/miku/parallel"
	"github.com/miku/skate"
)

var (
	keyFuncName     = flag.String("f", "tsand", "key function name")
	numWorkers      = flag.Int("w", runtime.NumCPU(), "number of workers")
	batchSize       = flag.Int("b", 50000, "batch size")
	compressProgram = flag.String("compress-program", "zstd", "compress program, passed to sort")
	verbose         = flag.Bool("verbose", false, "show progress")

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
	command := fmt.Sprintf("LC_ALL=C sort -k2,2 --compress-program %s", *compressProgram)
	cmd := exec.Command("bash", "-c", command)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	w, err := cmd.StdinPipe() // Pipe in our release entities.
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
	if *verbose {
		pp.LogEvery = 1000000
	}
	if err := pp.Run(); err != nil {
		log.Fatal(err)
	}
	if err := w.Close(); err != nil {
		log.Fatal(err)
	}
	<-done
}
