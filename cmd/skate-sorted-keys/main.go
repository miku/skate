// skate-sorted-keys derives a key from a JSON document.
//
// This is a processing stage for clustering. Input is jsonlines of release
// docs, output is a TSV with id, key and the json doc, optionally sorted by
// key.
package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"runtime"
	"strings"

	"github.com/DataDog/zstd"
	"github.com/miku/clam"
	"github.com/miku/parallel"
	"github.com/miku/skate"
)

var (
	keyFuncName    = flag.String("f", "tsand", "key function name")
	numWorkers     = flag.Int("w", runtime.NumCPU(), "number of workers")
	batchSize      = flag.Int("b", 50000, "batch size")
	outputFilename = flag.String("o", "", "output filename")

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
	f, err := ioutil.TempFile("", "skate-sorted-keys-")
	if err != nil {
		log.Fatal(err)
	}
	zf := zstd.NewWriterLevel(f, 9)
	defer func() {
		if err := zf.Close(); err != nil {
			log.Fatal(err)
		}
		output, err := clam.RunOutput("zstdcat -T0 {input} | LC_ALL=C sort -k2,2 | zstd -c9 > {output}",
			clam.Map{"input": f.Name()})
		if err != nil {
			log.Fatal(err)
		}
		if *outputFilename != "" {
			if err := os.Rename(output, *outputFilename); err != nil {
				log.Fatal(err)
			}
		} else {
			log.Println(output)
		}
	}()
	pp := parallel.NewProcessor(os.Stdin, zf, func(p []byte) ([]byte, error) {
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
