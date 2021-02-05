// skate-sorted-keys derives a key from a JSON document.
//
// This is a processing stage for clustering. Input is jsonlines of release
// docs, output is a TSV with id, key and the json doc, optionally sorted by
// key.
//
// Notes: Using https://github.com/DataDog/zstd#stream-api, 3700 docs/s for key
// extraction only.
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

	"github.com/miku/parallel"
	"github.com/miku/skate"
)

var (
	keyFuncName     = flag.String("f", "tsand", "key function name")
	numWorkers      = flag.Int("w", runtime.NumCPU(), "number of workers")
	batchSize       = flag.Int("b", 50000, "batch size")
	outputFilename  = flag.String("o", "", "output filename")
	compressProgram = flag.String("compress-program", "zstd", "compress program")

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
	if *outputFilename == "" {
		f, err := ioutil.TempFile("", "skate-sorted-keys-")
		if err != nil {
			log.Fatal(err)
		}
		*outputFilename = f.Name()
	}
	// We have more complex cleanup logic in the key extraction functions,
	// which run in parallel; the rest of the pipeline is compressed unix
	// hackery.
	command := fmt.Sprintf("LC_ALL=C sort -k2,2 --compress-program %s", *compressProgram)
	if *outputFilename != "" {
		command = fmt.Sprintf("%s | %s -c9 > %s", command, *compressProgram, *outputFilename)
	}
	cmd := exec.Command("bash", "-c", command)
	w, err := cmd.StdinPipe()
	if err != nil {
		log.Fatal(err)
	}
	defer w.Close()
	go func() {
		if err := cmd.Run(); err != nil {
			log.Fatal(err)
		}
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
	if err := pp.Run(); err != nil {
		log.Fatal(err)
	}
}
