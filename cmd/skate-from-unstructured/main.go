// skate-from-unstructured tries to parse various pieces of information from an
// unstrctured string.
package main

import (
	"flag"
	"log"
	"os"
	"regexp"
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

	PatDOI = regexp.MustCompile(`10[.][0-9]{1,8}/[^ ]*[\w]`)
)

func main() {
	pp := parallel.NewProcessor(os.Stdin, os.Stdout, func(p []byte) ([]byte, error) {
		var ref skate.Ref
		if err := json.Unmarshal(p, &ref); err != nil {
			return nil, err
		}
		// TODO: ref
		if err := parseUnstructured(&ref); err != nil {
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

// parseUnstructured will in-place augment missing DOI, arxiv id and so on.
func parseUnstructured(ref *skate.Ref) error {
	uns := ref.Biblio.Unstructured
	doi := PatDOI.FindString(uns)
	if doi != "" && ref.Biblio.DOI == "" {
		ref.Biblio.DOI = doi
	}
	return nil
}
