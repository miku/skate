// Generate pairs and run verification on larger number of records. Mimick
// fuzzycat.verify, but make it faster (e.g. fuzzycat took about 50h for the
// complete set).
//
// TODO
package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"runtime"

	jsoniter "github.com/json-iterator/go"
	"github.com/miku/skate"
	"github.com/miku/skate/parallel"
)

var (
	numWorkers = flag.Int("w", runtime.NumCPU(), "number of workers")
	batchSize  = flag.Int("b", 100000, "batch size")
	mode       = flag.String("m", "ref", "mode: ref, zip")

	json = jsoniter.ConfigCompatibleWithStandardLibrary

	// "release_year":"2011"
	PatYear = regexp.MustCompile(`"release_year":("[0-9]{4,4}")`)
)

type Doc struct {
	Key  string           `json:"k"`
	Docs []*skate.Release `json:"v"`
}

func (doc *Doc) NonRef() (*skate.Release, error) {
	for _, doc := range doc.Docs {
		if doc.Extra.Skate.Status != "ref" {
			return doc, nil
		}
	}
	return nil, fmt.Errorf("no release found")
}

func main() {
	flag.Parse()
	switch *mode {
	case "ref":
		// https://github.com/miku/fuzzycat/blob/b1f5d4fe1f32ceaaf7f380bb620ff2c742d5f356/fuzzycat/refs.py#L44-L71
		pp := parallel.NewProcessor(os.Stdin, os.Stdout, func(p []byte) ([]byte, error) {
			var (
				doc Doc
				buf bytes.Buffer
			)
			if err := json.Unmarshal(p, &doc); err != nil {
				return nil, err
			}
			pivot, err := doc.NonRef()
			if err != nil {
				return nil, err
			}
			for _, e := range doc.Docs {
				result := skate.Verify(pivot, e, 5)
				if _, err := fmt.Fprintf(&buf, "%s %s %s %s\n", pivot.Ident, e.Ident, result.Status, result.Reason); err != nil {
					return nil, err
				}
			}
			return buf.Bytes(), nil
		})
		pp.NumWorkers = *numWorkers
		pp.BatchSize = *batchSize
		if err := pp.Run(); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatal("not implemented")
	}
}
