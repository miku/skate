// skate-ref-to-release converts a "ref" document to a "release" document.
//
package main

import (
	"flag"
	"log"
	"os"
	"runtime"
	"strings"

	"github.com/miku/parallel"
	"github.com/miku/skate"

	jsoniter "github.com/json-iterator/go"
)

var (
	numWorkers = flag.Int("w", runtime.NumCPU(), "number of workers")
	batchSize  = flag.Int("b", 100000, "batch size")
	fromFormat = flag.String("f", "ref", "import data shape")

	json         = jsoniter.ConfigCompatibleWithStandardLibrary
	bytesNewline = []byte("\n")
)

func refToRelease(p []byte) ([]byte, error) {
	var ref skate.Ref
	if err := json.Unmarshal(p, &ref); err != nil {
		return nil, err
	}
	release, err := skate.RefToRelease(&ref)
	if err != nil {
		return nil, err
	}
	release.Extra.Skate.Status = "ref" // means: converted from ref
	release.Extra.Skate.Ref.Index = ref.Index
	release.Extra.Skate.Ref.Key = ref.Key
	b, err := json.Marshal(release)
	b = append(b, bytesNewline...)
	return b, err
}

func rgSitemapToRelease(p []byte) ([]byte, error) {
	var (
		s       skate.Sitemap
		release skate.Release
	)
	if err := json.Unmarshal(p, &s); err != nil {
		return nil, err
	}
	release.Title = s.Title
	if len(s.URL) > 41 {
		// XXX: A pseudo ident, maybe irritating.
		release.Ident = strings.Split(s.URL[41:], "_")[0]
	}
	release.Extra.Skate.Status = "rg"
	release.Extra.Skate.ResearchGate.URL = s.URL
	b, err := json.Marshal(release)
	b = append(b, bytesNewline...)
	return b, err
}

func main() {
	flag.Parse()
	switch *fromFormat {
	case "ref":
		pp := parallel.NewProcessor(os.Stdin, os.Stdout, refToRelease)
		pp.NumWorkers = *numWorkers
		pp.BatchSize = *batchSize
		if err := pp.Run(); err != nil {
			log.Fatal(err)
		}
	case "rg":
		pp := parallel.NewProcessor(os.Stdin, os.Stdout, rgSitemapToRelease)
		pp.NumWorkers = *numWorkers
		pp.BatchSize = *batchSize
		if err := pp.Run(); err != nil {
			log.Fatal(err)
		}
	}
}
