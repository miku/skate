// Experimental: Turn the minimal cluster result (key, target, source) into an
// indexable biblio ref (10eb30251f89806cb7a0f147f427c5ea7e5f9941).
//
// ---- id, title, ... --- ---- target -------------- ---- source --------------
//
// 10.1001/2012.jama.11164 zhscs2mjlvcdte2i3j44ibgzae icg7bkoeqvfqnc5t5ot4evto6a
// 10.1001/2012.jama.11164 zhscs2mjlvcdte2i3j44ibgzae ichuaiowbvbx5ajae5ing27lka
// 10.1001/2012.jama.11164 zhscs2mjlvcdte2i3j44ibgzae io6b76ow6ngxnilc24qsf5kw6i
//
// Input might change, so we keep this short.
package main

import (
	"flag"
	"log"
	"os"
	"runtime"
	"strings"

	jsoniter "github.com/json-iterator/go"
	"github.com/miku/skate"
	"github.com/miku/skate/parallel"
)

var (
	numWorkers = flag.Int("w", runtime.NumCPU(), "number of workers")
	batchSize  = flag.Int("b", 100000, "batch size")

	json         = jsoniter.ConfigCompatibleWithStandardLibrary
	bytesNewline = []byte("\n")
)

func main() {
	flag.Parse()
	pp := parallel.NewProcessor(os.Stdin, os.Stdout, func(p []byte) ([]byte, error) {
		fields := strings.Fields(string(p))
		br := skate.BiblioRef{
			SourceReleaseIdent: fields[2],
			TargetReleaseIdent: fields[1],
		}
		b, err := json.Marshal(br)
		b = append(b, bytesNewline...)
		return b, err
	})
	pp.NumWorkers = *numWorkers
	pp.BatchSize = *batchSize
	if err := pp.Run(); err != nil {
		log.Fatal(err)
	}
}
