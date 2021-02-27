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
	"runtime"

	jsoniter "github.com/json-iterator/go"
	"github.com/miku/skate"
	"github.com/miku/skate/parallel"
)

var (
	numWorkers = flag.Int("w", runtime.NumCPU(), "number of workers")
	batchSize  = flag.Int("b", 10000, "batch size")
	mode       = flag.String("m", "ref", "mode: ref, zip")

	json = jsoniter.ConfigCompatibleWithStandardLibrary
)

func main() {
	flag.Parse()
	switch *mode {
	case "ref":
		// https://git.io/JtACz
		pp := parallel.NewProcessor(os.Stdin, os.Stdout, func(p []byte) ([]byte, error) {
			var (
				cr  *skate.ClusterResult
				buf bytes.Buffer
			)
			if err := json.Unmarshal(p, &cr); err != nil {
				return nil, err
			}
			pivot, err := cr.NonRef()
			if err != nil {
				return nil, err
			}
			for _, re := range cr.Values {
				if re.Extra.Skate.Status != "ref" {
					continue
				}
				result := skate.Verify(pivot, re, 5)
				if _, err := fmt.Fprintf(&buf, "%s %s %s %s\n",
					pivot.Ident, re.Ident, result.Status, result.Reason); err != nil {
					return nil, err
				}
			}
			log.Printf("writing %db", buf.Len())
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
