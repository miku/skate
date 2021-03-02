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
	"runtime/pprof"

	jsoniter "github.com/json-iterator/go"
	"github.com/miku/skate"
	"github.com/miku/skate/parallel"
)

var (
	numWorkers = flag.Int("w", runtime.NumCPU(), "number of workers")
	batchSize  = flag.Int("b", 10000, "batch size")
	mode       = flag.String("m", "ref", "mode: ref, zip")
	cpuProfile = flag.String("cpuprofile", "", "write cpu profile to file")
	memProfile = flag.String("memprofile", "", "write heap profile to file (go tool pprof -png --alloc_objects program mem.pprof > mem.png)")

	json = jsoniter.ConfigCompatibleWithStandardLibrary
)

func main() {
	flag.Parse()
	if *cpuProfile != "" {
		file, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(file)
		defer pprof.StopCPUProfile()
	}
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
			pivot, err := cr.OneNonRef()
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
	if *memProfile != "" {
		f, err := os.Create(*memProfile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close()
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal(err)
		}
	}
}
