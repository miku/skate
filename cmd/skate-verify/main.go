// Generate pairs and run verification on larger number of records. Mimick
// fuzzycat.verify, but make it faster (e.g. fuzzycat took about 50h for the
// complete set).
//
// Currently: about 2h for 40M clusters (in "ref" mode).
package main

import (
	"flag"
	"log"
	"os"
	"runtime"
	"runtime/pprof"

	jsoniter "github.com/json-iterator/go"
	"github.com/miku/skate"
	"github.com/miku/skate/parallel"
)

var (
	numWorkers   = flag.Int("w", runtime.NumCPU(), "number of workers")
	batchSize    = flag.Int("b", 10000, "batch size")
	mode         = flag.String("m", "ref", "mode: ref, zip")
	releasesFile = flag.String("R", "", "releases, tsv, sorted by key (zip mode only)")
	refsFile     = flag.String("F", "", "refs, tsv, sorted by key (zip mode only)")
	cpuProfile   = flag.String("cpuprofile", "", "write cpu profile to file")
	memProfile   = flag.String("memprofile", "", "write heap profile to file (go tool pprof -png --alloc_objects program mem.pprof > mem.png)")

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
	case "zip":
		// Take two "sorted key files" (one refs, one releases) and run
		// verification across groups.
		if *refsFile == "" || *releasesFile == "" {
			log.Fatal("zip mode requires -R and -F to be set")
		}
		f, err := os.Open(*releasesFile)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		g, err := os.Open(*refsFile)
		if err != nil {
			log.Fatal(err)
		}
		defer g.Close()
		if err := skate.ZipVerify(f, g); err != nil {
			log.Fatal(err)
		}
	case "ref":
		// https://git.io/JtACz
		pp := parallel.NewProcessor(os.Stdin, os.Stdout, skate.RefCluster)
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
