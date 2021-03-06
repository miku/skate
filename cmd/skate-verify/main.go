// Generate pairs and run verification on larger number of records. Mimick
// fuzzycat.verify, but make it faster (e.g. fuzzycat took about 50h for the
// complete set).
//
// Currently: about 2h for 40M clusters (in "ref" mode).
//
// XXX: Cleanup inconsistent "modes".
package main

import (
	"bufio"
	"flag"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"

	jsoniter "github.com/json-iterator/go"
	"git.archive.org/martin/cgraph/skate"
	"git.archive.org/martin/cgraph/skate/parallel"
)

var (
	numWorkers   = flag.Int("w", runtime.NumCPU(), "number of workers")
	batchSize    = flag.Int("b", 10000, "batch size")
	mode         = flag.String("m", "ref", "mode: exact, ref, bref, zip, bzip")
	exactReason  = flag.String("r", "", "doi, pmid, pmcid, arxiv")
	provenance   = flag.String("p", "join", "provenance info")
	releasesFile = flag.String("R", "", "releases, tsv, sorted by key (zip mode only)")
	refsFile     = flag.String("F", "", "refs, tsv, sorted by key (zip mode only)")
	cpuProfile   = flag.String("cpuprofile", "", "write cpu profile to file")
	memProfile   = flag.String("memprofile", "", "write heap profile to file (go tool pprof -png --alloc_objects program mem.pprof > mem.png)")

	json = jsoniter.ConfigCompatibleWithStandardLibrary

	// XXX: This should be cleanup up soon.
	matchResults = map[string]skate.MatchResult{
		"doi":     skate.MatchResult{skate.StatusExact, skate.ReasonDOI},
		"pmid":    skate.MatchResult{skate.StatusExact, skate.ReasonPMID},
		"pmcid":   skate.MatchResult{skate.StatusExact, skate.ReasonPMCID},
		"arxiv":   skate.MatchResult{skate.StatusExact, skate.ReasonArxiv},
		"unknown": skate.MatchResult{skate.StatusUnknown, skate.ReasonUnknown},
	}
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
	case "exact":
		// Fixed zip mode for DOI.
		if *refsFile == "" || *releasesFile == "" {
			log.Fatal("mode requires -R and -F to be set")
		}
		if *exactReason == "" {
			var keys []string
			for k := range matchResults {
				keys = append(keys, k)
			}
			log.Fatalf("need a reason for the record, one of: %s", strings.Join(keys, ", "))
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
		bw := bufio.NewWriter(os.Stdout)
		defer bw.Flush()
		mr, ok := matchResults[*exactReason]
		if !ok {
			mr = matchResults["unknown"]
		}
		if err := skate.ZipUnverified(f, g, mr, *provenance, bw); err != nil {
			log.Fatal(err)
		}
	case "zip":
		// Take two "sorted key files" (one refs, one releases) and run
		// verification across groups, generate biblioref file.
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
		bw := bufio.NewWriter(os.Stdout)
		defer bw.Flush()
		if err := skate.ZipVerifyRefs(f, g, bw); err != nil {
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
	case "bref":
		// generate biblioref
		pp := parallel.NewProcessor(os.Stdin, os.Stdout, skate.RefClusterToBiblioRef)
		pp.NumWorkers = *numWorkers
		pp.BatchSize = *batchSize
		if err := pp.Run(); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatal("not implemented, only: zip, ref, bref")
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
