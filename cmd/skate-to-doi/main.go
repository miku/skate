// Filter to parse out a correctly looking DOI from a field.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"runtime"
	"strings"

	"github.com/miku/skate/parallel"
)

var (
	numWorkers     = flag.Int("w", runtime.NumCPU(), "number of workers")
	batchSize      = flag.Int("b", 100000, "batch size")
	delimiter      = flag.String("d", "\t", "delimiter")
	index          = flag.Int("f", 1, "one field to cleanup up a doi, 1-indexed")
	bestEffort     = flag.Bool("B", false, "only log errors, but do not stop")
	skipNonMatches = flag.Bool("S", false, "do not emit a line for non-matches")

	PatDOI = regexp.MustCompile(`10.[0-9]{1,8}/[^ ]*[\w]`)
)

func main() {
	flag.Parse()
	pp := parallel.NewProcessor(os.Stdin, os.Stdout, func(p []byte) ([]byte, error) {
		parts := strings.Split(string(p), *delimiter)
		if len(parts) < *index {
			msg := fmt.Sprintf("warn: line has too few fields (%d): %s", len(parts), string(p))
			if *bestEffort {
				log.Println(msg)
				return nil, nil
			}
			return nil, fmt.Errorf(msg)
		}
		result := PatDOI.FindString(parts[*index-1])
		if len(result) == 0 {
			if *skipNonMatches {
				return nil, nil
			}
		} else {
			parts[*index-1] = result
		}
		return []byte(strings.Join(parts, *delimiter)), nil
	})
	pp.NumWorkers = *numWorkers
	pp.BatchSize = *batchSize
	if err := pp.Run(); err != nil {
		log.Fatal(err)
	}
}
