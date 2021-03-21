// skate-from-unstructured tries to parse various pieces of information from an
// unstrctured string.
package main

import (
	"flag"
	"log"
	"os"
	"regexp"
	"runtime"
	"strings"

	jsoniter "github.com/json-iterator/go"
	"git.archive.org/martin/cgraph/skate"
	"git.archive.org/martin/cgraph/skate/parallel"
)

var (
	numWorkers   = flag.Int("w", runtime.NumCPU(), "number of workers")
	batchSize    = flag.Int("b", 100000, "batch size")
	json         = jsoniter.ConfigCompatibleWithStandardLibrary
	bytesNewline = []byte("\n")

	PatDOI         = regexp.MustCompile(`10[.][0-9]{1,8}/[^ ]*[\w]`)
	PatDOINoHyphen = regexp.MustCompile(`10[.][0-9]{1,8}/[^ -]*[\w]`)
	PatArxivPDF    = regexp.MustCompile(`http://arxiv.org/pdf/([0-9]{4,4}[.][0-9]{1,8})(v[0-9]{1,2})?(.pdf)?`)
	PatArxivAbs    = regexp.MustCompile(`http://arxiv.org/abs/([0-9]{4,4}[.][0-9]{1,8})(v[0-9]{1,2})?(.pdf)?`)
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
	var (
		v  string
		vs []string
	)
	// Handle things like: 10.1111/j.1550-7408.1968.tb02138.x-BIB5|cit5,
	// 10.1111/j.1558-5646.1997.tb02431.x-BIB0008|evo02431-cit-0008, ...
	if strings.Contains(strings.ToLower(ref.Key), "-bib") && ref.Biblio.DOI == "" {
		parts := strings.Split(strings.ToLower(ref.Key), "-bib")
		ref.Biblio.DOI = parts[0]
	}
	// DOI
	v = PatDOI.FindString(uns)
	if v != "" && ref.Biblio.DOI == "" {
		ref.Biblio.DOI = v
	}
	// DOI in Key
	v = PatDOINoHyphen.FindString(ref.Key)
	if v != "" && ref.Biblio.DOI == "" {
		ref.Biblio.DOI = v
	}
	// Arxiv
	vs = PatArxivPDF.FindStringSubmatch(uns)
	if len(vs) != 0 && ref.Biblio.ArxivId == "" {
		ref.Biblio.ArxivId = vs[1]
	} else {
		vs = PatArxivAbs.FindStringSubmatch(uns)
		if len(vs) != 0 && ref.Biblio.ArxivId == "" {
			ref.Biblio.ArxivId = vs[1]
		}
	}
	return nil
}
