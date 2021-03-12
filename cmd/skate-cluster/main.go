// skate-cluster takes the output of skate-sorted-keys and generates a
// "cluster" document, grouping docs by key. Can do some pre-filtering.
//
// For example, this:
//
//     id123    somekey123    {"a":"b", ...}
//     id391    somekey123    {"x":"y", ...}
//
// would turn into (a single line containing all docs with the same key).
//
//     {"k": "somekey123", "v": [{"a":"b", ...},{"x":"y",...}]}
//
// A single line cluster is easier to parallelize (e.g. for verification, etc.).
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

var (
	keyField       = flag.Int("k", 2, "which column contains the key (one based)")
	docField       = flag.Int("d", 3, "which column contains the doc")
	minClusterSize = flag.Int("min", 2, "minimum cluster size")
	maxClusterSize = flag.Int("max", 100000, "maximum cluster size")
	requireBoth    = flag.Bool("both", false,
		"require at least one ref and one non-ref item present in the cluster, implies -min 2")
	dropEmptyKeys = flag.Bool("D", false, "drop empty keys")
)

func main() {
	flag.Parse()
	var (
		br             = bufio.NewReader(os.Stdin)
		bw             = bufio.NewWriter(os.Stdout)
		prev, key, doc string
		batch, fields  []string
		keyIndex       = *keyField - 1
		docIndex       = *docField - 1
	)
	defer bw.Flush()
	for {
		line, err := br.ReadString('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		fields = strings.Split(line, "\t")
		if len(fields) <= keyIndex || len(fields) <= docIndex {
			log.Fatalf("line has only %d fields", len(fields))
		}
		key = strings.TrimSpace(fields[keyIndex])
		if *dropEmptyKeys && len(key) == 0 {
			continue
		}
		doc = strings.TrimSpace(fields[docIndex])
		if prev != key {
			if err := writeBatch(bw, key, batch); err != nil {
				log.Fatal(err)
			}
			batch = nil
		}
		prev = key
		batch = append(batch, doc)
	}
	if len(batch) > 0 {
		if err := writeBatch(bw, prev, batch); err != nil {
			log.Fatal(err)
		}
	}
}

// containsBoth return true, if we have a ref and a non-ref item in the batch.
func containsBoth(batch []string) bool {
	var isRef int
	for _, doc := range batch {
		// This is brittle. Most JSON should be in compact form, and there the
		// following chars are by convention added to distinguish a release
		// coming from a reference doc from other releases.
		if strings.Contains(doc, `"status":"ref"`) {
			isRef++
		}
	}
	return isRef > 0 && isRef < len(batch)
}

// writeBatch writes out a single line containing the key and the cluster values.
func writeBatch(w io.Writer, key string, batch []string) (err error) {
	if len(batch) < *minClusterSize || len(batch) > *maxClusterSize {
		return nil
	}
	if *requireBoth && !containsBoth(batch) {
		return nil
	}
	// This is brittle, but all items in a batch are valid JSON objects, hence,
	// the following will be valid JSON as well, or will it? The key should not
	// contain a quote.
	_, err = fmt.Fprintf(w, "{\"k\": \"%s\", \"v\": [%s]}\n", key, strings.Join(batch, ","))
	return
}
