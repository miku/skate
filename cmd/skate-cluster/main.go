// skate-cluster takes the output of skate-sorted-keys and generates a
// "cluster" document, grouping docs by key. Can do some pre-filtering.
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
		br       = bufio.NewReader(os.Stdin)
		bw       = bufio.NewWriter(os.Stdout)
		prev     string
		batch    []string
		keyIndex = *keyField - 1
		docIndex = *docField - 1
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
		fields := strings.Split(line, "\t")
		if len(fields) <= keyIndex || len(fields) <= docIndex {
			log.Fatalf("line has only %d fields", len(fields))
		}
		key := strings.TrimSpace(fields[keyIndex])
		if *dropEmptyKeys && key == "" {
			continue
		}
		doc := strings.TrimSpace(fields[docIndex])
		if prev != key {
			if err := writeBatch(bw, key, batch); err != nil {
				log.Fatal(err)
			}
			batch = nil
		}
		prev, batch = key, append(batch, doc)
	}
	if err := writeBatch(bw, prev, batch); err != nil {
		log.Fatal(err)
	}
}

// containsBoth return true, if we have a ref and a non-ref item in the batch.
func containsBoth(batch []string) bool {
	var isRef int
	for _, doc := range batch {
		// "ugly, but faster"
		if strings.Contains(doc, `"extra":{"skate":{"status":"ref"`) {
			isRef++
		}
	}
	return isRef > 0 && isRef < len(batch)
}

func writeBatch(w io.Writer, key string, batch []string) (err error) {
	if len(batch) < *minClusterSize || len(batch) > *maxClusterSize {
		return nil
	}
	if *requireBoth && !containsBoth(batch) {
		return nil
	}
	// "ugly, but faster"
	_, err = fmt.Fprintf(w, "{\"k\": \"%s\", \"v\": [%s]}\n", key, strings.Join(batch, ","))
	return
}
