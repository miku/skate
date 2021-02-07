// skate-cluster takes the output of skate-sorted-keys and generates a
// "cluster" document.

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
	keyField = flag.Int("k", 2, "which column contains the key (one based)")
	docField = flag.Int("d", 3, "which column contains the doc")
	// XXX: add max cluster size
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
		key, doc := fields[keyIndex], fields[docIndex]
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

func writeBatch(w io.Writer, key string, batch []string) (err error) {
	if len(batch) == 0 {
		return nil
	}
	// ugly, but faster
	_, err = fmt.Fprintf(w, "{\"k\": \"%s\", \"v\": [%s]}\n",
		key, strings.Join(batch, ","))
	return
}
