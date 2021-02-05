// skate-cluster takes the output of skate-sorted-keys and generates a
// "cluster" document.

package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

func main() {
	var (
		br    = bufio.NewReader(os.Stdin)
		bw    = bufio.NewWriter(os.Stdout)
		prev  string
		batch []string
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
		if len(fields) != 3 {
			log.Fatal(err)
		}
		key, doc := fields[1], fields[2]
		if prev != key {
			if err := writeBatch(bw, key, batch); err != nil {
				log.Fatal(err)
			}
			batch = nil
		}
		batch = append(batch, doc)
		prev = key
	}
	if err := writeBatch(bw, prev, batch); err != nil {
		log.Fatal(err)
	}
}

func writeBatch(w io.Writer, key string, batch []string) (err error) {
	if len(batch) == 0 {
		return nil
	}
	_, err = fmt.Fprintf(w, `{"%s": [%s]}\n`, key, strings.Join(batch, ","))
	return
}
