// skate-ref-to-release converts a "ref" document to a "release" document.
//
package main

import (
	"encoding/json"
	"log"
	"os"

	"github.com/miku/parallel"
	"github.com/miku/skate"
)

func main() {
	pp := parallel.NewProcessor(os.Stdin, os.Stdout, func(p []byte) ([]byte, error) {
		var ref skate.Ref
		if err := json.Unmarshal(p, &ref); err != nil {
			return nil, err
		}
		release, err := skate.RefToRelease(&ref)
		if err != nil {
			return nil, err
		}
		return json.Marshal(release)
	})
	if err := pp.Run(); err != nil {
		log.Fatal(err)
	}
}
