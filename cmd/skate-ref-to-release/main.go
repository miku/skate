// skate-ref-to-release converts a "ref" document to a "release" document.
//
package main

import (
	"log"
	"os"

	"github.com/miku/parallel"
	"github.com/miku/skate"

	jsoniter "github.com/json-iterator/go"
)

var bytesNewline = []byte("\n")

func main() {
	pp := parallel.NewProcessor(os.Stdin, os.Stdout, func(p []byte) ([]byte, error) {
		var (
			json = jsoniter.ConfigCompatibleWithStandardLibrary
			ref  skate.Ref
		)
		if err := json.Unmarshal(p, &ref); err != nil {
			return nil, err
		}
		release, err := skate.RefToRelease(&ref)
		if err != nil {
			return nil, err
		}
		b, err := json.Marshal(release)
		b = append(b, bytesNewline...)
		return b, err
	})
	if err := pp.Run(); err != nil {
		log.Fatal(err)
	}
}
