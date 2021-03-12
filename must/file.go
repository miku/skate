package must

import (
	"io/ioutil"
	"os"
)

// Open opens a file or panics.
func Open(filename string) *os.File {
	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	return f
}

// ReadFile reads a file or panics.
func ReadFile(filename string) []byte {
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err)
	}
	return b
}
