package skate

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func TestZipper(t *testing.T) {
	var cases = []struct {
		a string
		b string
		c string
	}{
		{
			"testdata/zip/c0a",
			"testdata/zip/c0b",
			"testdata/zip/c0c",
		},
		{
			"testdata/zip/c1a",
			"testdata/zip/c1b",
			"testdata/zip/c1c",
		},
	}
	keyFunc := func(s string) (string, error) {
		parts := strings.Fields(s)
		if len(parts) != 2 {
			return "", nil
		}
		return parts[0], nil
	}
	for _, c := range cases {
		ar := mustReadFileReader(c.a)
		br := mustReadFileReader(c.b)
		cr := strings.TrimSpace(mustReadFileString(c.c))
		var buf bytes.Buffer
		groupFunc := func(g *GroupedCluster) error {
			fmt.Fprintf(&buf, "A=%v, B=%v ",
				sliceMap(g.A, strings.TrimSpace),
				sliceMap(g.B, strings.TrimSpace))
			return nil
		}
		err := Zipper(ar, br, keyFunc, groupFunc, ioutil.Discard)
		if err != nil {
			t.Errorf("failed: %v", err)
		}
		got := strings.TrimSpace(buf.String())
		if cr != got {
			t.Errorf("got %v, want %v", got, cr)
		}
	}
}

func sliceMap(ss []string, f func(string) string) (result []string) {
	for _, v := range ss {
		result = append(result, f(v))
	}
	return
}

func mustReadFileReader(filename string) io.ReadCloser {
	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	return f
}

func mustReadFileString(filename string) string {
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err)
	}
	return string(b)
}
