package zipkey

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"git.archive.org/martin/cgraph/skate/must"
)

func TestZipRun(t *testing.T) {
	makeKeyFunc := func(index int) func(string) (string, error) {
		return func(s string) (string, error) {
			parts := strings.Fields(s)
			if index >= len(parts) {
				return "", nil
			}
			return parts[index], nil
		}
	}
	var cases = []struct {
		a  string
		b  string
		c  string
		kf func(string) (string, error)
	}{
		{
			"testdata/c0a",
			"testdata/c0b",
			"testdata/c0c",
			makeKeyFunc(0),
		},
		{
			"testdata/c1a",
			"testdata/c1b",
			"testdata/c1c",
			makeKeyFunc(0),
		},
		{
			"testdata/c2a",
			"testdata/c2b",
			"testdata/c2c",
			makeKeyFunc(0),
		},
		{
			"testdata/c3a",
			"testdata/c3b",
			"testdata/c3c",
			makeKeyFunc(0),
		},
		{
			"testdata/c4a",
			"testdata/c4b",
			"testdata/c4c",
			makeKeyFunc(1),
		},
		{
			"testdata/c5a",
			"testdata/c5b",
			"testdata/c5c",
			makeKeyFunc(0),
		},
	}
	for _, c := range cases {
		var (
			ar        = must.Open(c.a)
			br        = must.Open(c.b)
			cr        = strings.TrimSpace(string(must.ReadFile(c.c)))
			buf       bytes.Buffer
			groupFunc = func(g *Group) error {
				return json.NewEncoder(&buf).Encode(g)
			}
			cm = New(ar, br, c.kf, groupFunc)
		)
		if err := cm.Run(); err != nil {
			t.Errorf("[%s] failed: %v", c.a, err)
		}
		if got := strings.TrimSpace(buf.String()); cr != got {
			t.Errorf("[%s ...] got %v, want %v", c.a, got, cr)
		}
	}
}
