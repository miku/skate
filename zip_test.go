package skate

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"strings"
	"testing"
)

func TestZipper(t *testing.T) {
	a := strings.NewReader(`
k0 a
k1 b
k2 c
k3 d
`)
	b := strings.NewReader(`
k1 B
k3 D
`)
	keyFunc := func(s string) (string, error) {
		parts := strings.Fields(s)
		if len(parts) != 2 {
			return "", nil
		}
		return parts[0], nil
	}
	var buf bytes.Buffer
	groupFunc := func(g *GroupedCluster) error {
		fmt.Fprintf(&buf, "A=%v, B=%v ",
			sliceMap(g.A, strings.TrimSpace),
			sliceMap(g.B, strings.TrimSpace))
		return nil
	}
	err := Zipper(a, b, keyFunc, groupFunc, ioutil.Discard)
	if err != nil {
		t.Errorf("failed: %v", err)
	}
	want := "A=[k1 b], B=[k1 B] A=[k3 d], B=[k3 D]"
	if want != strings.TrimSpace(buf.String()) {
		t.Errorf("got %v, want %v", buf.String(), want)
	}
}

func sliceMap(ss []string, f func(string) string) (result []string) {
	for _, v := range ss {
		result = append(result, f(v))
	}
	return
}
