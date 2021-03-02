package skate

import (
	"fmt"
	"io/ioutil"
	"strings"
	"testing"
)

func TestSlugifyString(t *testing.T) {
	var cases = []struct {
		s      string
		result string
	}{
		{"", ""},
		{" ", ""},
		{" Optimize everything", "optimize everything"},
		{"ABCü~", "abc"},
	}
	for _, c := range cases {
		got := slugifyString(c.s)
		if got != c.result {
			t.Errorf("slugifyString: '%v', want '%v', got '%v'", c.s, c.result, got)
		}
	}
}
func TestLooksLikeComponent(t *testing.T) {
	var cases = []struct {
		a, b   string
		result bool
	}{
		{"", "", false},
		{"100.1", "100", true},
		{"100ABC.123", "100ABC", true},
		{"100ABC", "100ABC.1", true},
	}
	for _, c := range cases {
		got := looksLikeComponent(c.a, c.b)
		if got != c.result {
			t.Errorf("looksLikeComponent: %v %v, want %v, got %v", c.a, c.b, c.result, got)
		}
	}
}

func TestVerify(t *testing.T) {
	data, err := ioutil.ReadFile("testdata/verify.csv")
	if err != nil {
		t.Errorf("could not load test data: %v", err)
	}
	cases := strings.Split(string(data), "\n")
	t.Logf("running %d test cases from https://git.io/JtjRL", len(cases))
	for _, line := range cases {
		line = strings.TrimSpace(line)
		fields := strings.Split(line, ",")
		if len(fields) < 4 {
			continue
		}
		a, b, status, reason := fields[0], fields[1], fields[2], fields[3]
		if status == "" {
			continue
		}
		ar, err := parseRelease(a)
		if err != nil {
			t.Errorf("could not load release: %s", a)
		}
		br, err := parseRelease(b)
		if err != nil {
			t.Errorf("could not load release: %s", a)
		}
		result := Verify(ar, br, 5)
		if !statusEquals(result.Status.String(), status) {
			t.Errorf("%s %s: got %s (%s), want %s (%s) ", a, b, result.Status, result.Reason, status, reason)
		}
	}
}

// parseRelease uses the testdata dir to load a release.
func parseRelease(ident string) (*Release, error) {
	filename := fmt.Sprintf("testdata/release/%s", ident)
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	var r Release
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

// statusEquals compares status variants (from fuzzycat, skate), e.g.
// StatusExact, Status.EXACT
func statusEquals(s, t string) bool {
	s = strings.Replace(strings.ToLower(s), ".", "", -1)
	t = strings.Replace(strings.ToLower(t), ".", "", -1)
	return s == t
}
