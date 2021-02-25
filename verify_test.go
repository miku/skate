package skate

import "testing"

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
