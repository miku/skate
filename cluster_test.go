package skate

import "testing"

func TestKeyTitleSandcrawler(t *testing.T) {
	var cases = []struct {
		b     []byte
		ident string
		key   string
		err   error
	}{
		{
			[]byte(`{"ident": "123", "title": "abc"}`),
			"123",
			"abc",
			nil,
		},
		{
			[]byte(`{"ident": "123", "title": "abc++***##???ßßß"}`),
			"123",
			"abcsss",
			nil,
		},
		{
			[]byte(`{"ident": "123", "title": "A k"}`),
			"123",
			"ak",
			nil,
		},
	}
	for _, c := range cases {
		ident, key, err := KeyTitleSandcrawler(c.b)
		if key != c.key {
			t.Errorf("[key] got %v, want %v", key, c.key)
		}
		if ident != c.ident {
			t.Errorf("[ident] got %v, want %v", ident, c.ident)
		}
		if err != c.err {
			t.Errorf("[err] got %v, want %v", err, c.err)
		}
	}
}
