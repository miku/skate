package skate

import (
	"encoding/json"
	"regexp"
)

// IdentifierKeyFunc returns an id and key from a given blob.
type IdentifierKeyFunc func([]byte) (string, string, error)

var (
	wsReplacer = strings.NewReplacer("\t", " ", "\n", " ")
	repeatedWs = regexp.MustCompile(`[ ]{2,}`)
)

type IdentTitleDoc struct {
	Ident string `json:"ident"`
	Title string `json:"ident"`
}

func KeyTitle(p []byte) (ident string, key string, err error) {
	var doc IdentTitleDoc
	if err = json.Unmarshal(p, &doc); err != nil {
		return
	}
	return doc.Ident, strings.Trimspace(doc.Title), nil
}

func KeyTitleNormalized(p []byte) (ident string, key string, err error) {
	ident, key, err = KeyTitle(p)
	key = repeatedWs.ReplaceAllString(key, " ")
}

func KeyTitleNysiis(p []byte) (string, string, error)      {}
func KeyTitleSandcrawler(p []byte) (string, string, error) {}
