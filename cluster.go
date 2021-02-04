package skate

import (
	"encoding/json"
	"regexp"
	"strings"

	"golang.org/x/text/unicode/norm"
)

// IdentifierKeyFunc returns the id and some key from a given blob.
type IdentifierKeyFunc func([]byte) (string, string, error)

var (
	wsReplacer = strings.NewReplacer("\t", " ", "\n", " ")
	repeatedWs = regexp.MustCompile(`[ ]{2,}`)
	nonWord    = regexp.MustCompile(`[\W]+`)

	SandcrawlerCharMap = map[string]string{
		"\u00c6": "AE",
		"\u00e6": "ae",
		"\u00d0": "D",
		"\u00f0": "d",
		"\u00d8": "O",
		"\u00f8": "o",
		"\u00de": "Th",
		"\u00fe": "th",
		"\u00df": "s",
		"\u0110": "D",
		"\u0111": "d",
		"\u0126": "H",
		"\u0127": "h",
		"\u0131": "i",
		"\u0138": "k",
		"\u0141": "L",
		"\u0142": "l",
		"\u014a": "N",
		"\u014b": "n",
		"\u0152": "Oe",
		"\u0153": "oe",
		"\u0166": "T",
		"\u0167": "t",
		"\u00b5": "u",
		"c":      "c",
		"\u0192": "f",
		"\u2202": "",
		"\u0296": "",
		"\u2211": "",
		"\u220f": "",
		"\u02c6": "",
		"\u2603": "",
		"\u02c7": "",
	}
	SandcrawlerPrefixRemove = []string{
		"original article: ", "original article ", "article: ", "title: ",
	}
	// SandcrawlerPrefixRemove misses classes: Punctuation, M, InCombiningDiacriticalMarks.
	SandcrawlerRemoveCharRegex = regexp.MustCompile(`[!?.\s\u2000-\u206F\u2E00-\u2E7F’\u0060·“”‘’“”«»「」¿–±§_°ʖ©®¤=<>|+$^~≈√∫≤≥÷ƒ∆¬£¢∞¥◊€]`)
)

// IdentTitleDoc is a minimal subset of fields, we can work with.
type IdentTitleDoc struct {
	Ident string `json:"ident"`
	Title string `json:"ident"`
}

// KeyTitle is extract the title, and slight cleaning.
func KeyTitle(p []byte) (ident string, key string, err error) {
	var doc IdentTitleDoc
	if err = json.Unmarshal(p, &doc); err != nil {
		return
	}
	title := wsReplacer.Replace(strings.TrimSpace(doc.Title))
	return doc.Ident, title, nil
}

// KeyTitleNormalized applies further normalization.
func KeyTitleNormalized(p []byte) (ident string, key string, err error) {
	ident, key, err = KeyTitle(p)
	if err != nil {
		return
	}
	key = strings.ToLower(key)
	key = repeatedWs.ReplaceAllString(key, " ")
	key = nonWord.ReplaceAllString(key, "")
	return ident, key, err
}

// KeyTitleNysiis returns the New York State Identification and Intelligence
// System phonetic code for the title.
func KeyTitleNysiis(p []byte) (ident string, key string, err error) {
	ident, key, err = KeyTitle(p)
	if err != nil {
		return
	}
	return ident, skate.NYSIIS(key), nil
}

func sandcrawlerSlugify(s string) string {
	slug := strings.ToLower(strings.TrimSpace(s))
	for _, prefix := range SandcrawlerPrefixRemove {
		if strings.HasPrefix(slug, prefix) {
			slug = slug[:len(prefix)]
		}
	}
	slug = strings.ReplaceAll("&apos;", "'")
	for k, v := range SandcrawlerCharMap {
		slug = strings.ReplaceAll(slug, k, v)
	}
	if len(slug) == 0 {
		return slug
	}
	slug = norm.NFKD.String(slug)
	slug = SandcrawlerRemoveCharRegex.ReplaceAllString(slug, "")
	return strings.ToLower(slug)
}

func KeyTitleSandcrawler(p []byte) (ident string, key string, err error) {

}
