package skate

import (
	"fmt"
	"io"
	"strings"
)

// ZipVerifyRefs takes a release and refs reader (tsv, with ident, key, doc)
// and will execute gf for each group found.
func ZipVerifyRefs(releases, refs io.Reader, w io.Writer) error {
	// Define a grouper, as closure so we can use the writer.
	grouper := func(g *Group) error {
		if len(g.A) == 0 || len(g.B) == 0 {
			return nil
		}
		pivot, err := lineToRelease(g.A[0])
		if err != nil {
			return err
		}
		for _, line := range g.B {
			re, err := lineToRelease(line)
			if err != nil {
				return err
			}
			result := Verify(pivot, re, 5)
			if _, err := fmt.Fprintf(w, "%s %s %s %s\n",
				pivot.Ident, re.Ident, result.Status, result.Reason); err != nil {
				return err
			}
			// XXX: we need to assemble the final document here (as we can
			// access both release docs here)
		}
		return nil
	}
	return Zipper(releases, refs, getKey, grouper)
}

func getKey(line string) (string, error) {
	parts := strings.Split(strings.TrimSpace(line), "\t")
	if len(parts) == 3 {
		return parts[1], nil
	}
	return "", fmt.Errorf("unexpected input: %s", line)
}
func lineToRelease(line string) (*Release, error) {
	parts := strings.Split(strings.TrimSpace(line), "\t")
	if len(parts) == 3 {
		var re *Release
		if err := json.Unmarshal([]byte(parts[2]), &re); err != nil {
			return nil, err
		}
		return re, nil
	}
	return nil, fmt.Errorf("unexpected input: %s", line)
}
