package skate

import (
	"fmt"
	"io"
	"strings"
)

// ZipVerifyRefs takes a release and refs reader (tsv, with ident, key, doc)
// and will execute gf for each group found.
func ZipVerifyRefs(releases, refs io.Reader, w io.Writer) error {
	// Define a grouper, working on one set of refs and releases with the same
	// key at a time. Here, we do verification and write out the generated
	// biblioref.
	enc := json.NewEncoder(w)
	grouper := func(g *Group) error {
		if len(g.A) == 0 || len(g.B) == 0 {
			return nil
		}
		pivot, err := lineColumnToRelease(g.A[0], "\t", 3)

		if err != nil {
			return err
		}
		for _, line := range g.B {
			re, err := lineColumnToRelease(line, "\t", 3)
			if err != nil {
				return err
			}
			result := Verify(pivot, re, 5)
			if _, err := fmt.Fprintf(w, "%s %s %s %s\n",
				pivot.Ident, re.Ident, result.Status, result.Reason); err != nil {
				return err
			}
			br := generateBiblioRef(re, pivot, result.Status, result.Reason)
			if err := enc.Encode(br); err != nil {
				return err
			}
			if _, err := io.WriteString(w, "\n"); err != nil {
				return err
			}
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

// lineColumn returns a specific column (1-indexed, like cut) from a tabular
// file, returns empty string if column is invalid.
func lineColumn(line, sep string, column int) string {
	parts := strings.Split(strings.TrimSpace(line), sep)
	if len(parts) < column {
		return ""
	}
	return parts[column-1]
}

func lineColumnToRelease(line, sep string, column int) (*Release, error) {
	var re *Release
	if err := json.Unmarshal([]byte(lineColumn(line, sep, 3)), &re); err != nil {
		return nil, err
	}
	return re, nil
}
