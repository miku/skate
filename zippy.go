package skate

import (
	"fmt"
	"io"
	"strings"

	"github.com/miku/skate/zipkey"
)

// ZipUnverified takes a release and refs reader (tsv, with ident, key, doc)
// and assigns a fixed match result.
func ZipUnverified(releases, refs io.Reader, mr MatchResult, provenance string, w io.Writer) error {
	// Define a grouper, working on one set of refs and releases with the same
	// key at a time. Here, we do verification and write out the generated
	// biblioref.
	enc := json.NewEncoder(w)
	keyer := func(s string) (string, error) {
		if k := lineColumn(s, "\t", 2); k == "" {
			return k, fmt.Errorf("cannot get key: %s", s)
		} else {
			return k, nil
		}
	}
	grouper := func(g *zipkey.Group) error {
		if len(g.G0) == 0 || len(g.G1) == 0 {
			return nil
		}
		target, err := stringToRelease(lineColumn(g.G0[0], "\t", 3))
		if err != nil {
			return err
		}
		for _, line := range g.G1 {
			ref, err := stringToRef(lineColumn(line, "\t", 3))
			if err != nil {
				return err
			}
			var bref BiblioRef
			bref.SourceReleaseIdent = ref.ReleaseIdent
			bref.SourceWorkIdent = ref.WorkIdent
			bref.SourceReleaseStage = ref.ReleaseStage
			bref.SourceYear = fmt.Sprintf("%d", ref.ReleaseYear)
			bref.RefIndex = ref.Index + 1 // we want 1-index (also helps with omitempty)
			bref.RefKey = ref.Key
			bref.TargetReleaseIdent = target.Ident
			bref.TargetWorkIdent = target.WorkID
			bref.MatchProvenance = provenance
			bref.MatchStatus = mr.Status.Short()
			bref.MatchReason = mr.Reason.Short()
			if err := enc.Encode(bref); err != nil {
				return err
			}
		}
		return nil
	}
	zipper := zipkey.New(releases, refs, keyer, grouper)
	return zipper.Run()
}

// ZipVerifyRefs takes a release and refs reader (tsv, with ident, key, doc)
// and will execute gf for each group found.
func ZipVerifyRefs(releases, refs io.Reader, w io.Writer) error {
	// Define a grouper, working on one set of refs and releases with the same
	// key at a time. Here, we do verification and write out the generated
	// biblioref.
	enc := json.NewEncoder(w)
	keyer := func(s string) (string, error) {
		if k := lineColumn(s, "\t", 2); k == "" {
			return k, fmt.Errorf("cannot get key: %s", s)
		} else {
			return k, nil
		}
	}
	grouper := func(g *zipkey.Group) error {
		if len(g.G0) == 0 || len(g.G1) == 0 {
			return nil
		}
		pivot, err := stringToRelease(lineColumn(g.G0[0], "\t", 3))
		if err != nil {
			return err
		}
		for _, line := range g.G1 {
			re, err := stringToRelease(lineColumn(line, "\t", 3))
			if err != nil {
				return err
			}
			result := Verify(pivot, re, 5)
			switch result.Status {
			case StatusExact, StatusStrong:
				if result.Reason == ReasonDOI {
					continue
				}
				br := generateBiblioRef(re, pivot, result.Status, result.Reason, "fuzzy")
				if err := enc.Encode(br); err != nil {
					return err
				}
			}
		}
		return nil
	}
	zipper := zipkey.New(releases, refs, keyer, grouper)
	return zipper.Run()
}

// lineColumn returns a specific column (1-indexed, like cut) from a tabular
// file, returns empty string if column is invalid.
func lineColumn(line, sep string, column int) string {
	var parts = strings.Split(strings.TrimSpace(line), sep)
	if len(parts) < column {
		return ""
	} else {
		return parts[column-1]
	}
}

func stringToRelease(s string) (r *Release, err error) {
	err = json.Unmarshal([]byte(s), &r)
	return
}

func stringToRef(s string) (r *Ref, err error) {
	err = json.Unmarshal([]byte(s), &r)
	return
}
