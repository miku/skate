package skate

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

// ZipUnverified takes a release and refs reader (tsv, with ident, key, doc)
// and assigns a fixed match result.
func ZipUnverified(releases, refs io.Reader, mr MatchResult, provenance string, w io.Writer) error {
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
			br := generateBiblioRef(re, pivot, mr.Status, mr.Reason, provenance)
			if err := enc.Encode(br); err != nil {
				return err
			}
		}
		return nil
	}
	return Zipper(releases, refs, getKey, grouper)
}

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
			switch result.Status {
			case StatusExact, StatusStrong:
				if result.Reason == ReasonDOI {
					continue
				}
				br := generateBiblioRef(re, pivot, result.Status, result.Reason)
				if err := enc.Encode(br); err != nil {
					return err
				}
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
	if err := json.Unmarshal([]byte(lineColumn(line, sep, column)), &re); err != nil {
		return nil, err
	}
	return re, nil
}

// Zipper allows to take two streams (with newline delimited records), extract
// a key from them and group items from both streams into a single struct for
// further processing. The key extractor must currently be the same for both
// streams.
func Zipper(r, s io.Reader,
	keyFunc func(string) (string, error),
	groupFunc func(*Group) error) error {
	var (
		ra             = bufio.NewReader(r)
		rb             = bufio.NewReader(s)
		ka, kb, ca, cb string // key: ka, kb; current line: ca, cb
		done           bool
		err            error
	)
	for {
		if done {
			break
		}
		switch {
		case ka == "" || ka < kb:
			for ka == "" || ka < kb {
				ca, ka, err = keyLine(ra, keyFunc)
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return err
				}
			}
		case kb == "" || ka > kb:
			for kb == "" || ka > kb {
				cb, kb, err = keyLine(rb, keyFunc)
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return err
				}
			}
		case ka == kb:
			g := &Group{
				A: []string{ca},
				B: []string{cb},
			}
			for {
				ca, err = ra.ReadString('\n')
				if err == io.EOF {
					done = true
					break
				}
				if err != nil {
					return err
				}
				k, err := keyFunc(ca)
				if err != nil {
					return err
				}
				if k == ka {
					g.A = append(g.A, ca)
					ka = k
				} else {
					ka = k
					break
				}
			}
			for {
				cb, err = rb.ReadString('\n')
				if err == io.EOF {
					done = true
					break
				}
				if err != nil {
					return err
				}
				k, err := keyFunc(cb)
				if err != nil {
					return err
				}
				if k == kb {
					g.B = append(g.B, cb)
					kb = k
				} else {
					kb = k
					break
				}
			}
			if err := groupFunc(g); err != nil {
				return err
			}
		}
	}
	return nil
}

// keyLine, auxiliary function to return line, key and error in one call.
func keyLine(r *bufio.Reader, f func(string) (string, error)) (line, key string, err error) {
	if line, err = r.ReadString('\n'); err != nil {
		return "", "", err
	}
	if key, err = f(line); err != nil {
		return "", "", err
	} else {
		return line, key, nil
	}
}
