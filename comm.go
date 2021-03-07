package skate

import (
	"bufio"
	"io"
)

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
