package skate

import (
	"bufio"
	"io"
)

// Zipper allows to take two streams, extract a key from them and group items
// from both streams into a single bag for further processing.
func Zipper(r, s io.Reader, keyFunc func(string) (string, error),
	groupFunc func(*GroupedCluster) error, w io.Writer) error {
	var (
		ra                   = bufio.NewReader(r)
		rb                   = bufio.NewReader(s)
		line, ka, kb, ca, cb string // line, key: ka, kb; current line: ca, cb
		done                 bool
		err                  error
	)
	for {
		if done {
			break
		}
		switch {
		case ka == "":
			for ka == "" {
				line, err = ra.ReadString('\n')
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return err
				}
				ka, err = keyFunc(line)
				if err != nil {
					return err
				}
				ca = line
			}
		case kb == "":
			for kb == "" {
				line, err = rb.ReadString('\n')
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return err
				}
				kb, err = keyFunc(line)
				if err != nil {
					return err
				}
				cb = line
			}
		case ka < kb:
			for ka < kb {
				line, err = ra.ReadString('\n')
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return err
				}
				ka, err = keyFunc(line)
				if err != nil {
					return err
				}
				ca = line
			}
		case ka > kb:
			for ka > kb {
				line, err = rb.ReadString('\n')
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return err
				}
				kb, err = keyFunc(line)
				if err != nil {
					return err
				}
				cb = line
			}
		case ka == kb:
			bag := &GroupedCluster{
				A: []string{ca},
				B: []string{cb},
			}
			for {
				line, err = ra.ReadString('\n')
				if err == io.EOF {
					done = true
					break
				}
				if err != nil {
					return err
				}
				ca = line
				k, err := keyFunc(line)
				if err != nil {
					return err
				}
				if k == ka {
					bag.A = append(bag.A, line)
					ka = k
				} else {
					ka = k
					break
				}
			}
			for {
				line, err = rb.ReadString('\n')
				if err == io.EOF {
					done = true
					break
				}
				if err != nil {
					return err
				}
				cb = line
				k, err := keyFunc(line)
				if err != nil {
					return err
				}
				if k == kb {
					bag.B = append(bag.B, line)
					kb = k
				} else {
					kb = k
					break
				}
			}
			if err := groupFunc(bag); err != nil {
				return err
			}
		}
	}
	return nil
}
