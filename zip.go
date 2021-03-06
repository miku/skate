package skate

import (
	"bufio"
	"io"
)

// Zipper allows to take two streams, extract a key from them and group items
// from both streams into a single bag for further processing.
func Zipper(r, s io.Reader, keyFunc func(string) (string, error),
	groupFunc func(*GroupedCluster) error) error {
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
		case ka == "":
			for ka == "" {
				ca, err = ra.ReadString('\n')
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return err
				}
				if ka, err = keyFunc(ca); err != nil {
					return err
				}
			}
		case kb == "":
			for kb == "" {
				cb, err = rb.ReadString('\n')
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return err
				}
				if kb, err = keyFunc(cb); err != nil {
					return err
				}
			}
		case ka < kb:
			for ka < kb {
				ca, err = ra.ReadString('\n')
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return err
				}
				if ka, err = keyFunc(ca); err != nil {
					return err
				}
			}
		case ka > kb:
			for ka > kb {
				cb, err = rb.ReadString('\n')
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return err
				}
				if kb, err = keyFunc(cb); err != nil {
					return err
				}
			}
		case ka == kb:
			bag := &GroupedCluster{
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
					bag.A = append(bag.A, ca)
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
					bag.B = append(bag.B, cb)
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
