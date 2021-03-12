package zipkey

import (
	"bufio"
	"io"
)

// Group groups items by key and will contain the complete records (e.g. line)
// for further processing.
type Group struct {
	Key string
	G0  []string
	G1  []string
}

type (
	keyFunc   func(string) (string, error)
	groupFunc func(*Group) error
)

// ZipRun reads records (separated by sep) from two readers, extracts a key
// from each record with a keyFunc and collects records from the two streams
// into a Group. A callback can be registered, which allows to customize the
// processing of the group.
type ZipRun struct {
	r0, r1 *bufio.Reader
	kf     keyFunc
	gf     groupFunc
	sep    byte
}

// New create a new ready to run ZipRun value.
func New(r0, r1 io.Reader, kf keyFunc, gf groupFunc) *ZipRun {
	return &ZipRun{
		r0:  bufio.NewReader(r0),
		r1:  bufio.NewReader(r1),
		kf:  kf,
		gf:  gf,
		sep: '\n',
	}
}

// Run starts reading from both readers. The process stops, if one reader is
// exhausted or reads from any reader fail.
func (c *ZipRun) Run() error {
	var (
		k0, k1, c0, c1 string // key: k0, k1; current line: c0, c1
		done           bool
		err            error
		lineKey        = func(r *bufio.Reader) (line, key string, err error) {
			if line, err = r.ReadString(c.sep); err != nil {
				return
			}
			key, err = c.kf(line)
			return
		}
	)
	for {
		if done {
			break
		}
		switch {
		case k0 == "" || k0 < k1:
			for k0 == "" || k0 < k1 {
				c0, k0, err = lineKey(c.r0)
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return err
				}
			}
		case k1 == "" || k0 > k1:
			for k1 == "" || k0 > k1 {
				c1, k1, err = lineKey(c.r1)
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return err
				}
			}
		case k0 == k1:
			g := &Group{
				G0: []string{c0},
				G1: []string{c1},
			}
			for {
				c0, err = c.r0.ReadString(c.sep)
				if err == io.EOF {
					done = true
					break
				}
				if err != nil {
					return err
				}
				k, err := c.kf(c0)
				if err != nil {
					return err
				}
				if k == k0 {
					g.G0 = append(g.G0, c0)
					k0 = k
				} else {
					k0 = k
					break
				}
			}
			for {
				c1, err = c.r1.ReadString(c.sep)
				if err == io.EOF {
					done = true
					break
				}
				if err != nil {
					return err
				}
				k, err := c.kf(c1)
				if err != nil {
					return err
				}
				if k == k1 {
					g.G1 = append(g.G1, c1)
					k1 = k
				} else {
					k1 = k
					break
				}
			}
			if c.gf != nil {
				if err := c.gf(g); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
