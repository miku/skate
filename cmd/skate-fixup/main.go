// skate-fixup applies various schema fixes on release entities, e.g.
// normalizing years and subtitles.
package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	jsoniter "github.com/json-iterator/go"
	"github.com/miku/skate"
	"github.com/miku/skate/parallel"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func main() {
	pp := parallel.NewProcessor(os.Stdin, os.Stdout, func(p []byte) ([]byte, error) {
		var fixup ClusterFixup
		if err := json.Unmarshal(p, &fixup); err != nil {
			return nil, err
		}
		if err := fixup.Fixup(); err != nil {
			return nil, err
		}
		b, err := json.Marshal(fixup.Cluster)
		if err != nil {
			return nil, err
		}
		b = append(b, []byte("\n")...)
		return b, nil
	})
	if err := pp.Run(); err != nil {
		log.Fatal(err)
	}
}

// Cluster document.
type Cluster struct {
	Key    string           `json:"k"`
	Values []*skate.Release `json:"v"`
}

// ClusterFixup.
type ClusterFixup struct {
	Cluster
	Values []*ReleaseFixup `json:"v"`
}

func (c *ClusterFixup) Fixup() error {
	for _, rf := range c.Values {
		if err := rf.Fixup(); err != nil {
			return err
		}
		c.Cluster.Values = append(c.Cluster.Values, &rf.Release)
	}
	return nil
}

type ReleaseFixup struct {
	skate.Release
	ReleaseYear interface{} `json:"release_year,omitempty"` // might be int or str
	Extra       struct {
		Subtitle interface{} `json:"subtitle,omitempty"` // [] or str
	} `json:"extra,omitempty"`
}

// Fixup applies data fixes.
func (r *ReleaseFixup) Fixup() error {
	switch t := r.ReleaseYear.(type) {
	case int:
		r.Release.ReleaseYear = t
	case float64:
		r.Release.ReleaseYear = int(t)
	case nil:
		// do nothing
	case string:
		v, err := strconv.Atoi(t)
		if err != nil {
			return err
		}
		r.Release.ReleaseYear = v
	default:
		return fmt.Errorf("no fixup available for release year %T", t)
	}
	switch t := r.Extra.Subtitle.(type) {
	case string:
		r.Release.Extra.Subtitle = []string{t}
	case []interface{}:
		var ss []string
		for _, v := range t {
			ss = append(ss, fmt.Sprintf("%v", v))
		}
		r.Release.Extra.Subtitle = ss
	case []string:
		r.Release.Extra.Subtitle = t
	case nil:
		// do nothing
	default:
		return fmt.Errorf("no fixup available for extra.subtitle %T", t)
	}
	return nil
}
