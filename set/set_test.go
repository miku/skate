package set

import (
	"testing"

	"github.com/matryer/is"
)

func TestSet(t *testing.T) {
	is := is.New(t)

	s := make(Set)
	is.Equal(s.Len(), 0)
	is.True(s.IsEmpty())

	s.Add("1")
	is.Equal(s.Len(), 1)
	is.True(!s.IsEmpty())
	is.True(s.Contains("1"))
	is.True(!s.Contains("2"))
	is.Equal(s.Slice(), []string{"1"})

	r := make(Set)
	r.Add("2")
	is.True(s.Intersection(&r).IsEmpty())
	is.Equal(s.Union(&r).Len(), 2)
	is.Equal(s.Union(&r).SortedSlice(), []string{"1", "2"})

	r.Add("3")
	r.Add("4")
	r.Add("5")
	r.Add("6")
	r.Add("7")
	r.Add("8")
	top := make(Set)
	top.Add("2")
	top.Add("3")
	is.Equal(r.TopK(2), &top)
}
