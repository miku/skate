package set

import "sort"

// Set implements basic string set operations.
type Set map[string]struct{}

// Add adds an element.
func (s *Set) Add(v string) {
	(*s)[v] = struct{}{}
}

// Len returns number of elements in set.
func (s *Set) Len() int {
	return len(*s)
}

// IsEmpty returns if set has zero elements.
func (s *Set) IsEmpty() bool {
	return s.Len() == 0
}

// Equals returns true, if sets contain the same elements.
func (s *Set) Equals(t *Set) bool {
	for k := range *s {
		if !t.Contains(k) {
			return false
		}
	}
	return s.Len() == t.Len()
}

// Contains returns membership status.
func (s *Set) Contains(v string) bool {
	_, ok := (*s)[v]
	return ok
}

// Intersection returns a new set containing all elements found in both sets.
func (s *Set) Intersection(t *Set) *Set {
	u := New()
	for _, v := range s.Slice() {
		if t.Contains(v) {
			u.Add(v)
		}
	}
	return u
}

// Union returns the union of two sets.
func (s *Set) Union(t *Set) *Set {
	u := New()
	for _, v := range s.Slice() {
		u.Add(v)
	}
	for _, v := range t.Slice() {
		u.Add(v)
	}
	return u
}

// Slice returns all elements as a slice.
func (s *Set) Slice() (result []string) {
	for k := range *s {
		result = append(result, k)
	}
	return
}

// SortedSlice returns all elements as a slice, sorted.
func (s *Set) SortedSlice() (result []string) {
	for k := range *s {
		result = append(result, k)
	}
	sort.Strings(result)
	return
}

// TopK returns at most k elements.
func (s *Set) TopK(k int) *Set {
	var top []string
	for i, v := range s.SortedSlice() {
		if i < k {
			top = append(top, v)
		}
	}
	return FromSlice(top)
}

func (s *Set) Product(t *Set) (result [][]string) {
	for k := range *s {
		for l := range *t {
			result = append(result, []string{k, l})
		}
	}
	return
}

// Jaccard returns the jaccard index of sets s and t.
func (s *Set) Jaccard(t *Set) float64 {
	if s.IsEmpty() && t.IsEmpty() {
		return 1
	}
	if u := s.Union(t); u.IsEmpty() {
		return 0
	} else {
		return float64(s.Intersection(t).Len()) / float64(u.Len())
	}
}

// New creates a new set.
func New() *Set {
	s := make(Set)
	return &s
}

// FromSlice initializes a set from a slice.
func FromSlice(vs []string) *Set {
	s := New()
	for _, v := range vs {
		s.Add(v)
	}
	return s
}
