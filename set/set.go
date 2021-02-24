package set

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
	for _, v := range *s {
		if !t.Contains(v) {
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

// Slice returns all elements as a slice.
func (s *Set) Slice() (result []string) {
	for k := range *s {
		result = append(result, k)
	}
	return
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
