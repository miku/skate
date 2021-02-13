package set

type Set map[string]struct{}

func (s *Set) Add(v string) {
	(*s)[v] = struct{}{}
}

func (s *Set) Contains(v string) bool {
	_, ok := (*s)[v]
	return ok
}

func New() *Set {
	s := make(Set)
	return &s
}

func FromSlice(vs []string) *Set {
	s := New()
	for _, v := range vs {
		s.Add(v)
	}
	return s
}
