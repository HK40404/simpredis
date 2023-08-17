package database

type Set struct {
	s map[string]struct{}
}

func NewSet() *Set {
	return &Set{
		s: make(map[string]struct{}),
	}
}

func (s *Set) Add(member string) {
	s.s[member] = struct{}{}
}

func (s *Set) Len() int {
	return len(s.s)
}

func (s *Set) Members() []string {
	members := make([]string, 0, s.Len())
	for v := range s.s {
		members = append(members, v)
	}
	return members
}

func (s *Set) Remove(member string) bool {
	_, ok := s.s[member]
	delete(s.s, member)
	return ok
}

func (s *Set) IsMember(member string) bool {
	_, ok := s.s[member]
	return ok
}

func InterSets(sets []*Set) []string {
	if len(sets) <= 0 {
		return nil
	}

	members := make([]string, 0, len(sets[0].s))
	if len(sets) == 1 {
		for m := range sets[0].s {
			members = append(members, m)
		}
		return members
	}

	for m := range sets[0].s {
		interFlag := true
		for i := 1; i < len(sets); i++ {
			if _, ok := sets[i].s[m]; !ok {
				interFlag = false
				break
			}
		}
		if interFlag {
			members = append(members, m)
		}
	}
	return members
}

func (s *Set)Pop() string {
	for m := range s.s {
		delete(s.s, m)
		return m
	}
	return ""
}