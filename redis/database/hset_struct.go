package database

type HashTable struct {
	m map[string]string
}

func NewHashTable() *HashTable {
	return &HashTable{
		m: make(map[string]string),
	}
}

func (ht *HashTable) Len() int {
	return len(ht.m)
}

func (ht *HashTable) Set(key, value string) bool {
	_, ok := ht.m[key]
	ht.m[key] = value
	return !ok
}

func (ht *HashTable) Get(key string) string {
	v, ok := ht.m[key]
	if !ok {
		return ""
	}
	return v
}

func (ht *HashTable) Keys() []string {
	if ht.Len() == 0 {
		return nil
	}
	keys := make([]string, 0, ht.Len())
	for k := range ht.m {
		keys = append(keys, k)
	}
	return keys
}

func (ht *HashTable) Values() []string {
	if ht.Len() == 0 {
		return nil
	}
	values := make([]string, 0, ht.Len())
	for _, v := range ht.m {
		values = append(values, v)
	}
	return values
}

func (ht *HashTable) ALL() []string {
	if ht.Len() == 0 {
		return nil
	}
	items := make([]string, 0, ht.Len()*2)
	for k, v := range ht.m {
		items = append(items, k)
		items = append(items, v)
	}
	return items
}

func (ht *HashTable) Exist(key string) bool {
	_, ok := ht.m[key]
	return ok
}

func (ht *HashTable) Remove(key string) bool {
	_, ok := ht.m[key]
	delete(ht.m, key)
	return ok
}
