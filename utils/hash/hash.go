package hash

const (
	prime = uint32(16777619)
)

func Fnv(key string) uint32 {
	hash := uint32(2166136261)
	for _, v := range key {
		hash *= prime
		hash ^= uint32(v)
	}
	return uint32(hash)
}