package database

func Grow(src *[]byte, offset int) {
	curLen := len(*src)
	grownedLen := offset / 8
	if grownedLen % 8 != 0 {
		grownedLen++
	}
	*src = append(*src, make([]byte, grownedLen-curLen)...)
}

func SetBit(src *[]byte, offset int, bitVal int) {
	if offset >= len(*src) * 8 {
		Grow(src, offset)
	}
	index := offset / 8
	bitIndex := offset % 8
	mask := 1 << bitIndex
	switch bitVal {
	case 1:
		(*src)[index] |= byte(mask)
	case 0:
		(*src)[index] &= byte(^mask)
	}
}

func GetBit(src *[]byte, offset int) int {
	if offset >= len(*src) * 8 {
		return 0
	}
	index := offset / 8
	bitIndex := offset % 8
	mask := 1 << bitIndex
	v := (*src)[index] & byte(mask)
	if v > 0 {
		return 1
	} 
	return 0
}

func ForEachBit(src *[]byte, start, end int, f func(offset int, bitval byte) bool) {
	endByteIndex := end / 8
	byteIndex := start / 8
	bitIndex := start % 8
	offset := start
	for byteIndex <= endByteIndex {
		B := (*src)[byteIndex]
		for bitIndex < 8 && offset <= end {
			bitVal := B >> byte(bitIndex) & 0x01
			if !f(offset, bitVal) {
				return
			}
			offset++
			bitIndex++
		}
		bitIndex = 0
		byteIndex++
	}
}

func BitCount(src *[]byte, start, end int) int {
	count := 0
	ForEachBit(src, start, end, func(offset int, bitval byte) bool {
		if bitval > 0 {
			count++
		}
		if offset == end {
			return false
		}
		return true
	})
	return count
}

// 不要改变原来的vals[i]
// 不要append vals
// 
func BitOp(op string, vals [][]byte) []byte {
	maxLen := 0
	for _, v := range vals {
		if len(v) > maxLen {
			maxLen = len(v)
		}
	}
	res := make([]byte, maxLen)
	copy(res, vals[0])
	switch op {
	case "and":
		for i := 1; i < len(vals); i++ {
			if vals[i] == nil {
				for j := 0; j < len(res); j++ {
					res[j] &= 0
				}
				continue
			}
			for j := 0; j < len(vals[i]); j++ {
				res[j] &= vals[i][j]
			}
		}
		return res
	case "or":
		for i := 1; i < len(vals); i++ {
			if vals[i] == nil {
				for j := 0; j < len(res); j++ {
					res[j] |= 0
				}
				continue
			}
			for j := 0; j < len(vals[i]); j++ {
				res[j] |= vals[i][j]
			}
		}
		return res
	case "xor":
		for i := 1; i < len(vals); i++ {
			if vals[i] == nil {
				for j := 0; j < len(res); j++ {
					res[j] ^= 0
				}
				continue
			}
			for j := 0; j < len(vals[i]); j++ {
				res[j] ^= vals[i][j]
			}
		}
		return res
	case "not":
		for j := 0; j < len(res); j++ {
			res[j] = ^res[j]
		}
		return res
	default:
		return nil
	}
}