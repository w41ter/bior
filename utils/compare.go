package utils

// MinUint64 return minimum between a & b.
func MinUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

// MaxUint64 return maximum between a & b.
func MaxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
