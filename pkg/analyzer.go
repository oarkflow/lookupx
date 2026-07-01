package pkg

import (
	"strings"
	"unicode"
)

func normalize(s string, lower bool) string {
	s = strings.TrimSpace(s)
	if lower {
		return lowerNoAlloc(s)
	}
	return s
}

func lowerNoAlloc(s string) string {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			b := []byte(s)
			b[i] = c + ('a' - 'A')
			for j := i + 1; j < len(b); j++ {
				if b[j] >= 'A' && b[j] <= 'Z' {
					b[j] += 'a' - 'A'
				}
			}
			return string(b)
		}
		if c >= 128 {
			return strings.ToLower(s)
		}
	}
	return s
}

func tokenize(s string, lower bool, dst []string) []string {
	dst = dst[:0]
	if lower {
		s = lowerNoAlloc(s)
	}
	start := -1
	for i, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '-' || r == '@' || r == '.' {
			if start < 0 {
				start = i
			}
		} else if start >= 0 {
			dst = append(dst, s[start:i])
			start = -1
		}
	}
	if start >= 0 {
		dst = append(dst, s[start:])
	}
	return dst
}

func grams(s string, min, max int, dst []string) []string {
	dst = dst[:0]
	if min <= 0 {
		min = 2
	}
	if max <= 0 || max < min {
		max = min
	}
	if isASCII(s) {
		for n := min; n <= max; n++ {
			if n > len(s) {
				break
			}
			for i := 0; i+n <= len(s); i++ {
				dst = append(dst, s[i:i+n])
			}
		}
		return dst
	}
	r := []rune(s)
	for n := min; n <= max; n++ {
		if n > len(r) {
			break
		}
		for i := 0; i+n <= len(r); i++ {
			dst = append(dst, string(r[i:i+n]))
		}
	}
	return dst
}

func prefixes(s string, dst []string) []string {
	dst = dst[:0]
	if isASCII(s) {
		for i := 1; i <= len(s); i++ {
			dst = append(dst, s[:i])
		}
		return dst
	}
	r := []rune(s)
	for i := 1; i <= len(r); i++ {
		dst = append(dst, string(r[:i]))
	}
	return dst
}
func suffixes(s string, dst []string) []string {
	dst = dst[:0]
	if isASCII(s) {
		for i := 0; i < len(s); i++ {
			dst = append(dst, s[i:])
		}
		return dst
	}
	r := []rune(s)
	for i := 0; i < len(r); i++ {
		dst = append(dst, string(r[i:]))
	}
	return dst
}

func levenshtein(a, b string, max int) int {
	// ASCII fast path with stack buffers; avoids per-term allocations in fuzzy scans.
	if len(a) == 0 {
		return len(b)
	}
	if len(b) == 0 {
		return len(a)
	}
	if max >= 0 && abs(len(a)-len(b)) > max {
		return max + 1
	}
	if len(a) <= 255 && len(b) <= 255 && isASCII(a) && isASCII(b) {
		var prev, cur [256]int
		for j := 0; j <= len(b); j++ {
			prev[j] = j
		}
		for i := 1; i <= len(a); i++ {
			cur[0] = i
			rowMin := cur[0]
			lo, hi := 1, len(b)
			if max >= 0 {
				lo = i - max
				if lo < 1 {
					lo = 1
				}
				hi = i + max
				if hi > len(b) {
					hi = len(b)
				}
				if lo > 1 {
					cur[lo-1] = max + 1
				}
			}
			for j := lo; j <= hi; j++ {
				cost := 0
				if a[i-1] != b[j-1] {
					cost = 1
				}
				cur[j] = min3(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
				if cur[j] < rowMin {
					rowMin = cur[j]
				}
			}
			if max >= 0 && rowMin > max {
				return max + 1
			}
			prev, cur = cur, prev
		}
		return prev[len(b)]
	}
	ra, rb := []rune(a), []rune(b)
	if max >= 0 && abs(len(ra)-len(rb)) > max {
		return max + 1
	}
	prev := make([]int, len(rb)+1)
	cur := make([]int, len(rb)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(ra); i++ {
		cur[0] = i
		rowMin := cur[0]
		for j := 1; j <= len(rb); j++ {
			cost := 0
			if ra[i-1] != rb[j-1] {
				cost = 1
			}
			cur[j] = min3(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
			if cur[j] < rowMin {
				rowMin = cur[j]
			}
		}
		if max >= 0 && rowMin > max {
			return max + 1
		}
		prev, cur = cur, prev
	}
	return prev[len(rb)]
}
func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 128 {
			return false
		}
	}
	return true
}
func min3(a, b, c int) int {
	if a < b {
		if a < c {
			return a
		}
		return c
	}
	if b < c {
		return b
	}
	return c
}
func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
