package commands

// globMatch reports whether name matches a Redis-style glob pattern. It supports
// '*' (any, possibly empty, sequence), '?' (any single byte), '[...]' character
// classes with '^' negation and 'a-z' ranges, and '\' escaping of the next
// character. Matching is byte-wise so it works for binary-safe keys.
func globMatch(pattern, name string) bool {
	return globMatchBytes([]byte(pattern), []byte(name))
}

func globMatchBytes(p, s []byte) bool {
	for len(p) > 0 {
		switch p[0] {
		case '*':
			// Collapse consecutive stars; a trailing '*' matches the rest.
			for len(p) > 1 && p[1] == '*' {
				p = p[1:]
			}
			if len(p) == 1 {
				return true
			}
			for i := 0; i <= len(s); i++ {
				if globMatchBytes(p[1:], s[i:]) {
					return true
				}
			}
			return false

		case '?':
			if len(s) == 0 {
				return false
			}
			s, p = s[1:], p[1:]

		case '[':
			if len(s) == 0 {
				return false
			}
			p = p[1:]
			negate := false
			if len(p) > 0 && p[0] == '^' {
				negate = true
				p = p[1:]
			}
			matched := false
			for len(p) > 0 && p[0] != ']' {
				switch {
				case p[0] == '\\' && len(p) > 1:
					p = p[1:]
					if p[0] == s[0] {
						matched = true
					}
					p = p[1:]
				case len(p) >= 3 && p[1] == '-' && p[2] != ']':
					lo, hi := p[0], p[2]
					if lo > hi {
						lo, hi = hi, lo
					}
					if s[0] >= lo && s[0] <= hi {
						matched = true
					}
					p = p[3:]
				default:
					if p[0] == s[0] {
						matched = true
					}
					p = p[1:]
				}
			}
			if len(p) > 0 { // consume the closing ']'
				p = p[1:]
			}
			if matched == negate {
				return false
			}
			s = s[1:]

		case '\\':
			if len(p) > 1 {
				p = p[1:]
			}
			if len(s) == 0 || p[0] != s[0] {
				return false
			}
			s, p = s[1:], p[1:]

		default:
			if len(s) == 0 || p[0] != s[0] {
				return false
			}
			s, p = s[1:], p[1:]
		}
	}
	return len(s) == 0
}
