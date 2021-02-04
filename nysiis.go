package skate

import "strings"

type runestring []rune

// A safe way to index a runestring. It will return a null rune if you try
// to index outside of the bounds of the runestring.
func (r *runestring) SafeAt(pos int) rune {
	if pos < 0 || pos >= len(*r) {
		return 0
	} else {
		return (*r)[pos]
	}
}

// A safe way to obtain a substring of a runestring. It will return a null
// string ("") if you index somewhere outside its bounds.
func (r *runestring) SafeSubstr(pos int, length int) string {
	if pos < 0 || pos > len(*r) || (pos+length) > len(*r) {
		return ""
	} else {
		return string((*r)[pos : pos+length])
	}
}

// Delete characters at positions pos. It will do nothing if you provide
// an index outside the bounds of the runestring.
func (r *runestring) Del(pos ...int) {
	for _, i := range pos {
		if i >= 0 && i <= len(*r) {
			*r = append((*r)[:i], (*r)[i+1:]...)
		}
	}
}

// A helper to determine if any substrings exist within the given runestring.
func (r *runestring) Contains(start int, length int, criteria ...string) bool {
	substring := r.SafeSubstr(start, length)
	for _, c := range criteria {
		if substring == c {
			return true
		}
	}
	return false
}

func cleanInput(input string) string {
	return strings.ToUpper(strings.TrimSpace(input))
}

func isVowelNoY(c rune) bool {
	switch c {
	case 'A', 'E', 'I', 'O', 'U':
		return true
	default:
		return false
	}
}

// NYSIIS computes the NYSIIS phonetic encoding of the input string. It is a
// modification of the traditional Soundex algorithm.
func NYSIIS(s1 string) string {
	cleans1 := runestring(cleanInput(s1))
	input := runestring(make([]rune, 0, len(s1)))
	// The output can't be larger than the string itself
	output := runestring(make([]rune, 0, len(s1)))
	// 0. Remove all non-ASCII characters
	for _, v := range cleans1 {
		if v >= 65 && v <= 90 {
			input = append(input, v)
		}
	}
	if len(input) == 0 {
		return ""
	}
	// 1. Transcoding first characters
	switch input[0] {
	case 'M':
		if input.SafeSubstr(0, 3) == "MAC" {
			// MAC -> MCC
			input[1] = 'C'
		}
	case 'K':
		if input.SafeSubstr(0, 2) == "KN" {
			// KN -> NN
			input[0] = 'N'
		} else {
			// K -> C
			input[0] = 'C'
		}
	case 'P':
		next := input.SafeAt(1)
		if next == 'H' {
			// PH -> FF
			input[0] = 'F'
			input[1] = 'F'
		} else if next == 'F' {
			// PF -> FF
			input[0] = 'F'
		}
	case 'S':
		if input.SafeSubstr(0, 3) == "SCH" {
			input[1] = 'S'
			input[2] = 'S'
		}
	}
	// 2. Transcoding last characters
	switch input.SafeSubstr(len(input)-2, 2) {
	case "EE", "IE":
		// EE, IE -> Y
		input.Del(len(input) - 2)
		input[len(input)-1] = 'Y'
	case "DT", "RT", "RD", "NT", "ND":
		// DT, RT, RD, NT, ND -> D
		input.Del(len(input) - 2)
		input[len(input)-1] = 'D'
	}
	// 3. First character of key = first character of name
	output = append(output, input[0])
	last := input[0]
	for i := 1; i < len(input); i++ {
		c := input[i]
		switch c {
		case 'A', 'I', 'O', 'U':
			// A, E, I, O, U -> A (E is separate)
			input[i] = 'A'
		case 'E':
			// EV -> AF, else A
			if input.SafeAt(i+1) == 'V' {
				input[i+1] = 'F'
			}
			input[i] = 'A'
		case 'Q':
			// Q -> G
			input[i] = 'G'
		case 'Z':
			// Z -> S
			input[i] = 'S'
		case 'M':
			// M -> N
			input[i] = 'N'
		case 'K':
			// KN -> N, else K -> C
			if input.SafeAt(i+1) == 'N' {
				input.Del(i)
			} else {
				input[i] = 'C'
			}
		case 'S':
			// SCH -> SSS
			if input.SafeSubstr(i, 3) == "SCH" {
				input[i+1] = 'S'
				input[i+2] = 'S'
			}
		case 'P':
			// PH -> FF
			if input.SafeAt(i+1) == 'H' {
				input[i] = 'F'
				input[i+1] = 'F'
			}
		case 'H':
			// H -> $(previous character) if previous character or
			// next character is a non-vowel
			prev := input.SafeAt(i - 1)
			next := input.SafeAt(i + 1)
			if !isVowelNoY(prev) || !isVowelNoY(next) {
				input[i] = prev
			}
		case 'W':
			prev := input.SafeAt(i - 1)
			if isVowelNoY(prev) {
				input[i] = prev
			}
		}
		if input[i] != last && input[i] != 0 {
			output = append(output, input[i])
		}
		last = input[i]
	}
	// have to be careful here because we've already added the first
	// key value
	if len(output) > 1 {
		// remove trailing s
		if output.SafeAt(len(output)-1) == 'S' {
			output.Del(len(output) - 1)
		}
		// trailing AY -> Y
		if len(output) > 2 && output.SafeSubstr(len(output)-2, 2) == "AY" {
			output.Del(len(output) - 2)
		}
		// trailing A -> remove it
		if output.SafeAt(len(output)-1) == 'A' {
			output.Del(len(output) - 1)
		}
	}
	if len(output) > 6 {
		return string(output[0:6])
	} else {
		return string(output)
	}
}
