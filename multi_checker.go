package main

var workUnit = 950

// Takes pre-sanitized id strings and groups them into a map keyed by the
// number of digits they contain.
func groupByDigitLength(ids []string) (groups map[int][]string) {
	groups = make(map[int][]string)
	for i, _ := range ids {
		l := len(ids[i])
		if _, ok := groups[l]; ok {
			groups[l] = append(groups[l], ids[i])
		} else {
			groups[l] = []string{ids[i]}
		}
	}
	return
}

// Breaks an input slice of ids into a slice of smaller work-unit-size slices.
func split(ids []string) (workBits [][]string) {
	llen := len(ids)
	if llen < workUnit {
		return [][]string{ids}
	}
	start := 0
	end := workUnit
	for {
		if start >= llen {
			break
		}
		workBits = append(workBits, ids[start:end])
		start = end
		end = lesserOf(llen, end+workUnit)
	}
	return
}

func lesserOf(a, b int) int {
	if a < b {
		return a
	}
	return b
}
