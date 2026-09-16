package nginxtime

import (
	"strconv"
	"strings"
)

var units = map[byte]int64{'s': 1, 'm': 60, 'h': 3600, 'd': 86400}

func ParseSeconds(spec string) int64 {
	raw := strings.TrimSpace(strings.ToLower(spec))
	if raw == "" {
		return 0
	}

	mult := int64(1)

	if u, ok := units[raw[len(raw)-1]]; ok {
		mult = u
		raw = raw[:len(raw)-1]
	}

	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n <= 0 {
		return 0
	}

	return n * mult
}
