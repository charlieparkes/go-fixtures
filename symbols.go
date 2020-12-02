package fixtures

import (
	"html"
	"strconv"
)

// Unicode hex in decimal
var symbols map[string]int = map[string]int{
	"checkmark": 10004,
	"crossmark": 10008,
}

func castUnicodeToSymbol(hex int) string {
	// https://play.golang.org/p/Xr7ULppG7hq
	return html.UnescapeString("&#" + strconv.Itoa(hex) + ";")
}

func getSymbol(name string) string {
	if val, ok := symbols[name]; ok {
		return castUnicodeToSymbol(val)
	}
	return ""
}

func getStatusSymbol(exitCode int) string {
	if exitCode == 0 {
		return getSymbol("checkmark")
	}
	return getSymbol("crossmark")
}
