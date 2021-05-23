package symbols

import (
	"html"
	"strconv"
)

// Unicode hex in decimal
var symbols map[string]int = map[string]int{
	"checkmark": 10004,
	"crossmark": 10008,
}

func CastUnicodeToSymbol(hex int) string {
	// https://play.golang.org/p/Xr7ULppG7hq
	return html.UnescapeString("&#" + strconv.Itoa(hex) + ";")
}

func GetSymbol(name string) string {
	if val, ok := symbols[name]; ok {
		return CastUnicodeToSymbol(val)
	}
	return ""
}

func GetStatusSymbol(exitCode int) string {
	if exitCode == 0 {
		return GetSymbol("checkmark")
	}
	return GetSymbol("crossmark")
}
