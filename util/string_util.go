// Package util
package util

import "strings"

// StrToLowerBytes ...
func StrToLowerBytes(s string) []byte {
	return []byte(strings.ToLower(s))
}
