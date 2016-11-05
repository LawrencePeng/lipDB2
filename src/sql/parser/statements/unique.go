package statements

import (
	. "../../lexer"
)

type (
	Unique struct {
		Unique Token
	}
)

func (uni Unique) IsUnique() bool {
	return IsUniqueStatement(uni)
}

func IsUniqueStatement(uni Unique) bool {
	return uni.Unique.TypeInfo == "UNIQUE" &&
		uni.Unique.Value == "UNIQUE"
}
