package statements

import (
	. "../../lexer"
)

type (
	Number struct {
		Value Token
	}
)

func (num Number) IsNumber() bool {
	return IsNumberStatement(num)
}

func IsNumberStatement(num Number) bool {
	return num.Value.TypeInfo == "INT" || num.Value.TypeInfo == "DOUBLE"
}
