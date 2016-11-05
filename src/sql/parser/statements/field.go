package statements

import (
	. "../../lexer"
)

type (
	Field struct {
		Token Token
	}
)

func (f Field) IsField() bool {
	return IsIDF(f.Token)
}

func IsFieldStatement(f Field) bool {
	return IsIDF(f.Token)
}
