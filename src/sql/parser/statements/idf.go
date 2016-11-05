package statements

import (
	. "../../lexer"
)

func IsIDF(token Token) bool {
	return token.TypeInfo == "IDENTIFIER"
}
