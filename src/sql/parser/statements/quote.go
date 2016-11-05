package statements

import (
	. "../../lexer"
)

func IsQuote(token Token) bool {
	return token.Value == "'"
}

func IsDuoQuote(token Token) bool {
	return token.Value == "\""
}
