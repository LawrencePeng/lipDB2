package statements

import (
	. "../../lexer"
)

type (
	String struct {
		LeftQuote  Token
		Literal    Token
		RightQuote Token
	}
)

func (str String) IsString() bool {
	return IsStringStatement(str)
}

func IsStringStatement(str String) bool {
	quoteValid := IsQuote(str.LeftQuote) && IsQuote(str.RightQuote)

	if !quoteValid {
		quoteValid = IsDuoQuote(str.LeftQuote) && IsDuoQuote(str.RightQuote)
	}

	return quoteValid && IsIDF(str.Literal)
}
