package statements

import (
	. "../../lexer"
)

type (
	Star struct {
		Star Token
	}
)

func (star Star) IsStar() bool {
	return IsStarStatement(star)
}

func IsStarStatement(star Star) bool {
	return star.Star.TypeInfo == "STAR" &&
		star.Star.Value == "*"
}
