package statements

import (
	. "../../lexer"
)

type (
	Order struct {
		Token Token
	}
)

func (order Order) IsOrder() bool {
	return IsOrderStatement(order)
}

func IsOrderStatement(order Order) bool {
	return order.Token.TypeInfo == "IDENTIFIER" &&
		(order.Token.Value == "ASC" || order.Token.Value == "DESC")
}
