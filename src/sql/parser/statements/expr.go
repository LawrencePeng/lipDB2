package statements

import (
	. "../../lexer"
)

// ( Value | IDF | Expr) Operator (Value | IDF | Expr)

type (
	Expr struct {
		LeftOperand  interface{}

		RightOperand interface{}

		Operator     Operator
	}
)

func (expr Expr) IsExpression() bool {
	return IsExpr(expr)
}

func IsExpr(expr Expr) bool {
	lo := expr.LeftOperand
	ro := expr.RightOperand

	leftValid := IsNumberStatement(lo.(Number)) ||
		IsIDF(lo.(Token)) ||
		IsExpr(lo.(Expr))
	rightValid := IsNumberStatement(ro.(Number)) ||
		IsIDF(ro.(Token)) ||
		IsExpr(ro.(Expr))

	return leftValid && rightValid && IsOperatorStatement(expr.Operator)
}
