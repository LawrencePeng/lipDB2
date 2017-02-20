package statements

import (
	_ "../../lexer"
)

// ( Value | IDF | Expr) Operator (Value | IDF | Expr)

type (
	Expr struct {
		Conditions []Condition
		InterOP    []LogicOperation
	}
)

func (expr Expr) IsExpression() bool {
	return IsExpr(expr)
}

func IsExpr(expr Expr) bool {
	for _, value := range expr.Conditions {
		if !IsCondition(value) {
			return false
		}
	}

	return len(expr.InterOP)+1 == len(expr.Conditions)
}
