package statements

// Where:= WHERE Expr

type (
	Where struct {
		Expr Expr
	}
)

func (where Where) IsWhere() bool {
	return IsWhereStatement(where)
}

func IsWhereStatement(where Where) bool {
	return IsExpr(where.Expr)
}
