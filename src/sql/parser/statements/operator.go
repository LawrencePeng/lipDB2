package statements

import (
	. "../../lexer"
)

// (+| -| *| /| %| =| ==| !=| &&|| \|\| | !| <<| >>)

type (
	Operator struct {
		Token Token
	}
)

func (op Operator) IsOperator() bool {
	return IsOperatorStatement(op)
}

func IsOperatorStatement(op Operator) bool {
	return op.Token.Value == "+" ||
		op.Token.Value == "-" ||
		op.Token.Value == "*" ||
		op.Token.Value == "/" ||
		op.Token.Value == "%" ||
		op.Token.Value == "=" ||
		op.Token.Value == "==" ||
		op.Token.Value == "!=" ||
		op.Token.Value == "&&" ||
		op.Token.Value == "||" ||
		op.Token.Value == "!" ||
		op.Token.Value == "<<" ||
		op.Token.Value == ">>"
}
