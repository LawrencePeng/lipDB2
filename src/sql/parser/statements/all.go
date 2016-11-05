package statements

import "../../lexer"

type (
	All struct {
		All lexer.Token
	}
)

func (allStatement All) IsAll() bool {
	return IsAllStatement(allStatement)
}

func IsAllStatement(allStatement All) bool {
	return allStatement.All.TypeInfo == "ALL" &&
		allStatement.All.Value == "ALL"
}
