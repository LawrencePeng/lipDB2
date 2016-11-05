package statements

import (
	. "../../lexer"
)

// Table := IDF

type (
	Table struct {
		Idf Token
	}
)

func (table Table) IsTable() bool {
	return IsTableStatement(table)
}

func IsTableStatement(table Table) bool {
	return IsIDF(table.Idf)
}
