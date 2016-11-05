package statements

// From:= FROM Table

type (
	From struct {
		Table Table
	}
)

func (fromStatment From) IsFrom() bool {
	return IsFromStatement(fromStatment)
}

func IsFromStatement(fromStatement From) bool {
	return IsTableStatement(fromStatement.Table)
}
