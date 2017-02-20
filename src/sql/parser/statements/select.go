package statements

// Select:= SELECT (UNIQUE) (*| ALL| Fields) From Where OrderBy GroupBy Limit

type (
	SelectStatement struct {
		Unique Unique

		All All

		Star Star

		Fields Fields

		From From

		Where Where

		OrderBy OrderByStatement

		// TODO having HavingStatement

		// TODO groupBy GroupByStatement

		Appliable
	}
)

func (sel SelectStatement) IsSelect() bool {
	return IsSelectStatement(sel)
}

func IsSelectStatement(sel SelectStatement) bool {
	return IsUniqueStatement(sel.Unique) &&
		IsAllStatement(sel.All) &&
		IsFieldsStatement(sel.Fields) &&
		IsFromStatement(sel.From) &&
		IsWhereStatement(sel.Where) &&
		IsOrderByStatement(sel.OrderBy)
}
