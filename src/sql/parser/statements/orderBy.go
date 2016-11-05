package statements

type (
	OrderByStatement struct {
		Field Field
		Order Order
	}
)

func (orderBy OrderByStatement) IsOrderBy() bool {
	return IsOrderByStatement(orderBy)
}

func IsOrderByStatement(orderBy OrderByStatement) bool {
	return IsFieldStatement(orderBy.Field) &&
		IsOrderStatement(orderBy.Order)
}
