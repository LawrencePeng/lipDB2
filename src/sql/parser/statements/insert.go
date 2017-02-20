package statements

type InsertStatement struct {
	TableName string
	Values    []interface{}

	Appliable
}
