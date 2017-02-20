package statements

type UpdateStatement struct {
	TableName string
	Col       string
	Value     interface{}
	Where     *Where

	Appliable
}
