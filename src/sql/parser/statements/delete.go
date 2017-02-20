package statements

type DeleteStatement struct {
	TableName string
	Where     *Where

	Appliable
}
