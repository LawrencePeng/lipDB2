package statements

type CreateStatement struct {
	TableName string
	Cols      []string
	Types     []string
	Lens      []uint16
	Nullable  []bool

	Indexes   []string

	Appliable
}