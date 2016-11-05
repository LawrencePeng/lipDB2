package statements

type (
	Value struct {
		Value interface{}
	}
)

func (val Value) IsValue() bool {
	return IsValueStatement(val)
}

func IsValueStatement(val Value) bool {
	return IsNumberStatement(val.Value.(Number)) || IsStringStatement(val.Value.(String))
}
