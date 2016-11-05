package statements

type (
	Fields struct {
		Idfs []Field
	}
)

func (fs Fields) IsFields() bool {
	return IsFieldsStatement(fs)
}

func IsFieldsStatement(fs Fields) bool {
	allFields := true

	fds := fs.Idfs
	for _, field := range fds {
		if !IsFieldStatement(field) {
			return false
		}
	}

	return allFields
}
