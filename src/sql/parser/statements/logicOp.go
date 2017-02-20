package statements

type LogicOperation struct {
	Op string
}

func IsLogicOperation(operation LogicOperation) bool {
	return operation.Op == ">" ||
		operation.Op == "<" ||
		operation.Op == ">=" ||
		operation.Op == "<=" ||
		operation.Op == "=="
}

func IsInterLogicOperation(operation LogicOperation) bool {
	return operation.Op == "AND" ||
		operation.Op == "OR" ||
		operation.Op == "NOT"
}
