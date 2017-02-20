package statements

type Condition struct {
	LVal Value
	RVal Value
	Op   LogicOperation
}

func IsCondition(condition Condition) bool {
	return IsLogicOperation(condition.Op)
}
