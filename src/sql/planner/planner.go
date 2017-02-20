package planner

import "../parser/statements"
import "../../ds"

type Planner struct {}

var dataStorage = ds.NewDS()

func Eval(appliable statements.Appliable) string {
	planner := &Planner{}
	switch appliable.(type) {
		case statements.CreateStatement:
			return planner.evalCreate(appliable)
		case statements.SelectStatement:
			return planner.evalSelect(appliable)
		case statements.InsertStatement:
			return planner.evalInsert(appliable)
		case statements.UpdateStatement:
			return planner.evalDelete(appliable)
		case statements.DropStatement:
			return planner.evalDrop(appliable)
	}

	return "This kind of Op is not supported now."
}

func (pl Planner) evalCreate(create statements.CreateStatement) string {
	return dataStorage.CreateTable(create.TableName,
									create.Cols,
									create.Types,
									create.Lens,
									create.Nullable,
									create.Indexes)
}

func (pl Planner) evalSelect(sel statements.SelectStatement) string {
	all := sel.All != nil || sel.Star != nil

	fields := make([]string, 0)
	for _, f := range sel.Fields.Idfs {
		fields = append(fields, f)
	}

	return dataStorage.ReadTable(sel.From.Table.Idf, all, fields, sel.Where)
}

func (pl Planner) evalInsert(insert statements.InsertStatement) string {
	return dataStorage.Insert(insert.TableName, insert.Values)
}

func (pl Planner) evalUpdate(update statements.UpdateStatement) string {
	return dataStorage.Update(update.TableName, update.Col ,update.Value, update.TableName)
}

func (pl Planner) evalDelete(delete statements.DeleteStatement) string {
	return dataStorage.Delete(delete.TableName, delete.Where)
}

func (pl Planner) evalDrop(drop statements.DropStatement) string {
	return dataStorage.DropTable(drop.TableName)
}