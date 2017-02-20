package ds

import (
	"../dm"
	"../im"
	"../sql/lexer"
	"../sql/parser/statements"
	"encoding/binary"
	"errors"
	"math"
	"strconv"
)

type DS struct {
	tables map[string]*diPair
}

type diPair struct {
	dm  *dm.DM
	ims []*im.IM
}

func NewDS() *DS { return &DS{make(map[string]*diPair)} }

func (ds DS) CreateTable(tableName string,
	cols []string,
	types []string,
	lens []uint16,
	nullables []bool,
	indexes []string) string {

	indexesToBuild := make([]bool, len(cols))

	for _, s := range indexes {
		for i, c := range cols {
			if s == c {
				indexesToBuild[i] = true
				continue
			}
		}
		return "No col called" + s
	}

	if ds.tables[tableName] != nil {
		return "The table has been created"
	}

	dP := &diPair{}

	dataManager, err := dm.Create(tableName, cols, types, lens, nullables, indexesToBuild)
	if err != nil {
		return err.Error()
	}
	dP.dm = dataManager

	ims := make([]*im.IM, 0)
	for i, s := range indexesToBuild {
		if s {
			indexM, err := im.NewIndexManager(tableName, cols[i])
			if err != nil {
				return "Failed to Create Index for " + tableName +
					"." + cols[i]
			}
			ims = append(ims, indexM)
		}
	}
	dP.ims = ims

	ds.tables[tableName] = dP
	return "OK"
}

func (ds DS) DropTable(tableName string) string {
	t := ds.tables[tableName]
	if t == nil {
		tableFromDisk, err := loadTableFromDisk(tableName)
		if err != nil {
			return err.Error()
		}

		ds.tables[tableName] = tableFromDisk
		t = ds.tables[tableName]
	}

	if err := t.dm.Boom(); err != nil {
		return "Failed to Delete the table."
	}
	t.dm = nil

	for i, index := range t.ims {
		if err := index.Boom(); err != nil {
			return "RM Index Failed."
		}

		t.ims = append(t.ims[:i], t.ims[i+1:]...)
	}
	t.ims = nil

	delete(ds.tables, tableName)

	return "OK!"
}

func (ds DS) ReadTable(tableName string,
	all bool,
	fields []string,
	where *statements.Where) string {
	table := ds.tables[tableName]
	if table == nil {
		table, err := loadTableFromDisk(tableName)
		if err != nil {
			return "No Such Table."
		}

		ds.tables[tableName] = table
	}

	md := table.dm.Kacher.Metadata

	cols := md.Cols
	for _, f := range fields {
		in := false
		for _, c := range cols {
			if c == f {
				in = true
			}
		}
		if !in {
			return "No field " + f
		}
	}

	if where == nil {
		byteArrs := ReadAllPosFrom(table.dm)

		ret := "{ "

		for _, arr := range byteArrs {
			ret += "["
			for _, f := range fields {
				for i, c := range md.Cols {
					if f == c { // Support null
						if md.Types[i] == "INT" {
							bytes := arr[int(md.Offsets[i]):int(md.Offsets[i]+2)]
							ret += string(int(binary.BigEndian.Uint16(bytes))) + ","
						} else if md.Types[i] == "DOUBLE" {
							bytes := arr[int(md.Offsets[i]):int(md.Offsets[i]+4)]
							ret += string(strconv.FormatFloat(float64(binary.BigEndian.Uint64(bytes)), 'g', 1, 64)) + ","
						} else {
							ret += string(arr[int(md.Offsets[i]):int(md.Offsets[i]+md.Lens[i])]) + ","
						}
					}
				}
			}
			ret += "]"
		}

		return ret + " }"

	} else {

		arrs := table.dm.RetrieveBy(*where)

		ret := "{ "

		for _, arr := range arrs {
			ret += "["
			for _, f := range fields {
				for i, c := range md.Cols {
					if f == c { // Support null
						if md.Types[i] == "INT" {
							bytes := arr[int(md.Offsets[i]):int(md.Offsets[i]+2)]
							ret += string(int(binary.BigEndian.Uint16(bytes))) + ","
						} else if md.Types[i] == "DOUBLE" {
							bytes := arr[int(md.Offsets[i]):int(md.Offsets[i]+4)]
							ret += string(strconv.FormatFloat(float64(binary.BigEndian.Uint64(bytes)), 'g', 1, 64)) + ","
						} else {
							ret += string(arr[int(md.Offsets[i]):int(md.Offsets[i]+md.Lens[i])]) + ","
						}
					}
				}
			}
			ret += "]"
		}

		return ret + " }"

		//conds := where.Expr.Conditions

		// hasIndex := false
		// ind := ""
		// ran := []int{
		//	math.MinInt16,
		//	math.MaxInt16,
		// }
		//
		// for _, cond := range conds {
		//
		//	for i, index := range md.Indexes {
		//		if index && md.Types[i] != "INT" && cond == md.Cols[i] {
		//			if cond.LVal.Value == md.Cols[i] {
		//				hasIndex = true
		//				index = md.Cols[i]
		//
		//			} else if cond.RVal.Value == md.Cols[i] {
		//
		//			}
		//		} else if index {
		//			return "Now we only support int type index."
		//		}
		//	}
		// }

		//if hasIndex {
		//	ReadByIndex(table, table.ims[ind], ran)
		//} else {
		//
		//}
	}
}

func ReadAllPosFrom(table *dm.DM) [][]byte {
	return table.RetrieveAll()
}

func (ds DS) Delete(tableName string, where statements.Where) string {
	table := ds.tables[tableName]
	if table == nil {
		table, err := loadTableFromDisk(tableName)
		if err != nil {
			return "No Such Table."
		}

		ds.tables[tableName] = table
	}
	if err := table.dm.DeleteBy(where); err != nil {
		return "Fail to Delete."
	}
	return "OK"
}

func (ds DS) Insert(tableName string, values []interface{}) string {

	table := ds.tables[tableName]
	if table == nil {
		table, err := loadTableFromDisk(tableName)
		if err != nil {
			return "No Such Table."
		}

		ds.tables[tableName] = table
	}

	md := table.dm.Kacher.Metadata
	if len(values) != len(md.Cols) {
		return "You input more or less values than actual."
	}

	data := make([]byte, md.SizeOfRecord)

	for i, v := range values {
		tok := v.(lexer.Token)
		if tok.TypeInfo == "NULL" {
			if !md.Nullables[i] {
				return "Col" + md.Cols[i] + "is not nullable."
			} else {
				continue
			}
		}

		if tok.TypeInfo != md.Types[i] {
			return "Wrong type for " + md.Cols[i]
		}
	}

	for i, v := range values {
		tok := v.(lexer.Token)

		if tok.TypeInfo == "NULL" {

		} else if tok.TypeInfo == "STRING" {
			s := tok.Value.(string)
			if len(s) > int(md.Lens[i]) {
				return "To long for col" + md.Cols[i]
			}

			copy(data[md.Offsets[i]:], []byte(s))
		} else if tok.TypeInfo == "INT" {
			integer := tok.Value.(uint16)

			var bts []byte
			binary.BigEndian.PutUint16(bts, integer)

			copy(data[md.Offsets[i]:], bts)

		} else {
			double := tok.Value.(float64)
			integer := math.Float64bits(double)

			var bts []byte
			binary.BigEndian.PutUint64(bts, integer)

			copy(data[md.Offsets[i]:],bts)
		}
	}

	_, err := table.dm.Insert(data)
	if err != nil {
		return err.Error()
	}

	//md := table.dm.Kacher.Metadata

	//for i, index := range md.Indexes {
	//	if index {
	//		table.ims[i].InsertValue(pos, values[i])
	//	}
	//}

	return "OK"
}

func (ds DS) Update(tableName string,
	col string,
	value interface{},
	where *statements.Where) string {
	table := ds.tables[tableName]

	if table == nil {
		table, err := loadTableFromDisk(tableName)
		if err != nil {
			return "No Such Table."
		}

		ds.tables[tableName] = table
	}

	return table.dm.UpdateBy(where, col, value)
}

func loadTableFromDisk(tableName string) (*diPair, error) {
	diPair := &diPair{}
	dataManager, err := dm.Open(tableName)
	if err != nil {
		return nil, errors.New("Can't get dataManager.")
	}
	diPair.dm = dataManager

	indexes := dataManager.Kacher.Metadata.Indexes
	for i, v := range indexes {
		if v {
			b, err := im.GetIndexManager(tableName,
				dataManager.Kacher.Metadata.Cols[i])
			if err != nil {
				return nil, errors.New("Can't load indexManager.")
			}
			diPair.ims = append(diPair.ims, b)
		}
	}
	return diPair, nil
}
