package dm

import (
	"../sql/lexer"
	"../sql/parser/statements"
	"encoding/binary"
	"errors"
	"math"
	"os"
)

// 早期为了简化设计，使用定长记录。
// 文件头目前只存储块数。
// 块头目前只存放Freelist。

type (
	DataManager interface {
		Insert(data []byte) error
		Update(data []byte, uniPos uint) error
		Delete(uniPos uint) error
		Retrieve(uniPos uint) ([]byte, error)
		Boom() error
	}

	DM struct {
		TableName string
		Kacher    *cacher
	}
)

func Create(tableName string, cols []string, types []string, lens []uint16, nullables []bool, indexes []bool) (*DM, error) {
	metaDataFile, err := createFile(tableName + SUFFIX_META)
	if err != nil {
		return nil, errors.New("Failed to create mdFile.")
	}

	dataFile, err := createFile(tableName + SUFFIX_DB)
	if err != nil {
		return nil, errors.New("Failed to create dataFile.")
	}

	kacher, err := NewCacher(dataFile,
		metaDataFile,
		50,
		cols,
		types,
		lens,
		nullables,
		indexes)
	if err != nil {
		return nil, errors.New("Failed to create db.")
	}

	return &DM{
		tableName,
		kacher,
	}, nil
}

func Open(tableName string) (*DM, error) {
	metaDataFile, err := openFile(tableName + SUFFIX_META)
	if err != nil {
		return nil, errors.New("Unable to Open mdFile.")
	}
	dataFile, err := openFile(tableName + SUFFIX_DB)
	if err != nil {
		return nil, errors.New("Unable to Open dataFile")
	}

	kacher, err := NewCacher(dataFile, metaDataFile, 50, nil, nil, nil, nil, nil)
	if err != nil {
		return nil, errors.New("Unable to New Cacher")
	}

	return &DM{
		TableName: tableName,
		Kacher:    kacher,
	}, nil
}

func (dm DM) Boom() error {
	if err := os.Remove(dm.TableName + SUFFIX_DB); err != nil {
		return err
	}

	return os.Remove(dm.TableName + SUFFIX_DB)
}

func (dm DM) Insert(data []byte) (uint16, error) {
	if uint16(len(data)) != dm.Kacher.sizeOfRecord {
		return 0, errors.New("data to insert has a wrong format.")
	}

	page := dm.Kacher.GetPage(-1)
	defer page.Flush()

	freelist := page.FreeList()
	pos := freelist.Front()

	if pos.Value.(uint16) > page.maxPos {
		page.maxPos = pos.Value.(uint16)
	}

	page.numOfFreeList--
	page.freeList.Remove(freelist.Front())

	page.UpdateWith(data, pos.Value.(uint16))
	maxRecordOfPage := (PAGE_SIZE - 2) / (dm.Kacher.sizeOfRecord + 2)
	return page.index*maxRecordOfPage + pos.Value.(uint16), nil
}

func (dm DM) Update(data []byte, uniPos uint16) error {
	if uint16(len(data)) != dm.Kacher.sizeOfRecord {
		return errors.New("data to update has a wrong format.")
	}

	pgNo := uniPos / MaxNumOfRecord(dm.Kacher.sizeOfRecord)
	relativePos := uniPos % MaxNumOfRecord(dm.Kacher.sizeOfRecord)
	page := dm.Kacher.GetPage(int16(pgNo))

	for e := page.freeList.Front(); e != nil; e = e.Next() {
		if e.Value == relativePos {
			return errors.New("The Pos to update is deleted.")
		}
	}

	defer page.Flush()
	page.UpdateWith(data, relativePos)
	return nil
}

func (dm DM) UpdateBy(where *statements.Where, col string, value interface{}) string {
	md := dm.Kacher.Metadata

	index := -1
	for i, c := range md.Cols {
		if c == col {
			index = i
		}
	}

	if index == -1 {
		return "No such col"
	}

	numOfBlocks := dm.Kacher.numOfBlocks

	for i := 0; i < int(numOfBlocks); i++ {
		page := dm.Kacher.GetPage(int16(i))

		for pos := 0; pos < int(page.maxPos); i++ {
			begin := int(page.SizeOfBlockHead()) + pos*int(page.kacher.sizeOfRecord)
			data := page.data[begin : begin+int(page.kacher.sizeOfRecord)]

			if data[0]&0x80 != 0 {
				tok := value.(lexer.Token)
				switch tok.TypeInfo {
				case "INT":
					bts := make([]byte, 2)
					binary.BigEndian.PutUint16(bts, tok.Value.(uint16))
					copy(data[int(md.Offsets[i]):], bts)
				case "DOUBLE":
					bts := make([]byte, 4)
					binary.BigEndian.PutUint64(bts, math.Float64bits(tok.Value.(float64)))
					copy(data[int(md.Offsets[i]):], bts)
				case "STRING":
					bts := make([]byte, int(md.Lens[i]))
					copy(bts[:],tok.Value.(string))
					copy(data[int(md.Offsets[i]):], bts)
				}
				dm.Update(data, page.index * MaxNumOfRecord(md.SizeOfRecord) + uint16(pos))
			}
		}
	}

	return "OK!"

}

func (dm DM) Delete(uniPos uint16) error {
	pgNo := uniPos / MaxNumOfRecord(dm.Kacher.sizeOfRecord)

	if uint16(pgNo) >= dm.Kacher.numOfBlocks {
		return errors.New("The pos is not existed")
	}

	page := dm.Kacher.GetPage(int16(pgNo))

	relativePos := uniPos % MaxNumOfRecord(page.kacher.sizeOfRecord)

	// check if pos is in freelist
	for e := page.freeList.Front(); e != nil; e = e.Next() {
		if e.Value == relativePos {
			return errors.New("The Pos to deleted has been deleted.")
		}
	}

	defer page.Flush()

	page.MarkDeleteOn(relativePos)

	page.freeList.PushBack(relativePos)
	return nil
}

func (dm DM) DeleteBy(where statements.Where) error {
	numOfBlocks := dm.Kacher.numOfBlocks

	for i := 0; i < int(numOfBlocks); i++ {
		page := dm.Kacher.GetPage(int16(i))

		for pos := 0; pos < int(page.maxPos); i++ {
			begin := int(page.SizeOfBlockHead()) + pos*int(page.kacher.sizeOfRecord)
			data := page.data[begin : begin+int(page.kacher.sizeOfRecord)]

			if data[0]&0x80 != 0 {
				if dm.valid(data, where) {
					pos := page.index *
						MaxNumOfRecord(dm.Kacher.Metadata.SizeOfRecord)

					if err := dm.Delete(pos); err != nil {
						return errors.New("")
					}
				}

			}
		}
	}
	return nil
}

func DeleteAll(dm DM) error {
	os.Remove(dm.TableName + SUFFIX_DB)
	os.Remove(dm.TableName + SUFFIX_META)
	md := dm.Kacher.Metadata
	d, err := Create(dm.TableName, md.Cols, md.Types, md.Lens, md.Nullables, md.Indexes)
	dm = *d
	if err != nil {
		return errors.New("Unable to Create Table.")
	}
	return nil
}

func (dm DM) RetrieveAll() [][]byte {
	arrs := make([][]byte, 0)

	numOfBlocks := dm.Kacher.numOfBlocks
	for i := 0; i < int(numOfBlocks); i++ {
		page := dm.Kacher.GetPage(int16(i))

		for pos := 0; pos < int(page.maxPos); pos++ {
			begin := int(page.SizeOfBlockHead()) + pos*int(page.kacher.sizeOfRecord)
			data := page.data[begin : begin+int(page.kacher.sizeOfRecord)]

			if data[0]&0x80 != 0 {
				arrs = append(arrs, data)
			}
		}
	}

	return arrs
}

func (dm DM) RetrieveBy(where statements.Where) [][]byte {
	arrs := make([][]byte, 0)

	numOfBlocks := dm.Kacher.numOfBlocks

	for i := 0; i < int(numOfBlocks); i++ {
		page := dm.Kacher.GetPage(int16(i))

		for pos := 0; pos < int(page.maxPos); i++ {
			begin := int(page.SizeOfBlockHead()) + pos*int(page.kacher.sizeOfRecord)
			data := page.data[begin : begin+int(page.kacher.sizeOfRecord)]

			if data[0]&0x80 != 0 {
				if dm.valid(data, where) {
					arrs = append(arrs, data)
				}
			}
		}
	}

	return arrs
}

func (dm DM) valid(data []byte, where statements.Where) bool {
	conds := where.Expr.Conditions
	md := dm.Kacher.Metadata

	for _, cond := range conds {
		var col string
		var val uint16
		var fval float64
		var sval string

		lVal := cond.LVal.Value.(lexer.Token)
		rVal := cond.RVal.Value.(lexer.Token)

		if lVal.TypeInfo != "IDENTIFIER" || rVal.TypeInfo != "IDENTIFIER" {
			return false
		}

		if !IsValue(lVal.Value.(string)) && !IsValue(rVal.Value.(string)) {
			return false
		}

		if lVal.TypeInfo == "IDENTIFIER" {
			col = lVal.Value.(string)
			switch rVal.TypeInfo {
			case "INT":
				val = rVal.Value.(uint16)
			case "DOUBLE":
				fval = rVal.Value.(float64)
			case "STRING":
				sval = rVal.Value.(string)
			}
		} else {
			col = rVal.Value.(string)
			switch lVal.TypeInfo {
			case "INT":
				val = lVal.Value.(uint16)
			case "DOUBLE":
				fval = lVal.Value.(float64)
			case "STRING":
				sval = lVal.Value.(string)
			}
		}

		var index int
		for i, c := range md.Cols {
			if c == col {
				index = i
				break
			}
		}

		tp := md.Types[index]

		if tp == "INT" {
			parsedVal := binary.BigEndian.Uint16(data[md.Offsets[index] : md.Offsets[index]+2])
			switch cond.Op.Op {
			case "==":
				if parsedVal != val {
					return false
				}

			case ">=":
				if lVal.TypeInfo == "INT" && val < parsedVal {
					return false
				} else if parsedVal < val {
					return false
				}
			case "<=":
				if lVal.TypeInfo == "INT" && val > parsedVal {
					return false
				} else if parsedVal > val {
					return false
				}

			case ">":
				if lVal.TypeInfo == "INT" && val <= parsedVal {
					return false
				} else if parsedVal < val {
					return false
				}
			case "<":
				if lVal.TypeInfo == "INT" && val >= parsedVal {
					return false
				} else if parsedVal >= val {
					return false
				}
			}

		} else if tp == "DOUBLE" {
			parsedVal := float64(binary.BigEndian.Uint64(data[md.Offsets[index] : md.Offsets[index]+4]))

			switch cond.Op.Op {
			case "==":
				if parsedVal != fval {
					return false
				}

			case ">=":
				if lVal.TypeInfo == "DOUBLE" && fval < parsedVal {
					return false
				} else if parsedVal < fval {
					return false
				}
			case "<=":
				if lVal.TypeInfo == "DOUBLE" && fval > parsedVal {
					return false
				} else if parsedVal > fval {
					return false
				}

			case ">":
				if lVal.TypeInfo == "DOUBLE" && fval <= parsedVal {
					return false
				} else if parsedVal < fval {
					return false
				}
			case "<":
				if lVal.TypeInfo == "DOUBLE" && fval >= parsedVal {
					return false
				} else if parsedVal >= fval {
					return false
				}
			}

		} else {
			parsedVal := string(data[md.Offsets[index] : md.Offsets[index]+md.Lens[index]])

			if cond.Op.Op != "==" {
				return false
			}

			if parsedVal != sval {
				return false
			}
		}

	}
	return true
}

func IsValue(s string) bool {
	return s == "INT" || s == "DOUBLE" || s == "STRING"
}

func (dm DM) Retrieve(uniPos uint16) ([]byte, error) {
	pgNo := uniPos / MaxNumOfRecord(dm.Kacher.sizeOfRecord)
	if uint16(pgNo) >= dm.Kacher.numOfBlocks {
		return nil, errors.New("The pos is not existed")
	}

	page := dm.Kacher.GetPage(int16(pgNo))

	relativePos := uniPos % MaxNumOfRecord(page.kacher.sizeOfRecord)

	// check if pos is in freelist
	for e := page.freeList.Front(); e != nil; e = e.Next() {
		if e.Value == relativePos {
			return nil,
				errors.New("The Pos to Retrieve has been deleted.")
		}
	}

	begin := page.SizeOfBlockHead() +
		relativePos*page.kacher.sizeOfRecord

	return page.data[begin : begin+page.kacher.sizeOfRecord], nil
}

func createFile(path string) (*os.File, error) {
	file, err := os.OpenFile(path,
		os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_EXCL,
		0600)
	if err != nil {
		return nil, errors.New("Create File Err")
	}
	return file, nil
}

func openFile(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		return nil, errors.New("Open File Err")
	}

	return file, nil
}
