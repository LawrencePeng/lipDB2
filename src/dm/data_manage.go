package dm

import (
	"errors"
)

// 早期为了简化设计，使用定长记录。
// 文件头目前只存储块数。
// 块头目前只存放Freelist。

type DataManager interface {
	Insert(data []byte) error
	Update(data []byte, uniPos uint) error
	Delete(uniPos uint) error
	Retrieve(uniPos uint) ([]byte, error)
}

type dataManager struct {
	tableName string
	kacher *cacher
}

func Create(tableName string, cols []string, lens []uint16, nullables []bool) *DataManager {
	//offsets, sizeOfRecord := calcuOffsetsAndSizeOfRecord(lens)
	metaDataFile := createFile(tableName + SUFFIX_META)
	//writeThrough(metaDataFile,
	//	prepareMetaData(cols, lens, offsets, nullables, sizeOfRecord))

	dataFile := createFile(tableName + SUFFIX_DB)
	//appendFirstBlock(dataFile, sizeOfRecord)

	kacher := NewCacher(dataFile, metaDataFile, 50, cols, lens, nullables)

	return &dataManager {
		tableName: tableName,
		kacher: kacher,
	}
}


func Open(tableName string) {
	metaDataFile := openFile(tableName + SUFFIX_META)
	dataFile := openFile(tableName + SUFFIX_DB)

	kacher := NewCacher(dataFile, metaDataFile, 50, nil, nil, nil)
	return &dataManager {
		tableName: tableName,
		kacher: kacher,
	}
}

func (dm *dataManager) Insert(data []byte) error {
	if len(data) != dm.kacher.sizeOfRecord {
		return errors.New("data to insert has a wrong format.")
	}

	page := dm.kacher.GetPage(-1)
	defer page.Flush()

	freelist := page.FreeList()
	pos := freelist.Front()

	page.numOfFreeList --
	page.freeList.Remove(freelist.Front())

	updateWith(data, page, pos)
	return nil
}



func updateWith(bts []byte, page *page, pos uint) {
	sizeOfRecord := page.kacher.sizeOfRecord
	begin := page.sizeOfBlockHead() + pos * sizeOfRecord
	page.data[begin: begin + sizeOfRecord] = bts
}

func (dm *dataManager) Update(data []byte, uniPos uint) error {
	if len(data) != dm.kacher.sizeOfRecord {
		return errors.New("data to update has a wrong format.")
	}

	pgNo := uniPos / maxNumOfRecord(dm.kacher.sizeOfRecord)
	page := dm.kacher.GetPage(pgNo)

	for e := page.freeList.Front(); e != nil; e = e.Next() {
		if e.Value.(*uint) == uniPos {
			return errors.New("The Pos to update is deleted.")
		}
	}

	defer page.Flush()

	updateWith(data, page, uniPos)

	return nil
}

func (dm *dataManager) Delete(uniPos uint) error {
	pgNo := uniPos / maxNumOfRecord(dm.kacher.sizeOfRecord)
	if pgNo >= dm.kacher.numOfBlocks {
		return errors.New("The pos is not existed")
	}
	page := dm.kacher.GetPage(pgNo)

	relativePos := uniPos % maxNumOfRecord(page.kacher.sizeOfRecord)

	// check if pos is in freelist
	for e := page.freeList.Front(); e != nil; e = e.Next() {
		if e.Value.(*uint) == relativePos {
			return errors.New("The Pos to deleted has been deleted.")
		}
	}

	defer page.Flush()

	markDeleteOn(page, relativePos)

	page.freeList.PushBack(relativePos)
	return nil
}

func (dm *dataManager) Retrieve(uniPos uint) ([]byte, error) {
	pgNo := uniPos / maxNumOfRecord(dm.kacher.sizeOfRecord)
	if pgNo >= dm.kacher.numOfBlocks {
		return []byte{}, errors.New("The pos is not existed")
	}
	page := dm.kacher.GetPage(pgNo)

	relativePos :=
		uniPos % maxNumOfRecord(page.kacher.sizeOfRecord)

	// check if pos is in freelist
	for e := page.freeList.Front(); e != nil; e = e.Next() {
		if e.Value.(*uint) == relativePos {
			return []byte{},
				errors.New("The Pos to Retrieve has been deleted.")
		}
	}

	begin := page.sizeOfBlockHead() +
		relativePos * page.kacher.sizeOfRecord
	return page.data[begin : begin + page.kacher.sizeOfRecord], nil
}