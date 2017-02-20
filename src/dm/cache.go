package dm

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
)

type MetaData struct {
	NumOfBlocks  uint16
	SizeOfRecord uint16
	Cols         []string
	Types        []string
	Lens         []uint16
	Offsets      []uint16
	Nullables    []bool
	Indexes      []bool
}

const (
	MEM_LIT   = 1000
	PAGE_SIZE = 1 << 12

	SUFFIX_DB   = ".db"
	SUFFIX_META = ".meta"
)

type Cacher interface {
	NewPage() (PageIndex, error)
	GetPage(p PageIndex) *Page
}

type cacher struct {
	dbFile       *os.File   // file to store data
	numOfBlocks  PageNum    // block is like page in cache.
	pages        *list.List // pages are stored in cache.
	cacheLimit   PageNum    // max pages to keep in cache
	sizeOfRecord uint16     // every record's size
	Metadata     *MetaData  // metadata which is stored in .meta file
}

var ErrMemTooSmall = errors.New("Mem too small.")

func NewCacher(dbFile *os.File,
	metaFile *os.File,
	mem PageNum,
	cols []string,
	types []string,
	lens []uint16,
	nullables []bool,
	indexes []bool) (*cacher, error) {
	if mem > MEM_LIT {
		return nil, ErrMemTooSmall
	}

	size := getSizeOfFile(dbFile)
	numOfPages := size / PAGE_SIZE
	_, sizeOfRecord := calcuOffsetsAndSizeOfRecord(lens)

	if size == 0 { // no record in dbFile
		flushInitMetaData(metaFile, cols, types,lens, nullables, indexes)
	}

	md, err := getMetaData(metaFile)
	if err != nil {
		return nil, "Failed to Get metadata."
	}

	return &cacher{
		dbFile,
		PageNum(numOfPages),
		list.New(),
		mem,
		sizeOfRecord,
		md,
	}

}

func (kacher *cacher) NewPage() (*Pge, error) {
	maxNumOfRecord := MaxNumOfRecord(kacher.sizeOfRecord)

	freeList := list.New()

	for i := uint16(0); i < maxNumOfRecord; i++ {
		freeList.PushBack(i)
	}

	neoPage := &Pge{
		PageIndex(kacher.numOfBlocks),
		serializeTheBlockHead(prepareBlockHead(kacher.sizeOfRecord)),
		maxNumOfRecord,
		freeList,
		kacher,
		0,
	}

	return neoPage, nil
}

func flushInitMetaData(metaFile *os.File, cols []string,
	types []string,
	lens []uint16,
	nullables []bool,
	indexes   []bool) {
	offsets, sizeOfRecord := calcuOffsetsAndSizeOfRecord(lens)
	writeThrough(metaFile,
		prepareMetaData(cols, lens, types, offsets, nullables, sizeOfRecord, indexes))
}

func prepareMetaData(cols []string,
	lens []uint16,
	types []string,
	offsets []uint16,
	nullables []bool,
	sizeOfRecord uint16,
	indexes []bool) []byte {
	metaData, err := json.Marshal(&MetaData{
		0,
		sizeOfRecord,
		cols,
		types,
		lens,
		offsets,
		nullables,
		indexes,
	})

	if err != nil {
		panic(err)
	}

	return []byte(metaData)
}

func getSizeOfFile(file *os.File) int64 {
	info, err := file.Stat()
	if err != nil {
		panic(errors.New("FS Err"))
	}
	return info.Size()

}

func calcuOffsetsAndSizeOfRecord(lens []uint16) ([]uint16, uint16) {
	offsets := make([]uint16, len(lens))

	markAndNulls := 1 + uint16(len(lens)) // delete mark and null marks
	var sizeOfRecord uint16

	if markAndNulls%8 == 0 {
		sizeOfRecord = markAndNulls % 8
	} else {
		sizeOfRecord = markAndNulls/8 + 1
	}

	if sizeOfRecord%4 != 0 {
		tmp := sizeOfRecord / 4
		sizeOfRecord = tmp*4 + 4
	}

	for i := 0; i < len(lens); i++ {
		offsets[i] = sizeOfRecord

		if lens[i]%4 == 0 {
			sizeOfRecord += lens[i]
		} else {
			tmp := lens[i] / 4 // remember the padding
			sizeOfRecord += tmp*4 + 4
		}
	}

	return offsets, sizeOfRecord
}

//func appendFirstBlock(dataFile *os.File, sizeOfRecord uint16) {
//    writeThrough(dataFile, serializeTheBlockHead(prepareBlockHead(sizeOfRecord)))
//}

func prepareBlockHead(sizeOfRecord uint16) []uint16 {
	// NX + 2X + 2 == 4Ki  ==>  X == (4Ki - 2) / (N + 2), Where N is the sizeOfRecord.
	var MAX_NUM_OF_RECORDS uint16 = MaxNumOfRecord(sizeOfRecord)

	blockHead := make([]uint16, PAGE_SIZE/2)
	// At first, freelist of block is full.
	blockHead[0] = uint16(MAX_NUM_OF_RECORDS)

	// build the freelist
	for i := uint16(0); i < MAX_NUM_OF_RECORDS; i++ {
		blockHead[i+1] = i
	}

	return blockHead
}

func MaxNumOfRecord(sizeOfRecord uint16) uint16 {
	return (PAGE_SIZE - 2) / (sizeOfRecord + 2)
}

func serializeTheBlockHead(blockHead []uint16) []byte {
	buf := new(bytes.Buffer)

	for i := 0; i < len(blockHead); i++ { // serialization
		if err := binary.Write(buf, binary.LittleEndian, blockHead[i]); err != nil {
			panic(err)
		}
	}

	return buf.Bytes()
}

func writeThrough(file *os.File, byteArr []byte) {
	if _, err := file.Write(byteArr); err != nil {
		panic(err)
	}

	if err := file.Sync(); err != nil {
		panic(err)
	}
}

func writeThroughAt(file *os.File, index uint16, byteArr []byte) {
	if _, err := file.WriteAt(byteArr, int64(index*PAGE_SIZE)); err != nil {
		panic(err)
	}

	if err := file.Sync(); err != nil {
		panic(err)
	}
}

func getMetaData(metaDataFile *os.File) (*MetaData, error) {
	var metaData MetaData
	var bys []byte

	if _, err := metaDataFile.Read(bys); err != nil {
		return &metaData,
			errors.New("metaDatafile read failed")
	}

	if err := json.Unmarshal(bys, &metaData); err != nil {
		return &metaData,
			errors.New("Deserialization MetaDataFile failed")
	}

	return &metaData, nil
}

func (kacher *cacher) GetPage(pageIndex PageIndex) *Pge {
	if pageIndex >= PageIndex(kacher.numOfBlocks) {
		return nil
	}

	if pageIndex < 0 {
		return kacher.selectFromPageList()
	}
	return kacher.getSpecificPage(pageIndex)
}

func (kacher *cacher) selectFromPageList() *Pge {
	// if no Pge exists...
	if kacher.pages.Len() == 0 {
		// if no block now...
		if kacher.numOfBlocks == 0 {
			page, err := kacher.NewPage()
			if err != nil {
				panic(err)
			}

			kacher.pages.PushBack(page)
			kacher.numOfBlocks++
			page.Flush()
		} else {
			numOfBlockToFill := kacher.numOfBlocksToFill()
			for i := 0; i < int(numOfBlockToFill); i++ {
				page := kacher.loadPageAt(PageIndex(i))
				kacher.pages.PushBack(page)
			}
		}
	}

	maxNumOfFreeList, pgIndex := kacher.choosePage()

	if maxNumOfFreeList != 0 {
		return kacher.GetPage(pgIndex)
	}

	page, err := kacher.NewPage()
	if err != nil {
		panic(err)
	}

	if PageNum(kacher.pages.Len()) == kacher.cacheLimit {
		page := kacher.pages.Front().Value.(*Pge)
		page.Flush()
		kacher.pages.Remove(kacher.pages.Front())
	}

	kacher.pages.PushBack(page)
	kacher.numOfBlocks++

	page.Flush()
	return page

}

func (kacher *cacher) choosePage() (uint16, PageIndex) {
	maxNumOfFreeList := uint16(0)
	pgIndex := PageIndex(0)
	for page := kacher.pages.Front(); page != nil; page = page.Next() {
		pageInCache := page.Value.(*Pge)
		if pageInCache.numOfFreeList > maxNumOfFreeList {
			maxNumOfFreeList = pageInCache.numOfFreeList
			pgIndex = pageInCache.index
		}
	}

	return maxNumOfFreeList, pgIndex
}

func (kacher *cacher) numOfBlocksToFill() uint16 {
	if kacher.numOfBlocks > kacher.cacheLimit {
		return uint16(kacher.cacheLimit)
	}

	return uint16(kacher.numOfBlocks)
}

func (kacher *cacher) loadPageAt(index PageIndex) *Pge {
	data := make([]byte, PAGE_SIZE)
	kacher.dbFile.ReadAt(data, int64(index)*PAGE_SIZE)
	buf := bytes.NewReader(data)

	var numOfFreeList uint16
	binary.Read(buf, binary.BigEndian, &numOfFreeList)

	freeList := list.New()
	for i := 0; i < int(numOfFreeList); i++ {
		var item uint16
		binary.Read(buf, binary.BigEndian, &item)
		freeList.PushBack(item)
	}

	return &Pge{
		index:         index,
		data:          data,
		numOfFreeList: numOfFreeList,
		freeList:      freeList,
		kacher:        kacher,
	}
}

func (kacher *cacher) getSpecificPage(pageIndex PageIndex) *Pge {
	if kacher.pages.Len() == 0 { // if there is no Pge in cache

		numOfBlockToFill := kacher.numOfBlocksToFill()

		for i := 0; i < int(numOfBlockToFill); i++ {
			page := kacher.loadPageAt(PageIndex(i))
			kacher.pages.PushBack(page)
		}
	}

	// if Pge is already in cache.
	for e := kacher.pages.Front(); e != nil; e = e.Next() {

		pageInCache := e.Value.(*Pge)

		if pageInCache.index == pageIndex {
			kacher.pages.MoveToBack(e)
		}

		return e.Value.(*Pge)
	}

	// add the Page into lru cache.
	if PageNum(kacher.pages.Len()) == kacher.cacheLimit {

		leastUsed := kacher.pages.Front().Value.(*Pge)
		leastUsed.Flush()

		kacher.pages.Remove(kacher.pages.Front())
	}

	page := kacher.loadPageAt(pageIndex)
	kacher.pages.PushBack(page)
	page.Flush()

	return page
}
