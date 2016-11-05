package dm

import (
    "os"
    "encoding/json"
    "encoding/binary"
    "bytes"
	"errors"
    "container/list"
)

type MetaData struct {
	NumOfBlocks  uint16
	SizeOfRecord uint16
	Cols         []string
	Lens         []uint16
	Offsets      []uint16
	Nullables    []bool
}

const (
    MEM_LIT = 1000
	PAGE_SIZE = 1 << 12

	SUFFIX_DB   = ".db"
	SUFFIX_META = ".meta"
)

type PageIndex int
type PageNum PageIndex

type Page interface {
	Pgno() PageIndex
	Data() []byte
	Flush()
    NumOfFreeList() uint
    FreeList() *list.List
}

type page struct {
    index         PageIndex
    data          []byte
    numOfFreeList uint
    freeList      *list.List
    kacher        *cacher
}

type Cacher interface {
    NewPage() (PageIndex, error)
    GetPage(p PageIndex) *Page
}

type cacher struct {
    dbFile        *os.File    // 托管数据文件读写
    metaFile      *os.File    // 托管元数据文件读写 TODO remove metafile
    numOfBlocks   PageNum     // 数据文件Block数量
    pages         *list.List  // 缓存中的数据分页
    cacheLimit    PageNum     // 缓存的最大分页数
    sizeOfRecord  uint16      // 单个记录的大小
}

var (
    ErrMemTooSmall = errors.New("Mem too small.")
)

func NewCacher(dbFile *os.File,
    metaFile *os.File,
    mem PageNum,
    cols []string,
    lens []uint16,
    nullables []bool) *Cacher {
	if mem > MEM_LIT {
        panic(ErrMemTooSmall)
    }

    size := getSizeOfFile(dbFile)
    numOfPages := size / PAGE_SIZE
    _, sizeOfRecord := calcuOffsetsAndSizeOfRecord(lens)

    kacher := &cacher {
        dbFile: dbFile,
        metaFile: metaFile,
        numOfBlocks: numOfPages,
        pages: list.New(),
        cacheLimit: mem,
        sizeOfRecord: sizeOfRecord,
    }

    if numOfPages == 0 {
        kacher.flushInitMetaData(metaFile, cols, lens, nullables)
    }

    return kacher
}

func (kacher *cacher) NewPage() (*page, error) {
    maxNumOfRecord := maxNumOfRecord(kacher.sizeOfRecord)
    neoPage := &page{
        index : kacher.numOfBlocks,
        data : serializeTheBlockHead(prepareBlockHead(kacher.sizeOfRecord)),
        numOfFreeList : maxNumOfRecord,
        freeList: make([]uint, maxNumOfRecord),
        kacher: kacher,
    }

    return neoPage.index, nil
}

func (kacher *cacher) flushInitMetaData(metaFile *os.File, cols []string,
                        lens []uint16,
                        nullables []bool) {
    offsets, sizeOfRecord := calcuOffsetsAndSizeOfRecord(lens)
    writeThrough(metaFile,
        prepareMetaData(cols, lens, offsets, nullables, sizeOfRecord))
}


func prepareMetaData(cols []string,
    lens []uint16,
    offsets []uint16,
    nullables []bool,
    sizeOfRecord uint16) []byte {
    metaData, err := json.Marshal(&MetaData{
        NumOfBlocks:  0,
        SizeOfRecord: sizeOfRecord,
        Cols:         cols,
        Lens:         lens,
        Offsets:      offsets,
        Nullables:    nullables})

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

func appendFirstBlock(dataFile *os.File, sizeOfRecord uint16) {
    writeThrough(dataFile, serializeTheBlockHead(prepareBlockHead(sizeOfRecord)))
}

func prepareBlockHead(sizeOfRecord uint16) []uint16 {
    // NX + 2X + 2 == 4Ki  ==>  X == (4Ki - 2) / (N + 2), Where N is the sizeOfRecord.
    var MAX_NUM_OF_RECORDS uint16 = maxNumOfRecord(sizeOfRecord)

    blockHead := make([]uint16, PAGE_SIZE / 2)
    // At first, freelist of block is full.
    blockHead[0] = uint16(MAX_NUM_OF_RECORDS)

    // build the freelist
    for i := uint16(0); i < MAX_NUM_OF_RECORDS; i++ {
        blockHead[i+1] = i
    }

    return blockHead
}

func maxNumOfRecord(sizeOfRecord uint16) uint16 {
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

func createFile(path string) *os.File {
    file, err := os.OpenFile(path,
        os.O_RDWR|os.O_CREATE|os.O_TRUNC,
        0600)
    if err != nil {
        panic(err)
    }
    return file
}

func openFile(path string) *os.File {
    file, err := os.OpenFile(path, os.O_RDWR, 0600)
    if err != nil {
        panic(err)
    }

    return file
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

func markDeleteOn(page *page, pos uint) {
    begin := page.sizeOfBlockHead() +
        page.kacher.sizeOfRecord * pos
    page.data[begin: begin + 1] =
        page.data[begin: begin + 1] | 0x80
}

func getMetaData(metaDataFile *os.File) (MetaData, error) {
    var metaData MetaData
    var bys []byte

    if _, err := metaDataFile.Read(bys); err != nil {
        return metaData, errors.New("metaDatafile read failed")
    }

    if err := json.Unmarshal(bys, &metaData); err != nil {
        return metaData, errors.New("Deserialization MetaDataFile failed")
    }

    return metaData, nil
}


func (kacher *cacher) GetPage(pageIndex PageIndex) *page {
	if pageIndex < 0 {
		return kacher.selectFromPageList()
	}
	return kacher.getSpecificPage(pageIndex)
}

func (kacher *cacher) selectFromPageList() *page {
    // if no page exists...
    if kacher.pages.Len() == 0 {
        // if no block now...
        if kacher.numOfBlocks == 0 {
            page, err := kacher.NewPage()
            if err != nil {
                panic(err)
            }

            kacher.pages.PushBack(page)
            kacher.numOfBlocks ++
            page.Flush()
        } else {
            numOfBlockToFill := kacher.numOfBlocksToFill()
            for i := 0; i < numOfBlockToFill; i++ {
                page := kacher.loadPageAt(i)
                kacher.pages.PushBack(page)
            }
        }
    }

    maxNumOfFreeList := 0
    pgIndex := 0
    for page := kacher.pages.Front();
            page != nil;
            page = page.Next() {
        pageInCache := page.Value.(*page)
        if pageInCache.numOfFreeList > maxNumOfFreeList {
            maxNumOfFreeList = pageInCache.numOfFreeList
            pgIndex = pageInCache.index
        }
    }

    if maxNumOfFreeList != 0 {
        return kacher.GetPage(pgIndex)
    } else {
        page, err := kacher.NewPage()
        if err != nil {
            panic(err)
        }

        if kacher.pages == kacher.cacheLimit {
            page := kacher.pages.Front().Value().(*page)
            page.Flush()
            kacher.pages.Remove(kacher.pages.Front())
        }

        kacher.pages.PushBack(page)
        kacher.numOfBlocks ++

        page.Flush()
        return page
    }
}

func (kacher *cacher) numOfBlocksToFill() uint {
    if kacher.numOfBlocks > kacher.cacheLimit {
        return kacher.cacheLimit
    }
    return kacher.numOfBlocks
}

func (kacher *cacher) loadPageAt(index PageIndex) *page {
    data := make([]byte, PAGE_SIZE)
    kacher.dbFile.ReadAt(data, index * PAGE_SIZE)
    buf := bytes.NewReader(data)

    var numOfFreeList uint16
    binary.Read(buf, binary.BigEndian, &numOfFreeList)

    freeList := list.New()
    for i := 0; i < numOfFreeList; i++ {
        var item uint16
        binary.Read(buf, binary.BigEndian, &item)
        freeList.PushBack(item)
    }

    return &page{
        index: index,
        data: data,
        numOfFreeList: numOfFreeList,
        freeList: freeList,
        kacher: kacher,
    }
}

func (kacher *cacher) getSpecificPage(pageIndex *PageIndex) *page {
    if kacher.pages.Len() == 0 {
        numOfBlockToFill := kacher.numOfBlocksToFill()
        for i := 0; i < numOfBlockToFill; i++ {
            page := kacher.loadPageAt(i)
            kacher.pages.PushBack(page)
        }
    }

    // if page is already in cache.
    for e := kacher.pages.Front(); e != nil; e = e.Next() {
        pageElement := e
        pageInCache := pageElement.Value.(*page)
        if pageInCache.index == pageIndex {
            kacher.pages.MoveToBack(e)
        }
        return pageElement
    }

    // add the page into lru cache.
    if kacher.pages.Len() == kacher.cacheLimit {
        leastUsed := kacher.pages.Front().Value().(*page)
        leastUsed.Flush()
        kacher.pages.Remove(leastUsed)
    }

    page := kacher.loadPageAt(pageIndex)
    kacher.pages.PushBack(page)
    page.Flush()

    return page
}

func (p *page) sizeOfBlockHead() uint {
    return 2 + 2 * maxNumOfRecord(p.kacher.sizeOfRecord)
}

func (p *page) Flush() {
    //writeThroughAt(p.cacher.dbFile, p.index, p.data)
}

func (p *page) FreeList() *list.List {
    return p.freeList
}