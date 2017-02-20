package dm

import "container/list"

type Page interface {
	Pgno() uint16
	Data() []byte
	Flush()
	NumOfFreeList() uint16
}

type Pge struct {
	index         uint16
	data          []byte
	numOfFreeList uint16
	freeList      *list.List
	kacher        *cacher
	maxPos        uint16
}

func (p *Pge) PgNo() uint16 {
	return p.index
}

func (p *Pge) SizeOfBlockHead() uint16 {
	return 2 + 2*MaxNumOfRecord(p.kacher.sizeOfRecord)
}

func (p *Pge) Flush() {
	writeThroughAt(p.kacher.dbFile, uint16(p.index), p.data)
}

func (p *Pge) FreeList() *list.List {
	return p.freeList
}

func (p *Pge) NumOfFreeList() uint16 {
	return uint16(p.freeList.Len())
}

func (p *Pge) UpdateWith(bts []byte, pos uint16) {
	sizeOfRecord := p.kacher.sizeOfRecord
	begin := int(p.SizeOfBlockHead() + pos*sizeOfRecord)
	for i := 0; i < int(sizeOfRecord); i++ {
		p.data[begin+i] = bts[i]
	}
}

func (p *Pge) MarkDeleteOn(pos uint16) {
	begin := p.SizeOfBlockHead() + p.kacher.sizeOfRecord*pos
	p.data[begin] = p.data[begin] | 0x80
}
