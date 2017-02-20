package im

import (
	"github.com/datastream/btree"
	"errors"
	"os"
	"encoding/binary"
)

const SUFFIX_INDEX = ".index"

type IndexManager interface {

}

type IM struct {
	tableName string
	indexName string
	TR *btree.Btree
}

func NewIndexManager(tableName string, index string) (*IM, error) {
	if _, err :=
		btree.Unmarshal(tableName + "_" + index + SUFFIX_INDEX);
		err != nil {
		return nil, errors.New("Index_Manager has already existed.")
	}

	b := btree.NewBtree()
	if err := b.Marshal(tableName + "_" + index + SUFFIX_INDEX); err != nil {
		return nil, errors.New("Create_Index")
	}
	return &IM{
		tableName,
		index,
		b,
	}, nil
}

func GetIndexManager(tableName string, index string) (*btree.Btree, error) {
	return btree.Unmarshal(tableName + "_" + index + SUFFIX_INDEX)
}

func (im IM) Boom() error {
	return os.Remove(im.tableName + "_" + im.indexName + SUFFIX_INDEX)
}

//func (im IM) GetPositions(key uint16) []uint16 {
//	var bts []byte
//	binary.BigEndian.PutUint16(bts, key)
//	im.TR.Search(bts)
//}

//func (im IM) InsertValue(pos uint16, val ) []uint16 {
//	var bts []byte
//	binary.BigEndian.PutUint16(bts, pos)
//	im.TR.Search()
//}