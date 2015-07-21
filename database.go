package gomongo

import (
	"bytes"
	"encoding/binary"
	"github.com/dmliao/gomongo/buffer"
	"gopkg.in/mgo.v2/bson"
)

type Database interface {
	GetName() string
	// GetCollectionNames() []string
	GetCollection(string) Collection
	// DropCollection(Collection) bool
	ExecuteCommand(interface{}, interface{}) error
	// DropDatabase() bool
}

type DB struct {
	name  string
	mongo *MongoDB
}

func (d *DB) GetName() string {
	return d.name
}

func (d *DB) GetCollection(cName string) Collection {
	return &C{
		name:     cName,
		database: d,
		cursors:  make(map[int64]*cursorObj),
	}
}

func (d *DB) ExecuteCommand(command interface{}, result interface{}) error {
	commandBytes, err := bson.Marshal(command)
	if err != nil {
		return err
	}

	namespace := d.name + ".$cmd"

	requestID := d.mongo.conn.nextID()

	limit := int32(-1)
	skip := int32(0)
	responseTo := int32(0)

	// flags
	flags := int32(0)
	fullCollectionBytes := []byte(namespace)
	fullCollectionBytes = append(fullCollectionBytes, byte('\x00'))

	buf := new(bytes.Buffer)
	buffer.WriteToBuf(buf, int32(0), requestID, responseTo, int32(OP_QUERY), flags, fullCollectionBytes,
		skip, limit, commandBytes)

	input := buf.Bytes()

	respSize := make([]byte, 4)
	binary.LittleEndian.PutUint32(respSize, uint32(len(input)))
	input[0] = respSize[0]
	input[1] = respSize[1]
	input[2] = respSize[2]
	input[3] = respSize[3]

	err = d.mongo.conn.send(input)
	if err != nil {
		return err
	}

	res, err := d.mongo.conn.receive()
	if err != nil {
		return err
	}

	resultBytes := res.Document[0]
	return bson.Unmarshal(resultBytes, result)

}
