package gomongo

import (
	"bytes"
	"encoding/binary"
	"github.com/dmliao/gomongo/buffer"
	"github.com/dmliao/gomongo/convert"
	"gopkg.in/mgo.v2/bson"
)

const (
	OP_UPDATE       int32 = 2001
	OP_INSERT             = 2002
	OP_QUERY              = 2004
	OP_GET_MORE           = 2005
	OP_DELETE             = 2006
	OP_KILL_CURSORS       = 2007
)

func (c *Connection) Find(namespace string, query interface{}, options *FindOpts) (Cursor, error) {
	requestID := c.nextID()

	limit := int32(0)
	skip := int32(0)
	batchSize := int32(20)

	if options != nil {
		limit = options.Limit
		skip = options.Skip
		if options.BatchSize > 1 {
			batchSize = options.BatchSize
		}

	}
	if limit > 1 && batchSize > limit {
		batchSize = limit
	}

	responseTo := int32(0)

	// flags
	flags := int32(0)

	queryBytes, err := bson.Marshal(query)
	if err != nil {
		return nil, err
	}
	fullCollectionBytes := []byte(namespace)
	fullCollectionBytes = append(fullCollectionBytes, byte('\x00'))

	buf := new(bytes.Buffer)
	buffer.WriteToBuf(buf, int32(0), requestID, responseTo, int32(OP_QUERY), flags, fullCollectionBytes,
		skip, batchSize, queryBytes)

	if options != nil {
		if options.Projection != nil {
			projectionBytes, err := bson.Marshal(options.Projection)
			if err != nil {
				return nil, err
			}
			buffer.WriteToBuf(buf, projectionBytes)
		}
	}

	input := buf.Bytes()

	respSize := make([]byte, 4)
	binary.LittleEndian.PutUint32(respSize, uint32(len(input)))
	input[0] = respSize[0]
	input[1] = respSize[1]
	input[2] = respSize[2]
	input[3] = respSize[3]

	err = c.send(input)
	if err != nil {
		return nil, err
	}

	cursor := cursorObj{
		conn:      c,
		namespace: namespace,
		requestID: requestID,
		limit:     limit,
		batchSize: batchSize,
		flags:     flags,
	}

	err = c.receiveFindResponse(&cursor)

	if err != nil {
		return nil, err
	}

	c.cursors[cursor.cursorID] = &cursor

	return &cursor, nil
}

func (c *Connection) KillCursors(cursors ...Cursor) error {
	requestID := c.nextID()
	responseTo := int32(0)
	buf := new(bytes.Buffer)
	buffer.WriteToBuf(buf, int32(0), requestID, responseTo, int32(OP_KILL_CURSORS), int32(len(cursors)))
	for _, cursor := range cursors {
		buffer.WriteToBuf(buf, cursor.ID())
	}
	input := buf.Bytes()

	respSize := make([]byte, 4)
	binary.LittleEndian.PutUint32(respSize, uint32(len(input)))
	input[0] = respSize[0]
	input[1] = respSize[1]
	input[2] = respSize[2]
	input[3] = respSize[3]

	err := c.send(input)
	if err != nil {
		return err
	}
	return nil
}

func (c *Connection) GetMore(cursor Cursor) (Cursor, error) {
	requestID := c.nextID()
	responseTo := int32(0)

	fullCollectionBytes := []byte(cursor.Namespace())
	fullCollectionBytes = append(fullCollectionBytes, byte('\x00'))

	numberToReturn := cursor.BatchSize()
	buf := new(bytes.Buffer)
	buffer.WriteToBuf(buf, int32(0), requestID, responseTo, int32(OP_GET_MORE), int32(0), fullCollectionBytes,
		numberToReturn, cursor.ID())

	input := buf.Bytes()

	respSize := make([]byte, 4)
	binary.LittleEndian.PutUint32(respSize, uint32(len(input)))
	input[0] = respSize[0]
	input[1] = respSize[1]
	input[2] = respSize[2]
	input[3] = respSize[3]

	err := c.send(input)
	if err != nil {
		return nil, err
	}

	cObj, ok := cursor.(*cursorObj)
	if !ok {
		var ok2 bool
		cObj, ok2 = c.cursors[cursor.ID()]
		if !ok2 {
			cObj = &cursorObj{
				conn:      c,
				cursorID:  cursor.ID(),
				requestID: requestID,
				namespace: cursor.Namespace(),
				limit:     cursor.Limit(),
				batchSize: cursor.BatchSize(),
				err:       cursor.Error(),
			}
			c.cursors[cursor.ID()] = cObj
		}
	}

	err = c.receiveFindResponse(cObj)

	if err != nil {
		return nil, err
	}
	return cObj, nil
}

func (c *Connection) Run(database string, command interface{}, result interface{}) error {
	commandBytes, err := bson.Marshal(command)
	if err != nil {
		return err
	}

	namespace := database + ".$cmd"

	requestID := c.nextID()

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

	err = c.send(input)
	if err != nil {
		return err
	}

	res, err := c.receive()
	if err != nil {
		return err
	}

	resultBytes := res.Document[0]
	return bson.Unmarshal(resultBytes, result)

}

func (c *Connection) Insert(namespace string, docs ...interface{}) error {
	database, collection, err := ParseNamespace(namespace)
	if err != nil {
		return err
	}

	insertCommand := bson.D{{"insert", collection}, {"documents", docs}}

	var result bson.M
	c.Run(database, insertCommand, &result)

	if convert.ToInt(result["ok"]) == 1 {
		return nil
	}

	writeConcernError := convert.ToBSONMap(result["writeConcernError"])
	if writeConcernError != nil {
		return WriteConcernError{
			Code:   convert.ToInt32(writeConcernError["code"]),
			ErrMsg: convert.ToString(writeConcernError["errmsg"]),
		}
	}

	writeErrors, err := convert.ConvertToBSONMapSlice(result["writeErrors"])
	if err == nil {
		errors := WriteErrors{}
		errors.Errors = make([]WriteError, len(writeErrors))
		for i := 0; i < len(writeErrors); i++ {
			writeError := WriteError{
				Index:  convert.ToInt32(writeErrors[i]["index"]),
				Code:   convert.ToInt32(writeErrors[i]["code"]),
				ErrMsg: convert.ToString(writeErrors[i]["errmsg"]),
			}
			errors.Errors[i] = writeError
		}
		return errors
	}

	return MongoError{
		message: "Something failed",
	}
}

func (c *Connection) Update(namespace string, selector interface{}, update interface{}, options *UpdateOpts) error {
	database, collection, err := ParseNamespace(namespace)
	if err != nil {
		return err
	}

	multi := false
	if options != nil {
		multi = options.Multi
	}

	updates := make([]bson.M, 1)
	updates[0] = bson.M{
		"q":      selector,
		"u":      update,
		"upsert": false,
		"multi":  multi,
	}

	updateCommand := bson.D{{"update", collection}, {"updates", updates}}

	var result bson.M
	c.Run(database, updateCommand, &result)

	if convert.ToInt(result["ok"]) == 1 {
		return nil
	}

	writeConcernError := convert.ToBSONMap(result["writeConcernError"])
	if writeConcernError != nil {
		return WriteConcernError{
			Code:   convert.ToInt32(writeConcernError["code"]),
			ErrMsg: convert.ToString(writeConcernError["errmsg"]),
		}
	}

	writeErrors, err := convert.ConvertToBSONMapSlice(result["writeErrors"])
	if err == nil {
		errors := WriteErrors{}
		errors.Errors = make([]WriteError, len(writeErrors))
		for i := 0; i < len(writeErrors); i++ {
			writeError := WriteError{
				Index:  convert.ToInt32(writeErrors[i]["index"]),
				Code:   convert.ToInt32(writeErrors[i]["code"]),
				ErrMsg: convert.ToString(writeErrors[i]["errmsg"]),
			}
			errors.Errors[i] = writeError
		}
		return errors
	}

	return MongoError{
		message: "Something failed",
	}
}

func (c *Connection) Remove(namespace string, selector interface{}, options *RemoveOpts) error {
	database, collection, err := ParseNamespace(namespace)
	if err != nil {
		return err
	}

	limit := 1
	if options != nil {
		if options.Multi {
			limit = 0
		}
	}

	deletes := make([]bson.M, 1)
	deletes[0] = bson.M{
		"q":     selector,
		"limit": limit,
	}

	deleteCommand := bson.D{{"delete", collection}, {"deletes", deletes}}

	var result bson.M
	c.Run(database, deleteCommand, &result)

	if convert.ToInt(result["ok"]) == 1 {
		return nil
	}

	writeConcernError := convert.ToBSONMap(result["writeConcernError"])
	if writeConcernError != nil {
		return WriteConcernError{
			Code:   convert.ToInt32(writeConcernError["code"]),
			ErrMsg: convert.ToString(writeConcernError["errmsg"]),
		}
	}

	writeErrors, err := convert.ConvertToBSONMapSlice(result["writeErrors"])
	if err == nil {
		errors := WriteErrors{}
		errors.Errors = make([]WriteError, len(writeErrors))
		for i := 0; i < len(writeErrors); i++ {
			writeError := WriteError{
				Index:  convert.ToInt32(writeErrors[i]["index"]),
				Code:   convert.ToInt32(writeErrors[i]["code"]),
				ErrMsg: convert.ToString(writeErrors[i]["errmsg"]),
			}
			errors.Errors[i] = writeError
		}
		return errors
	}

	return MongoError{
		message: "Something failed",
	}
}
