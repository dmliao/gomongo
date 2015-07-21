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

type Collection interface {
	Find(query interface{}, options *FindOpts) (Cursor, error)
	Insert(docs ...interface{}) error
	Update(selector interface{}, update interface{}, options *UpdateOpts) error
	Remove(selector interface{}, options *RemoveOpts) error
	GetMore(cursor Cursor) (Cursor, error)
	KillCursors(cursors ...Cursor) error
	// GetCount(query interface{}) int64
}

type C struct {
	name     string
	database *DB
	cursors  map[int64]*cursorObj
}

func (c *C) Find(query interface{}, options *FindOpts) (Cursor, error) {
	namespace := c.database.GetName() + "." + c.name
	requestID := c.database.mongo.conn.nextID()

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

	err = c.database.mongo.conn.send(input)
	if err != nil {
		return nil, err
	}

	cursor := cursorObj{
		collection: c,
		requestID:  requestID,
		limit:      limit,
		batchSize:  batchSize,
		flags:      flags,
	}

	err = c.database.mongo.conn.receiveFindResponse(&cursor)

	if err != nil {
		return nil, err
	}

	c.cursors[cursor.cursorID] = &cursor

	return &cursor, nil
}

func (c *C) Insert(docs ...interface{}) error {
	collection := c.name

	insertCommand := bson.D{{"insert", collection}, {"documents", docs}}

	var result bson.M
	c.database.ExecuteCommand(insertCommand, &result)

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

func (c *C) Update(selector interface{}, update interface{}, options *UpdateOpts) error {
	collection := c.name

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
	c.database.ExecuteCommand(updateCommand, &result)

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

func (c *C) Remove(selector interface{}, options *RemoveOpts) error {
	collection := c.name

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
	c.database.ExecuteCommand(deleteCommand, &result)

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

func (c *C) GetMore(cursor Cursor) (Cursor, error) {
	requestID := c.database.mongo.conn.nextID()
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

	err := c.database.mongo.conn.send(input)
	if err != nil {
		return nil, err
	}

	cObj, ok := cursor.(*cursorObj)
	if !ok {
		var ok2 bool
		cObj, ok2 = c.cursors[cursor.ID()]
		if !ok2 {
			cObj = &cursorObj{
				collection: c,
				cursorID:   cursor.ID(),
				requestID:  requestID,
				namespace:  cursor.Namespace(),
				limit:      cursor.Limit(),
				batchSize:  cursor.BatchSize(),
				err:        cursor.Error(),
			}
			c.cursors[cursor.ID()] = cObj
		}
	}

	err = c.database.mongo.conn.receiveFindResponse(cObj)

	if err != nil {
		return nil, err
	}
	return cObj, nil
}

func (c *C) KillCursors(cursors ...Cursor) error {
	requestID := c.database.mongo.conn.nextID()
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

	err := c.database.mongo.conn.send(input)
	if err != nil {
		return err
	}
	for _, cursor := range cursors {
		c.cursors[cursor.ID()] = nil
	}

	return nil
}
