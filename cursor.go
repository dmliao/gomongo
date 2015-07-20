package gomongo

import (
	"gopkg.in/mgo.v2/bson"
	"io"
)

type Cursor interface {
	ID() int64
	Close() error
	Error() error

	Namespace() string
	BatchSize() int32
	Limit() int32

	HasNext() bool
	Next(result interface{}) error
}

type cursorObj struct {
	conn      *Connection
	cursorID  int64
	requestID int32
	namespace string
	limit     int32
	batchSize int32
	count     int32
	docCount  int32
	docs      [][]byte
	err       error
	flags     int32
}

func (c *cursorObj) fatal(err error) error {
	if c.err == nil {
		c.Close()
		c.err = err
	}
	return err
}

func (c *cursorObj) ID() int64 {
	return c.cursorID
}

func (c *cursorObj) Namespace() string {
	return c.namespace
}

func (c *cursorObj) BatchSize() int32 {
	return c.batchSize
}

func (c *cursorObj) Limit() int32 {
	return c.limit
}

func (c *cursorObj) Close() error {
	if c.err != nil {
		return nil
	}
	if c.requestID != 0 {
		c.conn.cursors[c.cursorID] = nil
	}
	if c.cursorID != 0 {
		c.conn.KillCursors(c)
	}
	c.err = MongoError{
		message: "Cursor closed",
	}
	c.conn = nil
	return nil
}

func (c *cursorObj) Error() error {
	return c.err
}

func (c *cursorObj) getNextBatch() error {
	if c.err != nil {
		return c.err
	}

	if c.limit > 0 && c.count >= c.limit {
		return c.fatal(io.EOF)
	}
	if c.cursorID == 0 {
		return c.fatal(io.EOF)
	}

	_, err := c.conn.GetMore(c)
	if err != nil {
		return c.fatal(err)
	}
	c.docCount = 0
	return nil
}

func (c *cursorObj) HasNext() bool {
	if c.err != nil {
		return false
	}

	if c.limit > 0 && c.count >= c.limit {
		return false
	}
	if c.docCount < int32(len(c.docs)) {
		return true
	}

	err := c.getNextBatch()
	if err != nil {
		return false
	}
	c.docCount = 0
	return true
}

func (c *cursorObj) Next(result interface{}) error {
	if !c.HasNext() {
		return io.EOF
	}
	r := c.docs[c.docCount]
	c.docCount++
	c.count++

	return bson.Unmarshal(r, result)
}
