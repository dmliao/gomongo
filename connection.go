package gomongo

import (
	"fmt"
	"net"
)

type Conn interface {
	Close() error
	Error() error

	Find(namespace string, query interface{}, options *FindOpts) (Cursor, error)
	Insert(namespace string, docs ...interface{}) error
	Update(namespace string, selector interface{}, update interface{}, options *UpdateOpts) error
	Remove(namespace string, selector interface{}, options interface{}) error
	GetMore(cursor Cursor) error
	KillCursors(cursors ...Cursor) error
	Run(database string, command interface{}, result interface{}) error
}

type Connection struct {
	conn      net.Conn
	address   string
	requestID int32
	cursors   map[int64]*cursorObj
	err       error
}

func (c *Connection) nextID() int32 {
	c.requestID += 1
	return c.requestID
}

func (c *Connection) connect() error {
	conn, err := net.Dial("tcp", fmt.Sprintf("%v", c.address))
	if err != nil {
		return err
	}
	if c.conn != nil {
		c.conn.Close()
	}
	c.conn = conn
	return nil
}

func (c *Connection) fatal(err error) error {
	if c.err == nil {
		c.Close()
		c.err = err
	}
	return err
}

func (c *Connection) Close() error {
	return nil
}

func (c *Connection) Error() error {
	return nil
}

func (c *Connection) send(message []byte) error {
	if c.err != nil {
		return c.err
	}
	_, err := c.conn.Write(message)
	if err != nil {
		return c.fatal(err)
	}
	return nil
}
