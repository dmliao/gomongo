package gomongo

import (
	"fmt"
	"gopkg.in/fatih/pool.v2"
	"net"
)

type Conn interface {
	Close() error
	Error() error
}

type Connection struct {
	connPool  pool.Pool
	conn      net.Conn
	address   string
	requestID int32
	err       error
}

func (c *Connection) nextID() int32 {
	c.requestID += 1
	return c.requestID
}

func (c *Connection) connect() error {
	factory := func() (net.Conn, error) {
		return net.Dial("tcp", fmt.Sprintf("%v", c.address))
	}
	p, err := pool.NewChannelPool(5, 30, factory)
	if err != nil {
		return err
	}
	if c.conn != nil {
		c.conn.Close()
	}
	if c.connPool != nil {
		c.connPool.Close()
	}
	c.connPool = p
	c.conn, err = p.Get()
	if err != nil {
		return err
	}

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
	connection := c.conn

	_, err := connection.Write(message)
	if err != nil {
		return c.fatal(err)
	}

	return nil
}

func (c *Connection) sendWithResponse(message []byte) (*OpResponse, error) {
	err := c.send(message)
	if err != nil {
		return nil, err
	}
	return c.receive()
}
