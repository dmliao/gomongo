package gomongo

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/dmliao/gomongo/buffer"
	"io"
)

func (c *Connection) receive() (*OpResponse, error) {
	connection := c.conn

	// Read the first 16 bytes for the message header
	response := OpResponse{}
	messageHeader := make([]byte, 16)
	n, err := connection.Read(messageHeader)
	if err != nil {
		if err != io.EOF {
			fmt.Printf("error reading from connection: %v\n", err)
		}
		fmt.Printf("client %v closed connection\n", connection.RemoteAddr())
		return nil, err
	}
	if n == 0 {
		fmt.Println("connection closed")
		return nil, io.EOF
	}
	msgHeader := MsgHeader{}
	err = binary.Read(bytes.NewReader(messageHeader), binary.LittleEndian, &msgHeader)
	if err != nil {
		fmt.Printf("error decoding from reader: %v\n", err)
		return nil, err
	}
	response.Header = msgHeader
	response.ResponseFlags, err = buffer.ReadInt32LE(connection)
	if err != nil {
		return nil, err
	}

	response.CursorID, err = buffer.ReadInt64LE(connection)
	if err != nil {
		return nil, err
	}
	response.StartingFrom, err = buffer.ReadInt32LE(connection)
	if err != nil {
		return nil, err
	}
	response.NumberReturned, err = buffer.ReadInt32LE(connection)
	if err != nil {
		return nil, err
	}
	for i := int32(0); i < response.NumberReturned; i++ {
		_, doc, err := buffer.ReadDocumentRaw(connection)
		if err != nil {
			return nil, err
		}
		response.Document = append(response.Document, doc)
	}

	return &response, nil
}
func receiveFindResponse(res *OpResponse, cursor *cursorObj) error {

	cursor.docs = res.Document
	cursor.cursorID = res.CursorID
	return nil
}
