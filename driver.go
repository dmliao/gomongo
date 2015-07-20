package gomongo

import (
	"strings"
)

func Connect(address string) (*Connection, error) {
	if strings.LastIndex(address, ":") <= strings.LastIndex(address, "]") {
		address = address + ":27017"
	}
	c := Connection{
		address: address,
		cursors: make(map[int64]*cursorObj),
	}
	return &c, c.connect()
}
