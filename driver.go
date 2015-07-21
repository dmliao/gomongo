package gomongo

import (
	"strings"
)

func Connect(address string) (Mongo, error) {
	if strings.LastIndex(address, ":") <= strings.LastIndex(address, "]") {
		address = address + ":27017"
	}
	c := Connection{
		address: address,
	}

	m := MongoDB{
		conn: &c,
	}
	return &m, c.connect()
}
