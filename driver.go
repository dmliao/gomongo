package gomongo

import (
	"strings"
)

func Connect(address string) (Mongo, error) {
	if strings.LastIndex(address, ":") <= strings.LastIndex(address, "]") {
		address = address + ":27017"
	}

	m := MongoDB{
		servers: make(map[string]*Connection),
	}
	return &m, m.connect(address)
}
