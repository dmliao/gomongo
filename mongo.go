package gomongo

import (
	"github.com/dmliao/gomongo/convert"
	"gopkg.in/mgo.v2/bson"
)

type Mongo interface {
	GetDB(string) Database
	//GetDBNameList() []string

	Close() error
	Error() error
}

type MongoDB struct {
	conn      *Connection
	servers   map[string]*Connection
	master    *Connection
	requestID int32
	err       error
}

func (m *MongoDB) connectToSeed(seed string) error {
	connection := &Connection{
		address: seed,
	}

	connection.connect()

	db := &DB{
		name:  "admin",
		mongo: m,
	}
	var result bson.M
	db.run(connection, bson.M{"isMaster": 1}, &result)

	meRaw, ok := result["me"]
	me := seed
	if ok {
		me = convert.ToString(meRaw)
		_, ok := m.servers[me]
		if ok {
			connection.Close()
			return nil
		}
	}

	isMaster := convert.ToBool(result["ismaster"])
	if isMaster {
		m.master = connection
		m.conn = connection
	}

	m.servers[me] = connection

	hostsRaw, ok := result["hosts"]
	if ok {
		hosts, err := convert.ConvertToStringSlice(hostsRaw)
		if err != nil {
			return err
		}
		for i := 0; i < len(hosts); i++ {
			m.connectToSeed(hosts[i])
		}
	}

	return nil

}

func (m *MongoDB) connect(seed string) error {
	return m.connectToSeed(seed)
}

func (m *MongoDB) nextID() int32 {
	m.requestID += 1
	return m.requestID
}

func (m *MongoDB) GetDB(dName string) Database {
	return &DB{
		name:  dName,
		mongo: m,
	}
}

func (m *MongoDB) Close() error {
	return nil
}

func (m *MongoDB) Error() error {
	return nil
}

// checks if a mongo's connection is alive, if not, spins up a new
// connection from the pool.
func (m *MongoDB) checkAlive() error {

	// ping the server
	c := m.conn

	admin := m.GetDB("admin")
	var result bson.M
	err := admin.ExecuteCommand(bson.M{"isMaster": 1}, &result)
	if err != nil {
		var err2 error
		c.conn, err2 = c.connPool.Get()
		if err2 != nil {
			return err2
		}
	}
	return nil

}
