package gomongo

type Mongo interface {
	GetDB(string) Database
	//GetDBNameList() []string

	Close() error
	Error() error
}

type MongoDB struct {
	conn *Connection
	err  error
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
