package gomongo

type MongoError struct {
	message string
	code    int32
}

// implement error
func (m MongoError) Error() string {
	return m.message
}

type WriteError struct {
	Index  int32
	Code   int32
	ErrMsg string
}

type WriteErrors struct {
	Errors []WriteError
}

func (i WriteErrors) Error() string {
	return "Insert write errors!"
}

type WriteConcernError struct {
	Code   int32
	ErrMsg string
}

func (w WriteConcernError) Error() string {
	return "Write concern error with message: " + w.ErrMsg
}
