package gomongo

type MsgHeader struct {
	MessageLength int32 // total message size, including this
	RequestID     int32 // identifier for this message
	ResponseTo    int32 // requestID from the original request
	//   (used in reponses from db)
	OpCode int32 // request type - see table below
}

type OpQuery struct {
	Flags              int32  // bit vector of query options.  See below for details.
	FullCollectionName string // "dbname.collectionname"
	NumberToSkip       int32  // number of documents to skip
	NumberToReturn     int32  // number of documents to return
	Query              interface{}
	Projection         interface{}
}

type OpGetMore struct {
	Reserved           int32  // 0 - reserved for future use
	FullCollectionName string // "dbname.collectionname"
	NumberToReturn     int32  // number of documents to return
	CursorID           int64  // cursorID from the OP_REPLY
}

type OpResponse struct {
	Header         MsgHeader // standard message header
	ResponseFlags  int32     // bit vector - see details below
	CursorID       int64     // cursor id if client needs to do get more's
	StartingFrom   int32     // where in the cursor this reply is starting
	NumberReturned int32     // number of documents in the reply
	Document       [][]byte  // documents
}
