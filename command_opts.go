package gomongo

type FindOpts struct {
	Sort            interface{}
	Projection      interface{}
	Skip            int32
	Limit           int32
	BatchSize       int32
	Tailable        bool
	OplogReplay     bool
	NoCursorTimeout bool
	AwaitData       bool
	Partial         bool
}

type UpdateOpts struct {
	Multi bool
}

type RemoveOpts struct {
	Multi bool
}
