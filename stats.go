package main

type statItem struct {
	key, val string
}

type Stats struct {
    Items uint64

	Ops uint64
	Gets uint64
	GetMisses uint64
	Sets uint64
	Deletes uint64
	Creates uint64
	Updates uint64
	RGets uint64
	RGetResults uint64

	IncomingValueBytes uint64
	OutgoingValueBytes uint64

	ErrNotMyRange uint64
}
