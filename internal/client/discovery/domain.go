package discovery

type EventType int

const (
	Join EventType = iota
	Leave
)

type MemberEvent struct {
	EventType EventType
	ID        string
	Addr      string
}

type Member struct {
	ID   string
	Addr string
}
