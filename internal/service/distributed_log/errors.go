package distributedlog

// NotLeaderError is returned when a write is attempted on a non-leader node.
// LeaderAddr contains the current leader's address for client redirection.
type NotLeaderError struct {
	LeaderAddr string
}

func (e *NotLeaderError) Error() string {
	return "not leader; leader at " + e.LeaderAddr
}
