package gateway

type ProduceRequest struct {
	Value string `json:"value"`
}

type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

type ConsumeResponse struct {
	Offset uint64 `json:"offset"`
	Value  string `json:"value"`
}

type StreamRequest struct {
	From uint64 `json:"from"`
}

type GetServersResponse struct {
	Servers []ServerResponse `json:"servers"`
}

type ServerResponse struct {
	ID       string `json:"id"`
	RpcAddr  string `json:"rpc_addr"`
	IsLeader bool   `json:"is_leader"`
}
