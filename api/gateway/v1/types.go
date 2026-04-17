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
