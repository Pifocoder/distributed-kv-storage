package cluster

type registerResponse struct {
	ID string `json:"id"`
}

type heartbeatRequest struct {
	ID string `json:"id"`
}

type nodeDTO struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

type heartbeatResponse struct {
	ActiveNodes []nodeDTO `json:"active_nodes"`
}

type NodeInfo struct {
	ID   string
	Addr string
}
