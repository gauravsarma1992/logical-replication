package replication

type (
	LogIndex uint64

	WALLog struct {
		Index LogIndex `json:"index"`
		Value []byte   `json:"value"`
	}

	ReplicationWAL interface {
		GetLogsAfterIndex(LogIndex, int) ([]WALLog, error)
		Commit([]WALLog) error
		GetSnapshot() ([]byte, error)
	}
)
