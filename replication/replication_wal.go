package replication

type (
	LogIndex uint64

	WALLog struct {
		Index LogIndex `json:"index"`
		Value []byte   `json:"value"`
	}

	ReplicationWAL interface {
		GetLogsAfterIndex(LogIndex, int) ([]WALLog, error)
		ApplyLogs([]WALLog) error
		GetSnapshot() ([]byte, error)
	}
)
