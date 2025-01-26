package tempwal

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/gauravsarma1992/logical-replication/replication"
)

type (
	LogTempWAL struct {
		ctx      context.Context
		currIdx  replication.LogIndex
		startIdx replication.LogIndex
		walLogs  []replication.WALLog
	}
)

func NewLogTempWAL(ctx context.Context) (logWal *LogTempWAL, err error) {
	logWal = &LogTempWAL{
		ctx:     ctx,
		walLogs: make([]replication.WALLog, 0),
	}
	go logWal.PushData()
	return
}

func (logWal *LogTempWAL) GetLogsAfterIndex(index replication.LogIndex, limit int) (logs []replication.WALLog, err error) {
	lastLogIdx := replication.LogIndex(0)
	for idx := replication.LogIndex(index); idx < logWal.currIdx; idx++ {
		log := logWal.walLogs[idx]
		if log.Index > index {
			logs = append(logs, log)
		}
		if len(logs) == limit {
			break
		}
		lastLogIdx = log.Index
	}
	logWal.startIdx = lastLogIdx
	return
}

func (logWal *LogTempWAL) Commit(logs []replication.WALLog) (err error) {
	return
}

func (logWal *LogTempWAL) GetSnapshot() (snapshot []byte, err error) {
	return
}

func (logWal *LogTempWAL) PushData() (err error) {
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-ticker.C:
			// Generate random log entries
			logs := make([]replication.WALLog, 0)
			for i := 0; i < 10; i++ {
				logs = append(logs, replication.WALLog{
					Index: logWal.currIdx,
					Value: []byte("Log content " + strconv.Itoa(int(logWal.currIdx))),
				})
				logWal.currIdx += 1
			}
			log.Println("Pushing data to WAL")
			logWal.walLogs = append(logWal.walLogs, logs...)
		}

	}

	return
}
