package main

import (
	"context"
	"strconv"
	"time"

	"github.com/gauravsarma1992/logical-replication/replication"
	"github.com/gauravsarma1992/logical-replication/replication/testsuite/tempwal"
)

const (
	StartingPort = 9000
)

type (
	ReplicationTestSuite struct {
		ctx        context.Context
		cancelFunc context.CancelFunc
		wal        replication.ReplicationWAL

		config *TestConfig

		replMgrs []*replication.ReplicationManager
	}
	TestConfig struct {
		NoOfNodes uint8
	}
)

func DefaultTestConfig() *TestConfig {
	return &TestConfig{
		NoOfNodes: 3,
	}
}

func DefaultWAL(ctx context.Context) (wal replication.ReplicationWAL) {
	wal, _ = tempwal.NewLogTempWAL(ctx)
	return
}

func NewReplicationTestSuite(config *TestConfig) (replSuite *ReplicationTestSuite, err error) {
	if config == nil {
		config = DefaultTestConfig()
	}
	ctx, cancelFunc := context.WithCancel(context.Background())

	replSuite = &ReplicationTestSuite{
		ctx:        ctx,
		cancelFunc: cancelFunc,
		config:     config,
		wal:        DefaultWAL(ctx),
	}
	return
}

func (replSuite *ReplicationTestSuite) createAndRunNodes() (err error) {
	for nodeCount := 0; nodeCount < int(replSuite.config.NoOfNodes); nodeCount++ {
		var (
			replMgr    *replication.ReplicationManager
			nodeConfig *replication.NodeConfig
			replConfig *replication.ReplicationConfig
		)
		nodeConfig = replication.DefaultSingleNodeConfig()
		nodeConfig.Local.Port = strconv.Itoa(StartingPort + nodeCount)
		nodeConfig.Remote.Host = nodeConfig.Local.Host
		nodeConfig.Remote.Port = strconv.Itoa(StartingPort)

		replConfig = &replication.ReplicationConfig{
			NodeConfig: nodeConfig,
		}

		if replMgr, err = replication.NewReplicationManager(replSuite.ctx,
			replConfig, replSuite.wal); err != nil {

			return
		}
		go func() {
			replMgr.Run()
		}()

		time.Sleep(2 * time.Second)
	}
	return
}

func (replSuite *ReplicationTestSuite) Start() (err error) {
	// Create and start the nodes
	if err = replSuite.createAndRunNodes(); err != nil {
		return
	}
	for {
		select {
		case <-replSuite.ctx.Done():
			return
		}
	}
	return
}

func main() {
	var (
		replSuite *ReplicationTestSuite
		err       error
	)
	if replSuite, err = NewReplicationTestSuite(nil); err != nil {
		return
	}
	if err = replSuite.Start(); err != nil {
		return
	}
}
