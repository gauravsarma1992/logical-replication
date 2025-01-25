package replication

import (
	"context"
	"fmt"
	"log"
	"os"
)

const (
	ReplicationManagerInContext = "replication-manager-in-context"
)

type (
	ReplicationManager struct {
		ctx        context.Context
		cancelFunc context.CancelFunc

		localNode *Node
		cluster   *Cluster

		transportMgr *TransportManager
		bootstrapMgr *BootstrapManager
		electionMgr  *ElectionManager
		hbMgr        *HeartbeatManager
		drMgr        *DataReplicationManager
		healthMgr    *HealthManager
		config       *ReplicationConfig

		log *log.Logger
	}

	ReplicationConfig struct {
		NodeConfig              *NodeConfig
		HearbeatIntervalInSecs  int `json:"heartbeat_interval_in_secs"`
		BootstrapIntervalInSecs int `json:"bootstrap_interval_in_secs"`
	}
)

func NewReplicationManager(ctx context.Context, config *ReplicationConfig, wal ReplicationWAL) (replMgr *ReplicationManager, err error) {
	replMgr = &ReplicationManager{
		config: config,
	}

	replMgr.ctx, replMgr.cancelFunc = context.WithCancel(ctx)
	replMgr.ctx = context.WithValue(ctx, ReplicationManagerInContext, replMgr)

	if replMgr.bootstrapMgr, err = NewBootstrapManager(replMgr.ctx); err != nil {
		return
	}
	if replMgr.hbMgr, err = NewHeartbeatManager(replMgr.ctx); err != nil {
		return
	}
	if replMgr.healthMgr, err = NewHealthManager(replMgr.ctx); err != nil {
		return
	}
	if replMgr.drMgr, err = NewDataReplicationManager(replMgr.ctx, wal); err != nil {
		return
	}
	if replMgr.transportMgr, err = NewTransportManager(replMgr.ctx); err != nil {
		return
	}
	if replMgr.electionMgr, err = NewElectionManager(replMgr.ctx); err != nil {
		return
	}
	if replMgr.cluster, err = NewCluster(replMgr.ctx); err != nil {
		return
	}
	if replMgr.log = log.New(os.Stderr, fmt.Sprintf("[%d]: ", replMgr.localNode.ID), log.LstdFlags); err != nil {
		return
	}
	return
}

func (replMgr *ReplicationManager) StartBootstrapPhase() (err error) {
	if err = replMgr.bootstrapMgr.Start(); err != nil {
		return
	}
	//replMgr.log.Println(
	//	"Bootstrap phase completed. Local node - ",
	//	replMgr.localNode,
	//	"Discovered nodes -", replMgr.cluster.GetRemoteNodes(),
	//)
	return
}

func (replMgr *ReplicationManager) StartHeartbeats() (err error) {
	//replMgr.log.Println("Heartbeats phase started for NodeID -", replMgr.localNode.ID)
	go replMgr.hbMgr.Start()
	return
}

func (replMgr *ReplicationManager) StartHealthManagerPhase() (err error) {
	go replMgr.healthMgr.Start()
	return
}

func (replMgr *ReplicationManager) StartDataReplicationPhase() (err error) {
	//replMgr.log.Println("Data replication phase started for NodeID -", replMgr.localNode.ID)
	if err = replMgr.drMgr.Start(); err != nil {
		return
	}
	return
}

func (replMgr *ReplicationManager) StartElectionManager() (err error) {
	//replMgr.log.Println("Election manager started for NodeID -", replMgr.localNode.ID)
	if err = replMgr.electionMgr.Start(); err != nil {
		return
	}
	return
}

func (replMgr *ReplicationManager) Run() (err error) {
	if err = replMgr.StartBootstrapPhase(); err != nil {
		replMgr.log.Println("Error in bootstrap phase", err)
		return
	}
	if err = replMgr.StartHealthManagerPhase(); err != nil {
		replMgr.log.Println("Error in health manager phase", err)
		return
	}
	if err = replMgr.StartHeartbeats(); err != nil {
		replMgr.log.Println("Error in heartbeats phase", err)
		return
	}
	if err = replMgr.StartDataReplicationPhase(); err != nil {
		replMgr.log.Println("Error in data replication phase", err)
		return
	}
	if err = replMgr.StartElectionManager(); err != nil {
		replMgr.log.Println("Error in election manager phase", err)
		return
	}
	for {
		select {
		case <-replMgr.ctx.Done():
			return
		}
	}
	replMgr.log.Println("Shutting down replication manager")
	return
}
