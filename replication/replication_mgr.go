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
	if replMgr.electionMgr, err = NewElectionManager(replMgr.ctx); err != nil {
		return
	}
	if replMgr.transportMgr, err = NewTransportManager(replMgr.ctx); err != nil {
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

func (replMgr *ReplicationManager) StartCluster() (err error) {
	if err = replMgr.cluster.Start(); err != nil {
		return
	}
	return
}

func (replMgr *ReplicationManager) StartBootstrapPhase() (err error) {
	if err = replMgr.bootstrapMgr.Start(); err != nil {
		return
	}
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
	/*
		This process lists the order in which various managers are started.
		The order is important as the managers are dependent on each other.
		Before defining the order, we need to understand the dependency between the managers.
		For example, an empty cluster can be created, but the leader not present check should not
		start until the bootstrap manager has completed its steps of connecting to the remote node,
		fetching the first election and cluster details, and then starts the node.

		The bootstrap manager usually updates the cluster details and their election details as well. It doesn't need
		the cluster to actuall start.

		The bootstrap manager should ensure that the transport manager is up and running before it starts.
		However, transport manager doesn't behave like a daemon and provides support for transport functions.
		Hence, it doesn't have to started.

		Heartbeats should not start until the node has been bootstrapped. But apart from that, it shouldn't
		have any dependency on any other manager. It doesn't matter if the cluster has a leader, etc.
		Health tracking is another manager that can start after the bootstrap manager has completed its steps.
		In case the health tracking is triggered before the election manager has started, it should not be a problem.
		It can just wait blocked on the filled channel before the election manager comes up.

		Data replication phase would start with copying the snapshot data if there is a lag.
		Data replication phase should wait till everything has been settled. It should wait for the cluster to be filled
		with nodes, for the election to be decided as it won't know its role till then, etc.

	*/

	if err = replMgr.StartBootstrapPhase(); err != nil {
		replMgr.log.Println("Error in bootstrap phase", err)
		return
	}
	if err = replMgr.StartCluster(); err != nil {
		replMgr.log.Println("Error in cluster phase", err)
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
	if err = replMgr.StartElectionManager(); err != nil {
		replMgr.log.Println("Error in election manager phase", err)
		return
	}
	if err = replMgr.StartDataReplicationPhase(); err != nil {
		replMgr.log.Println("Error in data replication phase", err)
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
