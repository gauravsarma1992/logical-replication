package replication

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

const ErrNoLeaderNode = "no leader node found"

type (
	Cluster struct {
		ctx context.Context

		leaderNode *Node
		nodes      map[NodeID]*Node

		clusterLock            *sync.RWMutex
		clusterEventCh         chan *ClusterNodeAddedOrRemovedEvent
		clusterNoLeaderEventCh chan bool

		replMgr *ReplicationManager

		lastLeaderChangedAt time.Time
		lastNodeAddedAt     time.Time

		createdAt time.Time
	}
	ClusterNodeAddedOrRemovedEvent struct {
		Node  *Node
		Added bool
	}
)

func NewCluster(ctx context.Context) (cluster *Cluster, err error) {
	cluster = &Cluster{
		ctx:                    ctx,
		clusterLock:            &sync.RWMutex{},
		nodes:                  make(map[NodeID]*Node, 10),
		clusterEventCh:         make(chan *ClusterNodeAddedOrRemovedEvent, 100),
		clusterNoLeaderEventCh: make(chan bool, 10),
		createdAt:              time.Now().UTC(),
	}
	cluster.replMgr = ctx.Value(ReplicationManagerInContext).(*ReplicationManager)
	return
}

func (cluster *Cluster) AddNode(node *Node) (err error) {
	cluster.clusterLock.Lock()
	defer cluster.clusterLock.Unlock()

	// Skip if the node is the local node
	if node.ID == cluster.replMgr.localNode.ID {
		return
	}
	// Log if the node is not present in the cluster
	if localNodeCopy, isPresent := cluster.nodes[node.ID]; isPresent {
		if localNodeCopy.LastUpdatedAt.Equal(node.LastUpdatedAt) || localNodeCopy.LastUpdatedAt.After(node.LastUpdatedAt) {
			return
		}
	}
	//cluster.replMgr.log.Println("Adding node to cluster", node)
	cluster.nodes[node.ID] = node
	cluster.lastNodeAddedAt = node.LastUpdatedAt.Add(1 * time.Second)

	cluster.clusterEventCh <- &ClusterNodeAddedOrRemovedEvent{
		Node:  node,
		Added: true,
	}

	return
}

func (cluster *Cluster) GetLeaderNode() (node *Node, err error) {
	cluster.clusterLock.RLock()
	defer cluster.clusterLock.RUnlock()

	node = cluster.leaderNode
	if node == nil {
		err = fmt.Errorf(ErrNoLeaderNode)
	}
	return
}

func (cluster *Cluster) AddLeaderNode(leaderNode *Node) (err error) {
	cluster.clusterLock.Lock()
	defer cluster.clusterLock.Unlock()

	if cluster.leaderNode != nil && leaderNode.ID == cluster.leaderNode.ID {
		return
	}

	cluster.leaderNode = leaderNode
	cluster.lastLeaderChangedAt = time.Now().UTC()
	cluster.replMgr.log.Println("Leader node added to the cluster", leaderNode)

	return
}

func (cluster *Cluster) GetRemoteNodes() (nodes []*Node) {
	cluster.clusterLock.RLock()
	defer cluster.clusterLock.RUnlock()

	for _, node := range cluster.nodes {
		if node.ID == cluster.replMgr.localNode.ID {
			continue
		}
		nodes = append(nodes, node)
	}
	return
}

func (cluster *Cluster) GetActiveRemoteNodes() (nodes []*Node) {
	cluster.clusterLock.RLock()
	defer cluster.clusterLock.RUnlock()

	for _, node := range cluster.nodes {
		if node.ID == cluster.replMgr.localNode.ID || node.State != NodeStateHealthy {
			continue
		}
		nodes = append(nodes, node)
	}
	return
}

func (cluster *Cluster) GetNodes() (nodes []*Node) {
	cluster.clusterLock.RLock()
	defer cluster.clusterLock.RUnlock()

	for _, node := range cluster.nodes {
		nodes = append(nodes, node)
	}
	return
}

func (cluster *Cluster) RemoveNode(nodeID NodeID) (err error) {
	var (
		node *Node
	)

	cluster.clusterLock.Lock()
	defer cluster.clusterLock.Unlock()

	node = cluster.nodes[nodeID]

	// Skip if the node is the local node
	if node.ID == cluster.replMgr.localNode.ID {
		return
	}
	cluster.nodes[node.ID] = node
	delete(cluster.nodes, node.ID)

	cluster.clusterEventCh <- &ClusterNodeAddedOrRemovedEvent{
		Node:  node,
		Added: false,
	}

	return
}

func (cluster *Cluster) Update(receivedNodes []*Node) (err error) {
	// TODO: Remove deleted nodes
	for _, node := range receivedNodes {
		if err = cluster.AddNode(node); err != nil {
			log.Printf("unable to add node %d to the cluster", node.ID)
			continue
		}
		//if err = cluster.AddLeaderNode(node); err != nil {
		//	log.Printf("unable to add leader node %d to the cluster", node.ID)
		//	continue
		//}

	}
	return
}

func (cluster *Cluster) startClusterPoller() (err error) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-cluster.ctx.Done():
			return
		case <-ticker.C:
			var (
				leaderNode *Node
			)
			if leaderNode, err = cluster.GetLeaderNode(); leaderNode != nil {
				continue
			}
			if time.Since(cluster.createdAt) < 25*time.Second || time.Since(cluster.lastLeaderChangedAt) < 25*time.Second {
				continue
			}
			cluster.clusterNoLeaderEventCh <- true
			cluster.replMgr.log.Println("No leader node found in the cluster")
		}
	}
}

func (cluster *Cluster) Start() (err error) {
	go cluster.startClusterPoller()
	return
}
