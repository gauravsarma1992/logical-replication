package replication

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

const (
	// Node phases
	BootstrapNodePhase       NodePhaseT = NodePhaseT(0)
	DataReadinessNodePhase   NodePhaseT = NodePhaseT(1)
	DataReplicationNodePhase NodePhaseT = NodePhaseT(2)

	// Node state or health
	NodeStateHealthy   NodeStateT = NodeStateT(0)
	NodeStateUnhealthy NodeStateT = NodeStateT(1)
	NodeStateUnknown   NodeStateT = NodeStateT(2)

	// Node types
	NodeTypeLeader   NodeTypeT = NodeTypeT(0)
	NodeTypeFollower NodeTypeT = NodeTypeT(1)
	NodeTypeHidden   NodeTypeT = NodeTypeT(2)
)

var (
	SameNodeError = fmt.Errorf("errored because remote node is the same as the local node")
)

type (
	NodeID     uint64
	NodePhaseT uint8
	NodeStateT uint8
	NodeTypeT  uint8

	Node struct {
		ctx context.Context
		ID  NodeID

		// Runtime information of the State, phase and type of the node.
		State    NodeStateT `json:"state"`
		Phase    NodePhaseT `json:"phase"`
		NodeType NodeTypeT  `json:"node_type"`

		replMgr *ReplicationManager
		Config  *NodeConfig

		nodeLock *sync.RWMutex `json:"-"`

		LastUpdatedAt time.Time `json:"last_updated_at"`
	}
	NodeAddr struct {
		Host string `json:"host"`
		Port string `json:"port"`
	}

	NodeConfig struct {
		Local *NodeAddr `json:"local"`

		// Remote host is the host of the node that we are replicating from.
		// If the remote host is the same as the local host or if the remote host
		// is not provided, then the node is a leader node or a single node.
		// The remote node doesn't have to be the leader. It can recursively learn
		// about the leader from the remote node.
		Remote *NodeAddr `json:"remote"`
	}
)

func (nodeType NodeTypeT) String() string {
	switch nodeType {
	case NodeTypeLeader:
		return "Leader"
	case NodeTypeFollower:
		return "Follower"
	case NodeTypeHidden:
		return "Hidden"
	default:
		return "Unknown"
	}
}

func (state NodeStateT) String() string {
	switch state {
	case NodeStateHealthy:
		return "Healthy"
	case NodeStateUnhealthy:
		return "Unhealthy"
	default:
		return "Unknown"
	}
}

func (node *Node) String() string {
	return fmt.Sprintf(
		"Node ID: %d, State: %d, Phase: %d, Type: %d, Config: %s",
		node.ID, node.State, node.Phase, node.NodeType, node.Config,
	)
}

func (addr *NodeAddr) String() string {
	return fmt.Sprintf("%s-%s", addr.Host, addr.Port)
}

func (addr NodeAddr) FromString(addrStr string) *NodeAddr {
	addrParts := strings.Split(addrStr, "-")
	return &NodeAddr{
		Host: addrParts[0],
		Port: addrParts[1],
	}
}

func (nodeConfig *NodeConfig) String() string {
	return fmt.Sprintf(
		"NodeConfig Local Host: %s, Local Port: %s, Remote Host: %s, Remote Port: %s",
		nodeConfig.Local.Host,
		nodeConfig.Local.Port,
		nodeConfig.Remote.Host,
		nodeConfig.Remote.Port,
	)
}

func DefaultSingleNodeConfig() (config *NodeConfig) {
	config = &NodeConfig{
		Local: &NodeAddr{
			Host: "127.0.0.1",
			Port: "8080",
		},
		Remote: &NodeAddr{},
	}
	return
}

func NewNode(ctx context.Context, config *NodeConfig) (node *Node, err error) {
	if config == nil {
		config = DefaultSingleNodeConfig()
	}
	node = &Node{
		ID:            NodeID(time.Now().UnixNano()),
		ctx:           ctx,
		Config:        config,
		State:         NodeStateUnknown,
		Phase:         BootstrapNodePhase,
		NodeType:      NodeTypeHidden,
		nodeLock:      &sync.RWMutex{},
		LastUpdatedAt: time.Now().UTC(),
	}
	node.replMgr = ctx.Value(ReplicationManagerInContext).(*ReplicationManager)
	return
}

func (node *Node) verifyConfig() (err error) {
	// check if local network config is correct
	if node.Config.Local.Host == "" || node.Config.Local.Port == "" {
		err = fmt.Errorf("local host or port is not provided for node %d", node.ID)
		return
	}
	// check if remote network config is correct
	if node.Config.Remote.Host == "" {
		err = fmt.Errorf("remote host is not provided for node %d", node.ID)
		return
	}
	// TODO: check current server resources
	return
}

func (node *Node) Boot() (err error) {
	// Start the node
	node.replMgr.log.Println("Node is booting. Config:", node.ID, node.Config)
	if err = node.verifyConfig(); err != nil {
		return
	}
	return
}

func (node *Node) GetLocalUser() (msgUser *MessageUser) {
	msgUser = &MessageUser{
		NodeID: node.ID,
		Addr:   node.Config.Local,
	}
	return
}

func (node *Node) ConnectToRemoteNode() (remoteNode *Node, err error) {
	if remoteNode, err = node.replMgr.transportMgr.ConnectToNode(node.Config.Remote); err != nil {
		return
	}
	return
}

func (node *Node) UpdateState(state NodeStateT) {
	node.nodeLock.Lock()
	defer node.nodeLock.Unlock()

	node.State = state
	node.LastUpdatedAt = time.Now().UTC()
}

func (node *Node) UpdateNodeType(nodeType NodeTypeT) {
	node.nodeLock.Lock()
	defer node.nodeLock.Unlock()

	node.NodeType = nodeType
	node.LastUpdatedAt = time.Now().UTC()

	node.replMgr.log.Println("Node type updated to", nodeType)
}

func (node *Node) Activate() (err error) {
	// Update the node's type
	if node.Config.Remote.String() == node.Config.Local.String() {
		node.UpdateNodeType(NodeTypeLeader)
	} else {
		node.UpdateNodeType(NodeTypeFollower)
	}

	// Update the node's status
	node.UpdateState(NodeStateHealthy)

	return
}

func (node *Node) Stop() (err error) {
	// Stop the node
	return
}
