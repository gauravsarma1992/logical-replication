package replication

import (
	"context"
	"fmt"
	"sync"

	"errors"
)

var (
	ErrTransportClientNotFoundError = errors.New("transport client not found")
	NodeNotPresentInTransportError  = errors.New("node is not present in the transport manager")
)

type (
	TransportManager struct {
		ctx context.Context

		addrToNodeStore map[string]NodeID
		nodeToAddrStore map[NodeID]*NodeAddr

		localTransport Transport

		msgHandlers map[MessageTypeT]MessageHandler

		clientLock *sync.RWMutex
		replMgr    *ReplicationManager
	}
)

func NewTransportManager(ctx context.Context) (transportManager *TransportManager, err error) {
	transportManager = &TransportManager{
		ctx:             ctx,
		addrToNodeStore: make(map[string]NodeID, 10),
		nodeToAddrStore: make(map[NodeID]*NodeAddr, 10),
		msgHandlers:     make(map[MessageTypeT]MessageHandler, 10),
		clientLock:      &sync.RWMutex{},
	}
	transportManager.replMgr = ctx.Value(ReplicationManagerInContext).(*ReplicationManager)
	if transportManager.localTransport, err = NewTransport(ctx, transportManager.replMgr.localNode); err != nil {
		return
	}
	if err = transportManager.setupMsgHandlers(); err != nil {
		return
	}
	return
}

func (transportManager *TransportManager) setupMsgHandlers() (err error) {
	transportManager.msgHandlers[PingMessageType] = transportManager.replMgr.bootstrapMgr.PingHandler
	transportManager.msgHandlers[ClusterDiscoveryMessageType] = transportManager.replMgr.bootstrapMgr.ClusterDiscoveryHandler
	transportManager.msgHandlers[HeartbeatMessageType] = transportManager.replMgr.hbMgr.HeartbeatHandler
	transportManager.msgHandlers[DataReplicationPushMessageType] = transportManager.replMgr.drMgr.DataReplicationPushHandler
	transportManager.msgHandlers[DataReplicationPullMessageType] = transportManager.replMgr.drMgr.DataReplicationPullHandler
	return
}

func (transportManager *TransportManager) ConvertIpToNode(remoteAddr *NodeAddr) (nodeID NodeID, err error) {
	isPresent := true

	transportManager.clientLock.RLock()
	defer transportManager.clientLock.RUnlock()

	if nodeID, isPresent = transportManager.addrToNodeStore[remoteAddr.String()]; isPresent {
		return
	}
	return
}

func (transportManager *TransportManager) ConvertNodeToAddr(remoteNodeID NodeID) (remoteAddr *NodeAddr, err error) {
	isPresent := true

	transportManager.clientLock.RLock()
	defer transportManager.clientLock.RUnlock()

	if remoteAddr, isPresent = transportManager.nodeToAddrStore[remoteNodeID]; isPresent {
		err = fmt.Errorf("node is not present in the transport manager %d", remoteNodeID)
		return
	}
	return
}

func (transportManager *TransportManager) ConnectToNode(remoteAddr *NodeAddr) (remoteNode *Node, err error) {
	var (
		pingRespMsg *Message
		pingResp    *PingResponse
	)
	if pingRespMsg, err = transportManager.localTransport.Ping(remoteAddr); err != nil {
		return
	}
	pingResp = &PingResponse{}
	if err = pingRespMsg.FillValue(pingResp); err != nil {
		return
	}
	remoteNode = pingResp.Node

	if err = transportManager.UpdateTransportMappings(remoteNode, remoteAddr); err != nil {
		return
	}
	return
}

func (transportManager *TransportManager) UpdateTransportMappings(node *Node, remoteAddr *NodeAddr) (err error) {
	transportManager.clientLock.Lock()
	defer transportManager.clientLock.Unlock()

	transportManager.addrToNodeStore[remoteAddr.String()] = node.ID
	transportManager.nodeToAddrStore[node.ID] = remoteAddr
	return
}

func (transportManager *TransportManager) Send(reqMsg *Message) (respMsg *Message, err error) {
	if respMsg, err = transportManager.localTransport.Send(reqMsg); err != nil {
		return
	}
	return
}
