package replication

import (
	"context"
	"sync"
	"time"
)

type (
	HeartbeatManager struct {
		ctx     context.Context
		replMgr *ReplicationManager

		heartbeatTracker map[NodeID]time.Time
		hbChan           chan *HeartbeatNotAckEvent
		hbLock           *sync.RWMutex
	}
	HeartbeatRequestMsg struct {
		Node *Node `json:"node"`
	}
	HeartbeatResponseMsg struct {
		NodeID NodeID `json:"node_id"`
	}

	HeartbeatNotAckEvent struct {
		NodeID      NodeID
		TimeElapsed time.Duration
	}
)

func NewHeartbeatManager(ctx context.Context) (hbMgr *HeartbeatManager, err error) {
	hbMgr = &HeartbeatManager{
		ctx:              ctx,
		heartbeatTracker: make(map[NodeID]time.Time, 10),
		hbLock:           &sync.RWMutex{},
		hbChan:           make(chan *HeartbeatNotAckEvent, 10),
	}
	hbMgr.replMgr = ctx.Value(ReplicationManagerInContext).(*ReplicationManager)
	return
}

func (hbMgr *HeartbeatManager) HeartbeatHandler(reqMsg *Message) (respMsg *Message, err error) {
	var (
		heartbeatReqMsg *HeartbeatRequestMsg
	)
	heartbeatReqMsg = &HeartbeatRequestMsg{}
	if err = reqMsg.FillValue(heartbeatReqMsg); err != nil {
		return
	}
	if err = hbMgr.replMgr.cluster.Update([]*Node{heartbeatReqMsg.Node}); err != nil {
		return
	}

	respMsg = NewMessage(
		InfoMessageGroup,
		HeartbeatMessageType,
		reqMsg.Remote,
		reqMsg.Local,
		&HeartbeatResponseMsg{NodeID: reqMsg.Remote.NodeID},
	)
	return
}

func (hbMgr *HeartbeatManager) sendHeartbeat() (err error) {
	// hbMgr.replMgr.log.Println("Sending heartbeat from node", hbMgr.replMgr.localNode, "to nodes - ", len(nodes))
	hbMgr.hbLock.Lock()
	defer hbMgr.hbLock.Unlock()

	for _, node := range hbMgr.replMgr.cluster.GetRemoteNodes() {
		if node.ID == hbMgr.replMgr.localNode.ID {
			hbMgr.replMgr.log.Println("Skipping heartbeat to local node", node)
			continue
		}
		heartbeatReqMsg := NewMessage(
			InfoMessageGroup,
			HeartbeatMessageType,
			hbMgr.replMgr.localNode.GetLocalUser(),
			node.GetLocalUser(),
			&HeartbeatRequestMsg{
				Node: hbMgr.replMgr.localNode,
			},
		)
		if _, err = hbMgr.replMgr.transportMgr.Send(heartbeatReqMsg); err != nil {
			return
		}
		hbMgr.heartbeatTracker[node.ID] = time.Now().UTC()
	}
	return
}

func (hbMgr *HeartbeatManager) startHeartbeatTracker() (err error) {
	hbMgr.hbLock.RLock()
	defer hbMgr.hbLock.RUnlock()

	for nodeID, lastSeenAtTime := range hbMgr.heartbeatTracker {
		if time.Since(lastSeenAtTime) > 10*time.Second {
			hbMgr.hbChan <- &HeartbeatNotAckEvent{
				NodeID:      nodeID,
				TimeElapsed: time.Since(lastSeenAtTime),
			}
		}
		if time.Since(lastSeenAtTime) > 120*time.Second {
			if err = hbMgr.replMgr.cluster.RemoveNode(nodeID); err != nil {
				return
			}
		}

	}
	return
}

func (hbMgr *HeartbeatManager) Start() (err error) {
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-hbMgr.ctx.Done():
			return
		case <-ticker.C:
			if err = hbMgr.sendHeartbeat(); err != nil {
				hbMgr.replMgr.log.Println("error in sending heartbeat", err)
				return
			}
		}
	}
	return
}
