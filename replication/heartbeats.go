package replication

import (
	"context"
	"time"
)

type (
	HeartbeatManager struct {
		ctx     context.Context
		replMgr *ReplicationManager
	}
	HeartbeatRequestMsg struct {
		Node *Node `json:"node"`
	}
	HeartbeatResponseMsg struct {
		NodeID NodeID `json:"node_id"`
	}
)

func NewHeartbeatManager(ctx context.Context) (hbMgr *HeartbeatManager, err error) {
	hbMgr = &HeartbeatManager{
		ctx: ctx,
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
