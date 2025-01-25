package replication

import (
	"encoding/json"
	"fmt"
	"log"
	"time"
)

const (
	// Message groups
	ReplicationMessageGroup MessageGroupT = MessageGroupT(0)
	InfoMessageGroup        MessageGroupT = MessageGroupT(1)
	HeartbeatMessageGroup   MessageGroupT = MessageGroupT(2)

	// Message Types
	PingMessageType                MessageTypeT = MessageTypeT(0)
	ClusterDiscoveryMessageType    MessageTypeT = MessageTypeT(1)
	HeartbeatMessageType           MessageTypeT = MessageTypeT(2)
	DataReplicationPushMessageType MessageTypeT = MessageTypeT(3)
	DataReplicationPullMessageType MessageTypeT = MessageTypeT(4)
)

type (
	MessageID     uint64
	MessageGroupT uint8
	MessageTypeT  uint16

	MessageHandler func(*Message) (*Message, error)

	Message struct {
		Version uint64    `json:"version"`
		ID      MessageID `json:"id"`

		Timestamp uint64 `json:"timestamp"`

		Local  *MessageUser `json:"local_user"`
		Remote *MessageUser `json:"remote_user"`

		// The group of the message.
		Group MessageGroupT `json:"group"`
		Type  MessageTypeT  `json:"message_type"`
		Value []byte        `json:"value"`
	}
	MessageUser struct {
		NodeID NodeID    `json:"node_id"`
		Addr   *NodeAddr `json:"addr"`
	}
)

func NewMessage(group MessageGroupT, msgType MessageTypeT, localUser *MessageUser, remoteUser *MessageUser, value interface{}) (msg *Message) {
	var (
		err    error
		msgVal []byte
	)
	if msgVal, err = json.Marshal(value); err != nil {
		log.Println("error while marshaling value in NewMessage", err)
		return
	}
	msg = &Message{
		ID:        MessageID(time.Now().UnixNano()),
		Version:   1,
		Timestamp: uint64(time.Now().UnixNano()),

		Local:  localUser,
		Remote: remoteUser,

		Group: group,
		Type:  msgType,
		Value: msgVal,
	}
	return
}

func (msg *Message) String() string {
	return fmt.Sprintf(
		"Message ID: %d, Group: %d, Type: %d, Local User: %s, Remote User: %s, Value: %s",
		msg.ID, msg.Group, msg.Type, msg.Local, msg.Remote, msg.Value,
	)
}

func (msg *Message) FillValue(msgVal interface{}) (err error) {
	err = json.Unmarshal(msg.Value, msgVal)
	return
}

func (msgUser *MessageUser) String() string {
	return fmt.Sprintf(
		"NodeID: %d, Addr: %s",
		msgUser.NodeID, msgUser.Addr,
	)
}
