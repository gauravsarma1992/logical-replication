package replication

import (
	"context"
	"time"
)

const (
	StartedElectionStatus  = ElectionStatusT(0)
	AcceptedElectionStatus = ElectionStatusT(1)
	RejectedElectionStatus = ElectionStatusT(2)
)

type (
	TermID          uint64
	ElectionStatusT uint8

	ElectionManager struct {
		ctx       context.Context
		elections []*Election
	}

	Election struct {
		ID        TermID
		Timestamp time.Time

		StartedByNode      *Node
		PrevLeaderNode     *Node
		ProposedLeaderNode *Node

		Status ElectionStatusT

		votes []*Vote
	}

	Vote struct {
		FromNode  *Node
		ForNode   *Node
		Timestamp time.Time
	}
)

func NewElectionManager(ctx context.Context) (electionMgr *ElectionManager, err error) {
	electionMgr = &ElectionManager{
		ctx: ctx,
	}
	return
}

func (electionMgr *ElectionManager) Start() (err error) {
	return
}
