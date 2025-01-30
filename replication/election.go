package replication

import (
	"fmt"
	"sync"
	"time"
)

type (
	Election struct {
		ID        TermID    `json:"id"`
		Timestamp time.Time `json:"timestamp"`

		StartedByNode      *Node `json:"started_by_node"`
		PrevLeaderNode     *Node `json:"prev_leader_node"`
		ProposedLeaderNode *Node `json:"proposed_leader_node"`

		Status ElectionStatusT `json:"status"`

		Votes     []*Vote `json:"votes"`
		wonVotes  uint32  `json:"-"`
		lostVotes uint32  `json:"-"`

		elecLock *sync.RWMutex `json:"-"`
	}

	Vote struct {
		TermID TermID `json:"term_id"`

		FromNode *Node `json:"from_node"`
		ForNode  *Node `json:"for_node"`

		Decision  bool      `json:"decision"`
		Timestamp time.Time `json:"timestamp"`
	}
)

func (electionMgr *ElectionManager) NewElection() (election *Election, err error) {
	election = &Election{
		ID:        TermID(time.Now().UnixNano()),
		Timestamp: time.Now().UTC(),

		StartedByNode:      electionMgr.replMgr.localNode,
		PrevLeaderNode:     electionMgr.replMgr.cluster.leaderNode,
		ProposedLeaderNode: electionMgr.replMgr.localNode,

		Status: StartedElectionStatus,
		Votes:  make([]*Vote, 0),

		elecLock: &sync.RWMutex{},
	}

	return
}

func (election *Election) TrackVote(vote *Vote) (err error) {
	election.elecLock.Lock()
	defer election.elecLock.Unlock()

	election.Votes = append(election.Votes, vote)

	return
}

func (election *Election) Decision() (err error) {
	election.elecLock.RLock()
	defer election.elecLock.RUnlock()

	for _, vote := range election.Votes {
		// If even one vote is false, then the election is lost
		if false == vote.Decision {
			err = fmt.Errorf(ElectionLostError)
			return
		}
	}

	return
}
