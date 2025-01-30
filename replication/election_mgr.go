package replication

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

const (
	StartedElectionStatus  = ElectionStatusT(0)
	AcceptedElectionStatus = ElectionStatusT(1)
	RejectedElectionStatus = ElectionStatusT(2)

	ElectionLostError = "election lost"
)

type (
	TermID          uint64
	ElectionStatusT uint8

	// The ElectionManager struct is responsible for managing the election process.
	// Few key messages:
	// 1. Triggering an election when a leader node is down
	// 2. Counting the votes and deciding the leader
	// 3. Accepting/rejecting the election
	ElectionManager struct {
		ctx             context.Context
		currElection    *Election
		ongoingElection *Election

		elecMgrLock *sync.RWMutex
		replMgr     *ReplicationManager

		latestTermID TermID
	}

	ElectionStartReqMsg struct {
		Election *Election `json:"election"`
	}
	ElectionStartRespMsg struct {
		Vote *Vote `json:"vote"`
	}
	ElectionResultReqMsg struct {
		Election *Election `json:"election"`
		Decision bool      `json:"decision"`
	}
)

func NewElectionManager(ctx context.Context) (electionMgr *ElectionManager, err error) {
	electionMgr = &ElectionManager{
		ctx:          ctx,
		elecMgrLock:  &sync.RWMutex{},
		currElection: &Election{},
	}
	electionMgr.replMgr = ctx.Value(ReplicationManagerInContext).(*ReplicationManager)
	return
}

func (electionMgr *ElectionManager) GetCurrentTerm() (termID TermID) {
	electionMgr.elecMgrLock.RLock()
	defer electionMgr.elecMgrLock.RUnlock()

	termID = electionMgr.currElection.ID
	return
}

func (electionMgr *ElectionManager) UpdateElection(election *Election) (err error) {
	// This is primarily being called by the bootstrap manager when the node is booting
	// up for the first time and the remote node is being connected with.
	// Based on the remote node's election data, the current node can map its election data.
	if electionMgr.currElection != nil {
		return
	}

	electionMgr.ongoingElection = election
	electionMgr.AcceptElection()

	return
}

func (electionMgr *ElectionManager) DecideVote(election *Election) (voteStatus bool) {
	electionMgr.elecMgrLock.RLock()
	defer electionMgr.elecMgrLock.RUnlock()

	// Check curr term's ID and election's ID
	if election.ID < electionMgr.currElection.ID {
		voteStatus = false
		return
	}

	// If another ongoing election is present, then vote should be a NO, unless the ongoing election has timed out
	if electionMgr.ongoingElection != nil {
		if time.Since(electionMgr.ongoingElection.Timestamp) > 60*time.Second {
			voteStatus = false
			return
		}
		// Check ongoing election'ID
		if election.ID < electionMgr.ongoingElection.ID {
			voteStatus = false
			return
		}
	}
	// TODO: Check if the election is healthy?
	// TODO: Check the log index

	voteStatus = true

	return
}

func (electionMgr *ElectionManager) ElectionStartHandler(reqMsg *Message) (respMsg *Message, err error) {
	var (
		elecStartReqMsg *ElectionStartReqMsg
		election        *Election
	)
	elecStartReqMsg = &ElectionStartReqMsg{}
	if err = reqMsg.FillValue(elecStartReqMsg); err != nil {
		return
	}
	election = elecStartReqMsg.Election
	// If the election is younger than the current election, then ignore the election
	if electionMgr.ongoingElection != nil && election.ID < electionMgr.ongoingElection.ID {
		log.Println("election ID mismatch in start handler", elecStartReqMsg.Election.ID, electionMgr.ongoingElection)
		return
	}
	electionMgr.ongoingElection = election

	respMsg = NewMessage(
		InfoMessageGroup,
		ElectionStartMessageType,
		reqMsg.Remote,
		reqMsg.Local,
		&ElectionStartRespMsg{
			Vote: &Vote{
				TermID:    election.ID,
				FromNode:  electionMgr.replMgr.localNode,
				ForNode:   election.ProposedLeaderNode,
				Decision:  electionMgr.DecideVote(election),
				Timestamp: time.Now().UTC(),
			},
		},
	)

	return
}

func (electionMgr *ElectionManager) ElectionResultHandler(reqMsg *Message) (respMsg *Message, err error) {
	var (
		elecResultReqMsg *ElectionResultReqMsg
	)
	elecResultReqMsg = &ElectionResultReqMsg{}
	if err = reqMsg.FillValue(elecResultReqMsg); err != nil {
		return
	}
	// This usually happens when the node joins the cluster after the election has been almost concluded
	// and gets the results of the election.
	// If the node refuses to accept the election, then it may have to fetch the election data again or may
	// miss out on the election data.
	// If the node accepts the election, then there may be cases of over eager leader nodes trying to
	// become the leader.
	if electionMgr.ongoingElection == nil {
		// || elecResultReqMsg.Election.ID != electionMgr.ongoingElection.ID {
		electionMgr.replMgr.log.Println("election ID mismatch", elecResultReqMsg, electionMgr.ongoingElection)
		err = fmt.Errorf("election ID mismatch")
		return
	}
	if false == elecResultReqMsg.Decision {
		if err = electionMgr.RejectElection(); err != nil {
			return
		}
		return
	}
	if err = electionMgr.AcceptElection(); err != nil {
		return
	}
	return
}

func (electionMgr *ElectionManager) sendElectionStartMsg(node *Node, election *Election) (vote *Vote, err error) {
	var (
		reqMsg  *Message
		respMsg *Message
	)
	reqMsg = NewMessage(
		InfoMessageGroup,
		ElectionStartMessageType,
		electionMgr.replMgr.localNode.GetLocalUser(),
		node.GetLocalUser(),
		&ElectionStartReqMsg{
			Election: election,
		},
	)
	if respMsg, err = electionMgr.replMgr.transportMgr.Send(reqMsg); err != nil || respMsg == nil {
		electionMgr.replMgr.log.Println("Error sending data to remote node or nil resp sent", node.ID, err)
		return
	}
	elecStartRespMsg := &ElectionStartRespMsg{}
	if err = respMsg.FillValue(elecStartRespMsg); err != nil {
		return
	}
	vote = elecStartRespMsg.Vote
	return
}

func (electionMgr *ElectionManager) StartElection() (election *Election, err error) {
	var (
		wg *sync.WaitGroup
	)
	if election, err = electionMgr.NewElection(); err != nil {
		return
	}
	wg = &sync.WaitGroup{}

	for _, node := range electionMgr.replMgr.cluster.GetActiveRemoteNodes() {
		wg.Add(1)

		go func(wg *sync.WaitGroup, node *Node, election *Election) {
			var (
				vote *Vote
			)
			defer wg.Done()

			if vote, err = electionMgr.sendElectionStartMsg(node, election); err != nil || vote == nil {
				electionMgr.replMgr.log.Println("Error sending election start message to remote node or nil vote", node.ID, err)
				return
			}
			if err = election.TrackVote(vote); err != nil {
				electionMgr.replMgr.log.Println("Error tracking vote", err)
				return
			}
			return
		}(wg, node, election)
	}
	wg.Wait()
	if err = election.Decision(); err != nil {
		return
	}
	return
}

func (electionMgr *ElectionManager) Stepdown(proposedLeader *Node) (err error) {
	return
}

func (electionMgr *ElectionManager) sendResultMsgToNode(node *Node, elecResultMsg *ElectionResultReqMsg) (err error) {
	var (
		reqMsg *Message
	)
	reqMsg = NewMessage(
		InfoMessageGroup,
		ElectionResultMessageType,
		electionMgr.replMgr.localNode.GetLocalUser(),
		node.GetLocalUser(),
		elecResultMsg,
	)
	if _, err = electionMgr.replMgr.transportMgr.Send(reqMsg); err != nil {
		return
	}
	return
}

func (electionMgr *ElectionManager) triggerElection() (err error) {
	var (
		elecResultReqMsg *ElectionResultReqMsg
		decision         bool
	)
	decision = true

	if electionMgr.ongoingElection, err = electionMgr.StartElection(); err != nil {
		decision = false
	}
	elecResultReqMsg = &ElectionResultReqMsg{
		Election: electionMgr.ongoingElection,
		Decision: decision,
	}
	for _, node := range electionMgr.replMgr.cluster.GetActiveRemoteNodes() {
		go func(node *Node) {
			if err = electionMgr.sendResultMsgToNode(node, elecResultReqMsg); err != nil {
				electionMgr.replMgr.log.Println("Error sending election result message to remote node", node.ID, err)
				return
			}
			return
		}(node)
	}
	return
}

func (electionMgr *ElectionManager) GetRandomInterval() (randInterval int) {
	rand.Seed(time.Now().UnixNano())
	min := 1
	max := 20
	randInterval = rand.Intn(max-min+1) + min
	return
}

func (electionMgr *ElectionManager) ConductElection() (err error) {
	electionMgr.replMgr.log.Println("Conducting election")

	beforeLoopElection := electionMgr.currElection
	for {
		if beforeLoopElection.ID != electionMgr.currElection.ID {
			// If the transition is not required, then return from the function
			// This is mainly to ensure that the election trigger can be cancelled
			// if there is another election which has already been accepted.
			// If another election has been started, then the current node can still
			// try to be a leader since there is a possibility that the other election
			// might fail.
			// To check if another election has been accepted, the current election should
			// be checked with the previous election
			if err = electionMgr.RejectElection(); err != nil {
				return
			}
			return
		}
		// The election should be triggered as long as the `ShouldTransition` returns true
		if err = electionMgr.triggerElection(); err != nil {
			electionMgr.replMgr.log.Println("Error triggering election", err)
			time.Sleep(time.Duration(electionMgr.GetRandomInterval()) * time.Second)
			continue
		}
		break
	}

	// If this stage has been reached, it means the election has been accepted.
	if err = electionMgr.AcceptElection(); err != nil {
		return
	}
	return
}

func (electionMgr *ElectionManager) AcceptElection() (err error) {

	//electionMgr.replMgr.log.Println("Election accepted for proposed leader node", electionMgr.ongoingElection.ProposedLeaderNode)

	// Complete the ongoing election to current election
	// This ensures the termID is also updated
	electionMgr.elecMgrLock.Lock()

	electionMgr.currElection = electionMgr.ongoingElection
	electionMgr.ongoingElection = nil

	electionMgr.elecMgrLock.Unlock()

	// Transition to the election's proposed leader node
	if err = electionMgr.replMgr.cluster.AddLeaderNode(electionMgr.currElection.ProposedLeaderNode); err != nil {
		return
	}
	return
}

func (electionMgr *ElectionManager) RejectElection() (err error) {
	electionMgr.elecMgrLock.Lock()
	defer electionMgr.elecMgrLock.Unlock()

	electionMgr.replMgr.log.Println("Election rejected for proposed leader node", electionMgr.ongoingElection.ProposedLeaderNode.ID)
	electionMgr.ongoingElection = nil
	return
}

func (electionMgr *ElectionManager) ShouldFocusOnNode(nodeID NodeID) (shouldFocus bool) {
	// Only the current local node and the current leader node shoud be evaluated.
	// Another node that may be evaluated is the proposed leader node of the ongoing election.
	shouldFocus = false
	if nodeID == electionMgr.replMgr.localNode.ID {
		shouldFocus = true
		return
	}
	if electionMgr.currElection == nil || electionMgr.currElection.ProposedLeaderNode == nil {
		return
	}
	if nodeID == electionMgr.currElection.ProposedLeaderNode.ID {
		shouldFocus = true
		return
	}
	return
}

func (electionMgr *ElectionManager) startPolling() (err error) {
	// The function listens to events from various managers and decides if the election should be triggered
	// It doesn't have any grace period for the election to be triggered since it banks on the source
	// managers to have the required intelligence to trigger the event only when the transition is required.
	// One thing that this block should check if the event is specific to the current node.
	for {
		select {
		case <-electionMgr.replMgr.ctx.Done():
			return
		// Checks for health changes
		case healthEvent := <-electionMgr.replMgr.healthMgr.healthChangeCh:
			if !electionMgr.ShouldFocusOnNode(healthEvent.Node.ID) {
				continue
			}
			if healthEvent.Node.State != NodeStateUnhealthy {
				continue
			}
			if err = electionMgr.ConductElection(); err != nil {
				electionMgr.replMgr.log.Println("error while conducting election", err)
				continue
			}
			continue
		// Checks for cluster events, nodes added or removed
		case clusterEvent := <-electionMgr.replMgr.cluster.clusterEventCh:
			if !electionMgr.ShouldFocusOnNode(clusterEvent.Node.ID) {
				continue
			}
			if clusterEvent.Added {
				continue
			}
			if err = electionMgr.ConductElection(); err != nil {
				electionMgr.replMgr.log.Println("error while conducting election", err)
				continue
			}
			continue
		// Checks if cluster has no leader
		case <-electionMgr.replMgr.cluster.clusterNoLeaderEventCh:
			if err = electionMgr.ConductElection(); err != nil {
				electionMgr.replMgr.log.Println("error while conducting election", err)
				continue
			}
			continue
		// Checks heartbeats
		case hbEvent := <-electionMgr.replMgr.hbMgr.hbChan:
			if !electionMgr.ShouldFocusOnNode(hbEvent.NodeID) {
				continue
			}
			if err = electionMgr.ConductElection(); err != nil {
				electionMgr.replMgr.log.Println("error while conducting election", err)
				continue
			}
			continue
		}
	}
	return
}

func (electionMgr *ElectionManager) Start() (err error) {
	go electionMgr.startPolling()
	return
}
