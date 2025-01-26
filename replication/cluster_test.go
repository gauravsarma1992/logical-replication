package replication

import (
	"context"
	"log"
	"os"
	"testing"
	"time"
)

type TestCluster struct {
	cluster *Cluster
}

func setupTestCluster(t *testing.T) *TestCluster {
	ctx := context.WithValue(context.Background(), ReplicationManagerInContext, &ReplicationManager{
		localNode: &Node{ID: 1},
		log:       log.New(os.Stdout, "test: ", log.LstdFlags),
	})
	cluster, err := NewCluster(ctx)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	return &TestCluster{cluster: cluster}
}

func teardownTestCluster(tc *TestCluster) {
	// Clean up resources if needed
}

func TestClusterAddNode(t *testing.T) {
	tc := setupTestCluster(t)
	defer teardownTestCluster(tc)

	node := &Node{ID: 2, LastUpdatedAt: time.Now().UTC()}
	err := tc.cluster.AddNode(node)
	if err != nil {
		t.Errorf("AddNode failed: %v", err)
	}

	if _, exists := tc.cluster.nodes[node.ID]; !exists {
		t.Errorf("Node was not added to the cluster")
	}
}

func TestClusterGetLeaderNode(t *testing.T) {
	tc := setupTestCluster(t)
	defer teardownTestCluster(tc)

	node := &Node{ID: 2, NodeType: NodeTypeLeader}
	tc.cluster.leaderNode = node

	leaderNode, err := tc.cluster.GetLeaderNode()
	if err != nil {
		t.Errorf("GetLeaderNode failed: %v", err)
	}

	if leaderNode.ID != node.ID {
		t.Errorf("Expected leader node ID %d, got %d", node.ID, leaderNode.ID)
	}
}

func TestClusterAddLeaderNode(t *testing.T) {
	tc := setupTestCluster(t)
	defer teardownTestCluster(tc)

	node := &Node{ID: 2, NodeType: NodeTypeLeader}
	err := tc.cluster.AddLeaderNode(node)
	if err != nil {
		t.Errorf("AddLeaderNode failed: %v", err)
	}

	if tc.cluster.leaderNode.ID != node.ID {
		t.Errorf("Expected leader node ID %d, got %d", node.ID, tc.cluster.leaderNode.ID)
	}
}

func TestClusterGetRemoteNodes(t *testing.T) {
	tc := setupTestCluster(t)
	defer teardownTestCluster(tc)

	node1 := &Node{ID: 2}
	node2 := &Node{ID: 3}
	tc.cluster.nodes[node1.ID] = node1
	tc.cluster.nodes[node2.ID] = node2

	remoteNodes := tc.cluster.GetRemoteNodes()
	if len(remoteNodes) != 2 {
		t.Errorf("Expected 2 remote nodes, got %d", len(remoteNodes))
	}
}

func TestClusterGetNodes(t *testing.T) {
	tc := setupTestCluster(t)
	defer teardownTestCluster(tc)

	node1 := &Node{ID: 2}
	node2 := &Node{ID: 3}
	tc.cluster.nodes[node1.ID] = node1
	tc.cluster.nodes[node2.ID] = node2

	nodes := tc.cluster.GetNodes()
	if len(nodes) != 2 {
		t.Errorf("Expected 2 nodes, got %d", len(nodes))
	}
}

func TestClusterRemoveNode(t *testing.T) {
	tc := setupTestCluster(t)
	defer teardownTestCluster(tc)

	node := &Node{ID: 2}
	tc.cluster.nodes[node.ID] = node

	err := tc.cluster.RemoveNode(node)
	if err != nil {
		t.Errorf("RemoveNode failed: %v", err)
	}

	if _, exists := tc.cluster.nodes[node.ID]; exists {
		t.Errorf("Node was not removed from the cluster")
	}
}

func TestClusterUpdate(t *testing.T) {
	tc := setupTestCluster(t)
	defer teardownTestCluster(tc)

	node1 := &Node{ID: 2, LastUpdatedAt: time.Now().UTC()}
	node2 := &Node{ID: 3, LastUpdatedAt: time.Now().UTC(), NodeType: NodeTypeLeader}
	nodes := []*Node{node1, node2}

	err := tc.cluster.Update(nodes)
	if err != nil {
		t.Errorf("Update failed: %v", err)
	}

	if len(tc.cluster.nodes) != 2 {
		t.Errorf("Expected 2 nodes in the cluster, got %d", len(tc.cluster.nodes))
	}

	if tc.cluster.leaderNode.ID != node2.ID {
		t.Errorf("Expected leader node ID %d, got %d", node2.ID, tc.cluster.leaderNode.ID)
	}
}
