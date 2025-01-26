package replication

import (
	"context"
	"log"
	"os"
	"testing"
)

type TestNode struct {
	node *Node
}

func setupTestNode(t *testing.T) *TestNode {
	config := DefaultSingleNodeConfig()
	config.Remote.Host = "127.0.0.1"
	config.Remote.Port = "8080"

	ctx := context.WithValue(context.Background(), ReplicationManagerInContext, &ReplicationManager{
		localNode: &Node{ID: 1},
		log:       log.New(os.Stdout, "test: ", log.LstdFlags),
	})
	node, err := NewNode(ctx, config)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}
	return &TestNode{node: node}
}

func teardownTestNode(tn *TestNode) {
	// Clean up resources if needed
}

func TestNodeDefaultSingleNodeConfig(t *testing.T) {
	config := DefaultSingleNodeConfig()
	if config.Local.Host != "127.0.0.1" || config.Local.Port != "8080" {
		t.Errorf("DefaultSingleNodeConfig returned incorrect local config: %+v", config.Local)
	}
}

func TestNodeNewNode(t *testing.T) {
	tn := setupTestNode(t)
	defer teardownTestNode(tn)

	if tn.node.ID == 0 {
		t.Errorf("NewNode returned node with ID 0")
	}
	if tn.node.Config.Local.Host != "127.0.0.1" {
		t.Errorf("NewNode returned node with incorrect local host: %s", tn.node.Config.Local.Host)
	}
}

func TestNodeVerifyConfig(t *testing.T) {
	tn := setupTestNode(t)
	defer teardownTestNode(tn)

	err := tn.node.verifyConfig()
	if err != nil {
		t.Errorf("verifyConfig failed: %v", err)
	}

	tn.node.Config.Local.Host = ""
	err = tn.node.verifyConfig()
	if err == nil {
		t.Errorf("verifyConfig did not fail for missing local host")
	}
}
