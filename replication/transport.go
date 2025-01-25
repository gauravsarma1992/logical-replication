package replication

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
)

type (
	Transport interface {
		Ping(*NodeAddr) (*Message, error)
		Send(*Message) (*Message, error)
	}

	HttpTransport struct {
		ctx     context.Context
		client  *http.Client
		server  *gin.Engine
		replMgr *ReplicationManager
	}
)

func NewTransport(ctx context.Context, node *Node) (transport Transport, err error) {
	transport, err = NewHttpTransport(ctx, node)
	return
}
