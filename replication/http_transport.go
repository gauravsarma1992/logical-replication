package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

func NewHttpTransport(ctx context.Context, node *Node) (httpTransport *HttpTransport, err error) {
	// Set gin to production mode
	gin.SetMode(gin.ReleaseMode)

	httpTransport = &HttpTransport{
		ctx:    ctx,
		client: &http.Client{},
		server: gin.New(),
	}
	httpTransport.replMgr = ctx.Value(ReplicationManagerInContext).(*ReplicationManager)

	httpTransport.server.Use(httpTransport.NoOpLogger())
	if err = httpTransport.setup(); err != nil {
		return
	}
	if err = httpTransport.run(); err != nil {
		return
	}
	return
}

func (httpTransport *HttpTransport) NoOpLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Do nothing
		c.Next()
	}
}

func (httpTransport *HttpTransport) setup() (err error) {
	httpTransport.server.POST("/handler", httpTransport.messageHandler)
	return
}

func (httpTransport *HttpTransport) getRemoteUrl(remoteAddr *NodeAddr) string {
	return fmt.Sprintf("http://%s:%s", remoteAddr.Host, remoteAddr.Port)
}

func (httpTransport *HttpTransport) Ping(remoteAddr *NodeAddr) (respMsg *Message, err error) {
	var (
		reqMsg *Message
	)
	reqMsg = NewMessage(
		InfoMessageGroup,
		PingMessageType,
		httpTransport.replMgr.localNode.GetLocalUser(),
		&MessageUser{Addr: remoteAddr},
		&PingRequest{Node: httpTransport.replMgr.localNode},
	)
	if respMsg, err = httpTransport.send(remoteAddr, reqMsg); err != nil {
		return
	}
	return
}

func (httpTransport *HttpTransport) Send(reqMsg *Message) (respMsg *Message, err error) {
	var (
		remoteAddr *NodeAddr
	)
	remoteAddr = reqMsg.Remote.Addr
	if remoteAddr == nil {
		if remoteAddr, err = httpTransport.replMgr.transportMgr.ConvertNodeToAddr(reqMsg.Remote.NodeID); err != nil {
			return
		}
	}
	if respMsg, err = httpTransport.send(remoteAddr, reqMsg); err != nil {
		return
	}
	return
}

func (httpTransport *HttpTransport) send(remoteAddr *NodeAddr, reqMsg *Message) (respMsg *Message, err error) {
	var (
		httpReq     *http.Request
		httpResp    *http.Response
		httpReqBody []byte
	)
	type (
		HttpRespMessage struct {
			Message *Message `json:"message"`
		}
	)
	if httpReqBody, err = json.Marshal(reqMsg); err != nil {
		return
	}
	httpReqBodyBuffer := bytes.NewReader(httpReqBody)
	if httpReq, err = http.NewRequest(
		"POST",
		fmt.Sprintf("%s/handler", httpTransport.getRemoteUrl(remoteAddr)),
		httpReqBodyBuffer,
	); err != nil {
		return
	}
	if httpResp, err = httpTransport.client.Do(httpReq); err != nil {
		return
	}
	defer httpResp.Body.Close()

	httpRespMsg := &HttpRespMessage{}
	httpRespBodyB, _ := io.ReadAll(httpResp.Body)

	if err = json.Unmarshal(httpRespBodyB, httpRespMsg); err != nil {
		log.Println("Error decoding JSON response:", err)
		return
	}

	respMsg = httpRespMsg.Message
	return
}

func (httpTransport *HttpTransport) messageHandler(c *gin.Context) {
	var (
		receivedMsg  *Message
		generatedMsg *Message
		msgHandler   MessageHandler
		isPresent    bool
		err          error
	)
	receivedMsg = &Message{}
	if err = c.ShouldBindJSON(receivedMsg); err != nil {
		httpTransport.replMgr.log.Println("json message could not be parsed and failed with error", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	//httpTransport.replMgr.log.Println("Received message at server:", receivedMsg)
	if msgHandler, isPresent = httpTransport.replMgr.transportMgr.msgHandlers[receivedMsg.Type]; !isPresent {
		err = errors.New("Message handler not set")
		httpTransport.replMgr.log.Println("failed to fetch message handler", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if generatedMsg, err = msgHandler(receivedMsg); err != nil {
		httpTransport.replMgr.log.Println("message handler failed with error", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"message": generatedMsg,
	})
	return
}

func (httpTransport *HttpTransport) run() (err error) {
	go func() {
		if err = httpTransport.server.Run(
			fmt.Sprintf("%s:%s",
				httpTransport.replMgr.localNode.Config.Local.Host,
				httpTransport.replMgr.localNode.Config.Local.Port,
			),
		); err != nil {
			httpTransport.replMgr.log.Println("http transport failed", err)
		}
	}()
	time.Sleep(1 * time.Second)
	return
}
