package cluster

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"reservoir/pkg/logger"
)

// Transport timeouts bound blocking network operations so a single slow or
// unreachable peer cannot stall cluster-wide message processing.
const (
	transportDialTimeout  = 3 * time.Second
	transportWriteTimeout = 5 * time.Second
	// transportRedialCooldown rate-limits automatic reconnection so a dead peer
	// is redialed at most once per interval (bounding how often sendMessage can
	// block the sendLoop on a dial).
	transportRedialCooldown = 2 * time.Second
)

// MessageType represents the type of cluster message
type MessageType uint8

const (
	// Core cluster messages
	MsgHeartbeat MessageType = iota
	MsgJoinRequest
	MsgJoinResponse
	MsgLeaveNotification
	MsgNodeUpdate

	// Leader election messages (Raft-inspired)
	MsgVoteRequest
	MsgVoteResponse
	MsgAppendEntries

	// Data replication messages
	MsgReplicationPush
	MsgReplicationAck
	MsgSyncRequest
	MsgSyncResponse

	// Gossip protocol messages (SWIM-inspired)
	MsgGossip
	MsgProbe
	MsgAck
	MsgIndirectProbe
)

// Message represents a cluster message
type Message struct {
	Type      MessageType `json:"type"`
	From      UUIDv7      `json:"from"`
	To        UUIDv7      `json:"to"`
	Term      uint64      `json:"term"`
	Timestamp time.Time   `json:"timestamp"`
	Payload   []byte      `json:"payload"`
	Auth      []byte      `json:"auth,omitempty"` // HMAC over (Type,From,Term,Payload) when a cluster secret is configured
}

// Transport handles network communication between nodes
type Transport struct {
	node     *Node
	listener net.Listener

	// Connection pools (inspired by KeyDB's connection management)
	connections   map[UUIDv7]*nodeConnection
	addresses     map[UUIDv7]string    // nodeID -> "host:port", for reconnecting after a transient failure
	lastDial      map[UUIDv7]time.Time // nodeID -> last redial attempt, to rate-limit reconnection
	connectionsMu sync.RWMutex

	// Message handlers
	handlers   map[MessageType]MessageHandler
	handlersMu sync.RWMutex

	// Channels
	incoming chan *Message
	outgoing chan *outgoingMessage

	// Control
	stopCh chan struct{}
	wg     sync.WaitGroup

	// authKey is the shared cluster secret used to HMAC-authenticate inter-node
	// messages. When nil, authentication is disabled (backward compatible).
	authKey []byte
}

// SetAuthKey enables HMAC authentication of inter-node messages using the given
// shared secret. Must be called before Start. An empty key leaves auth disabled.
func (t *Transport) SetAuthKey(key []byte) {
	if len(key) == 0 {
		t.authKey = nil
		return
	}
	t.authKey = append([]byte(nil), key...)
}

// messageMAC computes the HMAC-SHA256 of a message's authenticated fields
// (Type, From, Term, Payload). To is intentionally excluded so a single
// signature is valid regardless of the destination.
func messageMAC(key []byte, msg *Message) []byte {
	mac := hmac.New(sha256.New, key)
	var hdr [1 + 16 + 8]byte
	hdr[0] = byte(msg.Type)
	copy(hdr[1:17], msg.From[:])
	binary.BigEndian.PutUint64(hdr[17:25], msg.Term)
	mac.Write(hdr[:])
	mac.Write(msg.Payload)
	return mac.Sum(nil)
}

// MessageHandler handles a specific message type
type MessageHandler func(*Message) error

// nodeConnection represents a connection to another node. Each connection owns a
// bounded send queue drained by its own goroutine, so a slow or hung peer only
// backs up its own queue instead of blocking sends to every other node.
type nodeConnection struct {
	conn         net.Conn
	encoder      *gob.Encoder
	decoder      *gob.Decoder
	lastActivity time.Time
	sendQueue    chan *Message
	stop         chan struct{}
	stopOnce     sync.Once
}

// newNodeConnection wraps a raw net.Conn and initializes its per-peer send queue.
func newNodeConnection(conn net.Conn) *nodeConnection {
	return &nodeConnection{
		conn:         conn,
		encoder:      gob.NewEncoder(conn),
		decoder:      gob.NewDecoder(conn),
		lastActivity: time.Now(),
		sendQueue:    make(chan *Message, 256),
		stop:         make(chan struct{}),
	}
}

// close stops the connection's send worker and closes the socket, exactly once.
func (nc *nodeConnection) close() {
	nc.stopOnce.Do(func() {
		close(nc.stop)
		_ = nc.conn.Close()
	})
}

// outgoingMessage wraps a message with destination info
type outgoingMessage struct {
	destination UUIDv7
	message     *Message
}

// NewTransport creates a new transport layer
func NewTransport(node *Node) *Transport {
	return &Transport{
		node:        node,
		connections: make(map[UUIDv7]*nodeConnection),
		addresses:   make(map[UUIDv7]string),
		lastDial:    make(map[UUIDv7]time.Time),
		handlers:    make(map[MessageType]MessageHandler),
		incoming:    make(chan *Message, 1000),
		outgoing:    make(chan *outgoingMessage, 1000),
		stopCh:      make(chan struct{}),
	}
}

// Start starts the transport layer
func (t *Transport) Start() error {
	addr := fmt.Sprintf("%s:%d", t.node.Address, t.node.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to start listener: %w", err)
	}

	t.listener = listener
	logger.Info("Transport started on %s", addr)

	// Start worker goroutines
	t.wg.Add(3)
	go t.acceptLoop()
	go t.sendLoop()
	go t.processLoop()

	return nil
}

// Stop stops the transport layer
func (t *Transport) Stop() {
	close(t.stopCh)

	if t.listener != nil {
		t.listener.Close()
	}

	// Close all connections (stops their per-peer send workers too).
	t.connectionsMu.Lock()
	for _, conn := range t.connections {
		conn.close()
	}
	t.connectionsMu.Unlock()

	t.wg.Wait()
	logger.Info("Transport stopped")
}

// RegisterHandler registers a message handler
func (t *Transport) RegisterHandler(msgType MessageType, handler MessageHandler) {
	t.handlersMu.Lock()
	defer t.handlersMu.Unlock()
	t.handlers[msgType] = handler
}

// Send sends a message to a specific node
func (t *Transport) Send(destination UUIDv7, msgType MessageType, payload interface{}) error {
	// Serialize payload
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(payload); err != nil {
		return fmt.Errorf("failed to encode payload: %w", err)
	}

	msg := &Message{
		Type:      msgType,
		From:      t.node.NodeID,
		To:        destination,
		Term:      t.node.CurrentTerm.Load(),
		Timestamp: time.Now(),
		Payload:   buf.Bytes(),
	}
	if t.authKey != nil {
		msg.Auth = messageMAC(t.authKey, msg)
	}

	select {
	case t.outgoing <- &outgoingMessage{destination: destination, message: msg}:
		return nil
	case <-t.stopCh:
		return fmt.Errorf("transport is stopped")
	default:
		return fmt.Errorf("outgoing queue is full")
	}
}

// Broadcast sends a message to all known nodes
func (t *Transport) Broadcast(msgType MessageType, payload interface{}) error {
	t.connectionsMu.RLock()
	destinations := make([]UUIDv7, 0, len(t.connections))
	for nodeID := range t.connections {
		destinations = append(destinations, nodeID)
	}
	t.connectionsMu.RUnlock()

	var lastErr error
	for _, dest := range destinations {
		if err := t.Send(dest, msgType, payload); err != nil {
			lastErr = err
			logger.Warning("Failed to send to %s: %v", dest, err)
		}
	}

	return lastErr
}

// acceptLoop accepts incoming connections
func (t *Transport) acceptLoop() {
	defer t.wg.Done()

	for {
		conn, err := t.listener.Accept()
		if err != nil {
			select {
			case <-t.stopCh:
				return
			default:
				logger.Error("Accept error: %v", err)
				continue
			}
		}

		go t.handleConnection(conn)
	}
}

// handleConnection handles an incoming connection
func (t *Transport) handleConnection(conn net.Conn) {
	defer conn.Close()

	decoder := gob.NewDecoder(conn)

	for {
		var msg Message
		if err := decoder.Decode(&msg); err != nil {
			if err != io.EOF {
				logger.Debug("Connection read error: %v", err)
			}
			return
		}

		// Reject unauthenticated or forged messages when a cluster secret is
		// configured. Without this, any client that can reach the cluster port
		// could inject e.g. a replication FLUSHALL or a node-eviction update.
		if t.authKey != nil {
			expected := messageMAC(t.authKey, &msg)
			if !hmac.Equal(msg.Auth, expected) {
				logger.Warning("Dropping cluster message with invalid authentication from %s (type %d)", msg.From, msg.Type)
				continue
			}
		}

		// Update stats
		t.node.MessagesReceived.Add(1)
		t.node.BytesReceived.Add(uint64(len(msg.Payload)))

		// Queue message for processing
		select {
		case t.incoming <- &msg:
		case <-t.stopCh:
			return
		default:
			logger.Warning("Incoming queue full, dropping message")
		}
	}
}

// sendLoop dispatches outgoing messages to per-peer send queues. It never blocks
// on the actual network write — the connection's own worker (peerSendLoop) does
// the blocking encode — so one slow peer cannot stall sends to the others.
func (t *Transport) sendLoop() {
	defer t.wg.Done()

	for {
		select {
		case <-t.stopCh:
			return
		case out := <-t.outgoing:
			nc, err := t.getConnection(out.destination)
			if err != nil {
				logger.Debug("No route to %s: %v", out.destination, err)
				continue
			}
			select {
			case nc.sendQueue <- out.message:
			case <-t.stopCh:
				return
			default:
				logger.Warning("Send queue full for %s, dropping message", out.destination)
			}
		}
	}
}

// peerSendLoop is the per-connection worker that performs the blocking encode for
// one peer. On a write error it removes and closes the connection; a later Send
// will reconnect via getConnection.
func (t *Transport) peerSendLoop(dest UUIDv7, nc *nodeConnection) {
	for {
		select {
		case <-t.stopCh:
			return
		case <-nc.stop:
			return
		case msg := <-nc.sendQueue:
			_ = nc.conn.SetWriteDeadline(time.Now().Add(transportWriteTimeout))
			if err := nc.encoder.Encode(msg); err != nil {
				logger.Debug("Send to %s failed: %v", dest, err)
				t.connectionsMu.Lock()
				if t.connections[dest] == nc {
					delete(t.connections, dest)
				}
				t.connectionsMu.Unlock()
				nc.close()
				return
			}
			nc.lastActivity = time.Now()
			t.node.MessagesSent.Add(1)
			t.node.BytesSent.Add(uint64(len(msg.Payload)))
		}
	}
}

// getConnection returns a live connection to a node, reconnecting on demand if
// a previous connection was dropped by a transient network error.
func (t *Transport) getConnection(nodeID UUIDv7) (*nodeConnection, error) {
	t.connectionsMu.RLock()
	conn, exists := t.connections[nodeID]
	t.connectionsMu.RUnlock()
	if exists {
		return conn, nil
	}

	// No live connection. Reconnect if we know the peer's address and are not
	// still within the redial cooldown (which prevents dial storms and bounds
	// how often this can block the caller).
	t.connectionsMu.Lock()
	if conn, exists := t.connections[nodeID]; exists { // re-check under write lock
		t.connectionsMu.Unlock()
		return conn, nil
	}
	addr, known := t.addresses[nodeID]
	if !known {
		t.connectionsMu.Unlock()
		return nil, fmt.Errorf("no connection to node %s", nodeID)
	}
	if last, ok := t.lastDial[nodeID]; ok && time.Since(last) < transportRedialCooldown {
		t.connectionsMu.Unlock()
		return nil, fmt.Errorf("no connection to node %s (redial on cooldown)", nodeID)
	}
	t.lastDial[nodeID] = time.Now()
	t.connectionsMu.Unlock()

	netConn, err := net.DialTimeout("tcp", addr, transportDialTimeout)
	if err != nil {
		return nil, fmt.Errorf("reconnect to %s failed: %w", addr, err)
	}
	nc := newNodeConnection(netConn)
	t.connectionsMu.Lock()
	// Another goroutine may have reconnected in the meantime; prefer the existing one.
	if existing, ok := t.connections[nodeID]; ok {
		t.connectionsMu.Unlock()
		netConn.Close()
		return existing, nil
	}
	t.connections[nodeID] = nc
	t.connectionsMu.Unlock()
	go t.peerSendLoop(nodeID, nc)
	logger.Info("Reconnected to node %s at %s", nodeID, addr)
	return nc, nil
}

// processLoop processes incoming messages
func (t *Transport) processLoop() {
	defer t.wg.Done()

	for {
		select {
		case <-t.stopCh:
			return
		case msg := <-t.incoming:
			t.processMessage(msg)
		}
	}
}

// processMessage processes a single message
func (t *Transport) processMessage(msg *Message) {
	t.handlersMu.RLock()
	handler, exists := t.handlers[msg.Type]
	t.handlersMu.RUnlock()

	if !exists {
		logger.Warning("No handler for message type %d", msg.Type)
		return
	}

	if err := handler(msg); err != nil {
		logger.Error("Message handler error: %v", err)
	}
}

// ConnectToNode establishes a connection to another node
func (t *Transport) ConnectToNode(nodeID UUIDv7, address string, port int) error {
	addr := fmt.Sprintf("%s:%d", address, port)
	// Use a bounded dial: ConnectToNode is called from message handlers running
	// on the single processLoop goroutine, so a blocking net.Dial to an
	// unreachable peer would stall all message processing (heartbeats, votes)
	// for the full OS TCP timeout (~2 min).
	conn, err := net.DialTimeout("tcp", addr, transportDialTimeout)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", addr, err)
	}

	nc := newNodeConnection(conn)

	t.connectionsMu.Lock()
	// If a connection to this node already exists, close the new one and keep the
	// existing worker rather than orphaning it.
	if _, ok := t.connections[nodeID]; ok {
		t.addresses[nodeID] = addr
		t.connectionsMu.Unlock()
		conn.Close()
		logger.Debug("Connection to %s already exists; reusing it", nodeID)
		return nil
	}
	t.connections[nodeID] = nc
	// Remember the address so a connection dropped by a transient network error
	// can be re-established on demand (see getConnection).
	t.addresses[nodeID] = addr
	t.connectionsMu.Unlock()
	go t.peerSendLoop(nodeID, nc)

	logger.Info("Connected to node %s at %s", nodeID, addr)
	return nil
}

// DisconnectFromNode closes connection to a specific node
func (t *Transport) DisconnectFromNode(nodeID UUIDv7) {
	t.connectionsMu.Lock()
	defer t.connectionsMu.Unlock()

	if nc, exists := t.connections[nodeID]; exists {
		nc.close() // stops the peer send worker and closes the socket
		delete(t.connections, nodeID)
		logger.Debug("Disconnected from node %s", nodeID)
	}
	// Explicit disconnect (e.g. eviction or leave) is intentional: forget the
	// address so we do not keep auto-reconnecting. A rejoin re-registers it.
	// (A transient send error only drops the connection, not the address, so
	// getConnection can still auto-reconnect in that case.)
	delete(t.addresses, nodeID)
	delete(t.lastDial, nodeID)
}

// GetConnectedNodes returns list of connected node IDs
func (t *Transport) GetConnectedNodes() []UUIDv7 {
	t.connectionsMu.RLock()
	defer t.connectionsMu.RUnlock()

	nodes := make([]UUIDv7, 0, len(t.connections))
	for nodeID := range t.connections {
		nodes = append(nodes, nodeID)
	}
	return nodes
}
