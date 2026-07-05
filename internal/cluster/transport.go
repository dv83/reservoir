package cluster

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"reservoir/pkg/logger"
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
}

// Transport handles network communication between nodes
type Transport struct {
	node     *Node
	listener net.Listener

	// Connection pools (inspired by KeyDB's connection management)
	connections   map[UUIDv7]*nodeConnection
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
}

// MessageHandler handles a specific message type
type MessageHandler func(*Message) error

// nodeConnection represents a connection to another node
type nodeConnection struct {
	conn         net.Conn
	encoder      *gob.Encoder
	decoder      *gob.Decoder
	lastActivity time.Time
	mu           sync.Mutex
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

	// Close all connections
	t.connectionsMu.Lock()
	for _, conn := range t.connections {
		conn.conn.Close()
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

// sendLoop sends outgoing messages
func (t *Transport) sendLoop() {
	defer t.wg.Done()

	for {
		select {
		case <-t.stopCh:
			return
		case msg := <-t.outgoing:
			if err := t.sendMessage(msg); err != nil {
				logger.Error("Failed to send message: %v", err)
			}
		}
	}
}

// sendMessage sends a single message
func (t *Transport) sendMessage(out *outgoingMessage) error {
	conn, err := t.getConnection(out.destination)
	if err != nil {
		return err
	}

	conn.mu.Lock()
	defer conn.mu.Unlock()

	if err := conn.encoder.Encode(out.message); err != nil {
		// Remove failed connection
		t.connectionsMu.Lock()
		delete(t.connections, out.destination)
		t.connectionsMu.Unlock()
		conn.conn.Close()
		return err
	}

	conn.lastActivity = time.Now()

	// Update stats
	t.node.MessagesSent.Add(1)
	t.node.BytesSent.Add(uint64(len(out.message.Payload)))

	return nil
}

// getConnection gets or creates a connection to a node
func (t *Transport) getConnection(nodeID UUIDv7) (*nodeConnection, error) {
	t.connectionsMu.RLock()
	conn, exists := t.connections[nodeID]
	t.connectionsMu.RUnlock()

	if exists {
		return conn, nil
	}

	// Create new connection
	// In real implementation, we'd need a node registry to get address
	// For now, returning error
	return nil, fmt.Errorf("no connection to node %s", nodeID)
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
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", addr, err)
	}

	nc := &nodeConnection{
		conn:         conn,
		encoder:      gob.NewEncoder(conn),
		decoder:      gob.NewDecoder(conn),
		lastActivity: time.Now(),
	}

	t.connectionsMu.Lock()
	t.connections[nodeID] = nc
	t.connectionsMu.Unlock()

	logger.Info("Connected to node %s at %s", nodeID, addr)
	return nil
}

// DisconnectFromNode closes connection to a specific node
func (t *Transport) DisconnectFromNode(nodeID UUIDv7) {
	t.connectionsMu.Lock()
	defer t.connectionsMu.Unlock()

	if nc, exists := t.connections[nodeID]; exists {
		nc.conn.Close()
		delete(t.connections, nodeID)
		logger.Debug("Disconnected from node %s", nodeID)
	}
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
