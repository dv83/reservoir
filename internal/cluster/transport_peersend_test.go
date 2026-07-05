package cluster

import (
	"encoding/gob"
	"net"
	"strconv"
	"testing"
	"time"
)

// TestPerPeerSendDelivers exercises the full send path — Send enqueues to the
// global outgoing channel, sendLoop dispatches to the peer's send queue, and the
// per-peer worker performs the encode — end to end to a real listener.
func TestPerPeerSendDelivers(t *testing.T) {
	// Peer B: a raw listener that decodes one Message.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	got := make(chan Message, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		var m Message
		if err := gob.NewDecoder(c).Decode(&m); err == nil {
			got <- m
		}
	}()

	host, portStr, _ := net.SplitHostPort(ln.Addr().String())
	port, _ := strconv.Atoi(portStr)

	// Transport A (sender): Start so sendLoop runs; listen on an ephemeral port.
	a := NewTransport(NewNode("127.0.0.1", 0, NewUUIDv7()))
	if err := a.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer a.Stop()
	defer a.node.OptimizedVectorClock.Stop()

	peer := NewUUIDv7()
	if err := a.ConnectToNode(peer, host, port); err != nil {
		t.Fatalf("ConnectToNode: %v", err)
	}

	if err := a.Send(peer, MsgHeartbeat, Heartbeat{NodeID: a.node.NodeID, Term: 7}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	select {
	case m := <-got:
		if m.Type != MsgHeartbeat {
			t.Fatalf("received message type %d, want MsgHeartbeat", m.Type)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("peer did not receive the sent message")
	}
}
