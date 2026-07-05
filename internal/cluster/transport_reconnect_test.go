package cluster

import (
	"net"
	"strconv"
	"testing"
	"time"
)

// TestTransportReconnectsAfterDrop verifies that a connection dropped by a
// transient error is transparently re-established by getConnection, and that an
// explicit DisconnectFromNode forgets the address so no further reconnect
// occurs.
func TestTransportReconnectsAfterDrop(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	accepted := make(chan net.Conn, 4)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			accepted <- c
		}
	}()

	host, portStr, _ := net.SplitHostPort(ln.Addr().String())
	port, _ := strconv.Atoi(portStr)

	tr := NewTransport(NewNode("127.0.0.1", 0, NewUUIDv7()))
	peer := NewUUIDv7()

	// Initial connect registers the peer's address.
	if err := tr.ConnectToNode(peer, host, port); err != nil {
		t.Fatalf("ConnectToNode: %v", err)
	}
	waitAccept(t, accepted, "initial connection")

	// Simulate a transient drop: close and remove the connection but keep the
	// address (exactly what a send error does).
	tr.connectionsMu.Lock()
	tr.connections[peer].conn.Close()
	delete(tr.connections, peer)
	tr.connectionsMu.Unlock()

	// getConnection should transparently reconnect.
	conn, err := tr.getConnection(peer)
	if err != nil {
		t.Fatalf("expected transparent reconnect, got error: %v", err)
	}
	if conn == nil {
		t.Fatalf("reconnect returned a nil connection")
	}
	waitAccept(t, accepted, "reconnection")

	// After an explicit disconnect the address is forgotten: no reconnect.
	tr.DisconnectFromNode(peer)
	if _, err := tr.getConnection(peer); err == nil {
		t.Fatalf("expected error after DisconnectFromNode forgets the address")
	}
}

func waitAccept(t *testing.T, ch <-chan net.Conn, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("server did not accept %s", what)
	}
}
