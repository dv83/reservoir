package cluster

import (
	"crypto/hmac"
	"testing"
)

// TestMessageMACAuthentication verifies that HMAC authentication accepts a
// correctly-signed message and rejects tampered or wrong-key messages.
func TestMessageMACAuthentication(t *testing.T) {
	key := []byte("cluster-shared-secret")
	from := NewUUIDv7()

	msg := &Message{
		Type:    MsgReplicationPush,
		From:    from,
		Term:    3,
		Payload: []byte("replicate: SET k v"),
	}
	msg.Auth = messageMAC(key, msg)

	// A correctly-signed message verifies.
	if !hmac.Equal(msg.Auth, messageMAC(key, msg)) {
		t.Fatalf("valid message failed authentication")
	}

	// Tampering with the payload invalidates the MAC.
	tampered := *msg
	tampered.Payload = []byte("replicate: FLUSHALL")
	if hmac.Equal(tampered.Auth, messageMAC(key, &tampered)) {
		t.Fatalf("tampered payload passed authentication")
	}

	// A different key does not verify.
	if hmac.Equal(msg.Auth, messageMAC([]byte("wrong-secret"), msg)) {
		t.Fatalf("message verified under the wrong key")
	}

	// Changing the message type invalidates the MAC.
	retyped := *msg
	retyped.Type = MsgHeartbeat
	if hmac.Equal(retyped.Auth, messageMAC(key, &retyped)) {
		t.Fatalf("retyped message passed authentication")
	}
}

// TestSetAuthKeyCopiesSecret verifies SetAuthKey enables/disables auth and does
// not alias the caller's slice.
func TestSetAuthKeyCopiesSecret(t *testing.T) {
	tr := NewTransport(newRemoteNode(NewUUIDv7(), "127.0.0.1", 7500, NewUUIDv7()))

	secret := []byte("secret")
	tr.SetAuthKey(secret)
	if tr.authKey == nil {
		t.Fatalf("SetAuthKey did not enable authentication")
	}
	secret[0] = 'X' // mutate caller's slice
	if tr.authKey[0] == 'X' {
		t.Fatalf("SetAuthKey aliased the caller's secret slice")
	}

	tr.SetAuthKey(nil)
	if tr.authKey != nil {
		t.Fatalf("empty key should disable authentication")
	}
}
