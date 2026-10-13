package osc

import (
	"bufio"
	"bytes"
	"net"
	"sync"
	"testing"
	"time"
)

// TestReadBundleTrailingZeros checks that a bundle followed by trailing zero
// bytes still decodes with all of its messages. Before the fix, the trailing
// zeros were parsed as a bundle element with length 0, which produced an EOF
// error and the whole packet was dropped. (Issue #53.)
func TestReadBundleTrailingZeros(t *testing.T) {
	b := NewBundle(time.Now())
	b.Append(NewMessage("/a", "test"))
	b.Append(NewMessage("/b", "test2"))

	d, err := b.MarshalBinary()
	if err != nil {
		t.Fatalf("bundle marshal error: %v", err)
	}
	d = append(d, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

	bundle, err := readBundle(bufio.NewReader(bytes.NewReader(d)))
	if err != nil {
		t.Fatalf("bundle read error: %v", err)
	}
	if len(bundle.Messages) != 2 {
		t.Fatalf("incorrect message count on decode: %d, want 2", len(bundle.Messages))
	}
}

// TestReadBlobLarge checks that a blob larger than the bufio.Reader buffer
// size (4096 bytes) is read correctly. Before the fix, readBlob validated the
// length against reader.Buffered(), which is capped at the bufio buffer size,
// so any blob over 4096 bytes failed with "invalid blob length". (Issue #62.)
func TestReadBlobLarge(t *testing.T) {
	big := make([]byte, 6000)
	for i := range big {
		big[i] = byte(i % 251)
	}
	m := NewMessage("/blob", big)

	d, err := m.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	p, err := readPacket(bufio.NewReader(bytes.NewReader(d)))
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	msg, ok := p.(*Message)
	if !ok {
		t.Fatalf("parsed packet is %T, want *Message", p)
	}
	if len(msg.Arguments) != 1 {
		t.Fatalf("argument count = %d, want 1", len(msg.Arguments))
	}
	got, ok := msg.Arguments[0].([]byte)
	if !ok {
		t.Fatalf("argument type is %T, want []byte", msg.Arguments[0])
	}
	if !bytes.Equal(got, big) {
		t.Fatal("blob content mismatch")
	}
}

// TestReadBlobTruncated checks that a blob whose declared length exceeds the
// available data yields an error instead of a short read.
func TestReadBlobTruncated(t *testing.T) {
	// Blob length 8 but only 4 bytes of data follow.
	d := []byte("/blob\x00\x00\x00,b\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00")
	_, err := readPacket(bufio.NewReader(bytes.NewReader(d)))
	if err == nil {
		t.Fatal("expected error for truncated blob, got nil")
	}
}

// TestMessageMatchWholeAddress checks that a message address only matches
// handler addresses in full. Before the fix, the pattern regex was unanchored,
// so a message to /time also dispatched to a handler for /time/str.
// (Issue #44.)
func TestMessageMatchWholeAddress(t *testing.T) {
	for _, tt := range []struct {
		addr string
		h    string
		want bool
	}{
		{"/time", "/time/str", false},
		{"/foo/bar", "/foo/bar/baz", false},
		{"/time", "/foo/time", false},
		{"/time", "/time", true},
		{"/time/str", "/time/str", true},
		{"/a/b", "/a/b", true},
		{"/a/*", "/a/b", true},
	} {
		if got := NewMessage(tt.addr).Match(tt.h); got != tt.want {
			t.Errorf("NewMessage(%q).Match(%q) = %v, want %v", tt.addr, tt.h, got, tt.want)
		}
	}
}

// TestMessageMatchInvalidPattern checks that a message address that is not a
// valid pattern expression (e.g. a stray '\' or an unbalanced '[') never
// matches and does not panic. Before the fix, getRegEx used MustCompile, so a
// garbled datagram could panic inside the Dispatch goroutine and take down
// the whole process. (Same crash class as issue #29.)
func TestMessageMatchInvalidPattern(t *testing.T) {
	for _, addr := range []string{`/a\q`, `/a[b`, `/a+c/`, `/x\x`} {
		if NewMessage(addr).Match("/anything") {
			t.Errorf("NewMessage(%q).Match returned true for invalid pattern", addr)
		}
	}
}

// TestServeSurvivesMalformedDatagram checks that a server dispatches messages
// received after a malformed datagram, instead of terminating Serve.
// (Issue #29.)
func TestServeSurvivesMalformedDatagram(t *testing.T) {
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	addr := conn.LocalAddr().String()

	d := NewStandardDispatcher()
	received := make(chan *Message, 2)
	if err := d.AddMsgHandler("/ok", func(msg *Message) {
		received <- msg
	}); err != nil {
		t.Fatal(err)
	}

	s := &Server{Dispatcher: d}
	served := make(chan error, 1)
	go func() { served <- s.Serve(conn) }()

	client, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	raddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		t.Fatal(err)
	}

	// Send a malformed datagram first, then a truncated one (a parse error,
	// not just a nil packet), then a valid message.
	if _, err := client.WriteTo([]byte{0x00, 0x01, 0x02}, raddr); err != nil {
		t.Fatal(err)
	}
	if _, err := client.WriteTo([]byte("/ok\x00,s\x00\x00te"), raddr); err != nil {
		t.Fatal(err)
	}
	msg := NewMessage("/ok", int32(1))
	data, err := msg.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.WriteTo(data, raddr); err != nil {
		t.Fatal(err)
	}

	select {
	case m := <-received:
		if m.Address != "/ok" {
			t.Errorf("received address = %q, want /ok", m.Address)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not dispatch message after malformed datagram")
	}

	// Close the connection so Serve returns.
	conn.Close()
	select {
	case <-served:
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after connection close")
	}
}

// TestReceivePacketReturnsParseError checks that the one-shot ReceivePacket
// API still reports parse errors to its caller, rather than swallowing them
// the way the serving loop does. It also covers the timetag read error path
// in readArguments, which used to return nil instead of the error.
func TestReceivePacketReturnsParseError(t *testing.T) {
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	addr := conn.LocalAddr().String()

	client, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	raddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		t.Fatal(err)
	}

	s := &Server{}
	// A truncated message: typetags announce a string but the data is cut.
	if _, err := client.WriteTo([]byte("/a\x00\x00,s\x00\x00te"), raddr); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReceivePacket(conn); err == nil {
		t.Fatal("expected parse error from ReceivePacket, got nil")
	}

	// A truncated timetag argument: readArguments must return the error.
	// Message with typetags ",t" but only 4 bytes of the 8-byte timetag.
	if _, err := client.WriteTo([]byte("/a\x00\x00,t\x00\x00\x00\x00\x00\x00"), raddr); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReceivePacket(conn); err == nil {
		t.Fatal("expected error for truncated timetag, got nil")
	}
}

// TestDispatcherDynamicAddMsgHandler checks that a handler can be added
// while the dispatcher is serving, without racing concurrent Dispatch calls.
// (Issue #64.)
func TestDispatcherDynamicAddMsgHandler(t *testing.T) {
	d := NewStandardDispatcher()
	if err := d.AddMsgHandler("/dyn", func(msg *Message) {}); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				d.Dispatch(NewMessage("/dyn"))
			}
		}
	}()

	// Register a second handler while dispatching is in flight.
	second := make(chan struct{}, 1)
	if err := d.AddMsgHandler("/dyn2", func(msg *Message) {
		select {
		case second <- struct{}{}:
		default:
		}
	}); err != nil {
		t.Fatal(err)
	}
	d.Dispatch(NewMessage("/dyn2"))

	select {
	case <-second:
	case <-time.After(2 * time.Second):
		t.Fatal("handler added after Serve started was not invoked")
	}
	close(stop)
	wg.Wait()
}
