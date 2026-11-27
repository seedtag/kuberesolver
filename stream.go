package kuberesolver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"google.golang.org/grpc/grpclog"
)

// Interface can be implemented by anything that knows how to watch and report changes.
type watchInterface interface {
	// Stops watching. Will close the channel returned by ResultChan(). Releases
	// any resources used by the watch.
	Stop()

	// Returns a chan which will receive all the events. If an error occurs
	// or Stop() is called, this channel will be closed, in which case the
	// watch should be completely cleaned up.
	ResultChan() <-chan Event
}

// StreamWatcher turns any stream for which you can write a Decoder interface
// into a watch.Interface.
type streamWatcher struct {
	result  chan Event
	done    chan struct{} // closed by Stop
	r       io.ReadCloser
	decoder *json.Decoder
	sync.Mutex
	stopped bool
}

// NewStreamWatcher creates a StreamWatcher from the given io.ReadClosers.
func newStreamWatcher(r io.ReadCloser) watchInterface {
	sw := &streamWatcher{
		r:       r,
		decoder: json.NewDecoder(r),
		result:  make(chan Event),
		done:    make(chan struct{}),
	}
	go sw.receive()

	return sw
}

// ResultChan implements Interface.
func (sw *streamWatcher) ResultChan() <-chan Event {
	return sw.result
}

// Stop implements Interface.
func (sw *streamWatcher) Stop() {
	sw.Lock()
	defer sw.Unlock()

	if !sw.stopped {
		sw.stopped = true
		close(sw.done)
		_ = sw.r.Close()
	}
}

// stopping returns true if Stop() was called previously.
func (sw *streamWatcher) stopping() bool {
	sw.Lock()
	defer sw.Unlock()

	return sw.stopped
}

// receive reads result from the decoder in a loop and sends down the result channel.
func (sw *streamWatcher) receive() {
	defer close(sw.result)
	defer sw.Stop()

	for {
		obj, err := sw.Decode()
		if err != nil {
			// Ignore expected error.
			if sw.stopping() {
				return
			}

			switch err {
			case io.EOF:
				// watch closed normally
			case context.Canceled:
				// canceled normally
			case io.ErrUnexpectedEOF:
				grpclog.Infof("kuberesolver: Unexpected EOF during watch stream event decoding: %v", err)
			default:
				grpclog.Infof("kuberesolver: Unable to decode an event from the watch stream: %v", err)
			}

			return
		}

		// Don't block forever on a reader that stopped listening.
		select {
		case sw.result <- obj:
		case <-sw.done:
			return
		}
	}
}

// Decode blocks until it can return the next object in the writer. Returns an error
// if the writer is closed or an object can't be decoded.
func (sw *streamWatcher) Decode() (Event, error) {
	// The object is an EndpointSlice, or a Status for ERROR events. Decoding
	// into both at once avoids a second pass over the JSON.
	var got struct {
		Type   EventType `json:"type"`
		Object struct {
			EndpointSlice
			Status
		} `json:"object"`
	}
	if err := sw.decoder.Decode(&got); err != nil {
		return Event{}, err
	}

	switch got.Type {
	case Added, Modified, Deleted:
		return Event{Type: got.Type, Object: got.Object.EndpointSlice}, nil
	case Error:
		return Event{Type: got.Type, Status: got.Object.Status}, nil
	default:
		return Event{}, fmt.Errorf("got invalid watch event type: %v", got.Type)
	}
}
