package kuberesolver

import (
	"fmt"
	"testing"

	"google.golang.org/grpc/resolver"
)

// discardConn accepts every state and keeps nothing, so benchmarks measure
// the resolver and not the test double.
type discardConn struct {
	resolver.ClientConn // unused methods; calling one panics
}

func (discardConn) UpdateState(resolver.State) error { return nil }

// BenchmarkApplyEvent measures one MODIFIED watch event for a Service whose
// pods are split into slices of 100, as the EndpointSlice controller does.
func BenchmarkApplyEvent(b *testing.B) {
	for _, total := range []int{200, 2_000, 10_000} {
		b.Run(fmt.Sprintf("pods=%d", total), func(b *testing.B) {
			k := newTestResolver(b, discardConn{}, nil)

			for i := range total / 100 {
				k.applyEvent(Event{Type: Added, Object: slice(fmt.Sprintf("svc-%03d", i), pods(i*100, 100)...)})
			}

			event := Event{Type: Modified, Object: slice("svc-000", pods(0, 100)...)}

			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				k.applyEvent(event)
			}
		})
	}
}
