package servers

import (
	"context"
	"errors"
	"sync"

	capnp "capnproto.org/go/capnp/v3"

	"e2e-rpc-test/internal/resolve_disembargo"
)

// ReflectorServer implements the reflected-capability resolve/disembargo
// scenario (the SERVER / reflector side).
//
// reflect(target): imports the caller's CallSequence, RETAINS it past the call
// via AddRef(), exports an UNRESOLVED promise capability (capnp.NewLocalPromise),
// and returns that promise as `promise` — but does NOT resolve it yet. The
// caller pipelines getNumber calls on the returned promise; those calls PARK at
// this reflector because the promise has no target.
//
// resolveNow(): fulfills the promise with the retained target. That reflected
// resolution forwards the parked pipelined calls back out to the caller's
// CallSequence and drives the caller's senderLoopback -> receiverLoopback
// Disembargo before it issues a direct call.
//
// State is per-connection: the Go server creates one ReflectorServer per
// accepted connection (see cmd/server/main.go), and the e2e harness runs one
// connection at a time and calls reflect() exactly once before resolveNow(), so
// a single instance guarded by a mutex is sufficient.
type ReflectorServer struct {
	mu sync.Mutex
	// The retained caller-hosted CallSequence (held past reflect() via AddRef).
	target *resolve_disembargo.CallSequence
	// The local promise client and its resolver handed back from reflect(). We
	// retain our OWN reference to the promise client (`promise`) for the whole
	// connection: calls that flow through the promise after resolution (the
	// post-embargo DIRECT getNumber) require the promise client to stay alive,
	// and the go-capnp localPromise ties the lifetime of the fulfilled target to
	// the lifetime of the promise client. If our only promise reference were the
	// one stolen into the reflect results, the caller's Finish for the reflect
	// question would release it — dropping the fulfilled target to null and
	// failing later calls with "call on null client".
	promise  resolve_disembargo.CallSequence
	resolver capnp.Resolver[resolve_disembargo.CallSequence]
}

// NewReflectorClient returns a Reflector bootstrap client backed by a fresh
// ReflectorServer.
func NewReflectorClient() resolve_disembargo.Reflector {
	return resolve_disembargo.Reflector_ServerToClient(&ReflectorServer{})
}

// Reflect imports and retains the caller's CallSequence, exports an unresolved
// promise, and returns it as `promise`.
func (s *ReflectorServer) Reflect(ctx context.Context, call resolve_disembargo.Reflector_reflect) error {
	// Retain the caller's CallSequence import so it outlives this call; it is
	// later named as the promise's resolution target in resolveNow(). AddRef()
	// takes an owning reference that survives past the call return.
	target := call.Args().Target().AddRef()

	// Export an unresolved promise. NewLocalPromise buffers calls made on the
	// returned client until the resolver is fulfilled, then forwards them. On the
	// wire the promise export is encoded as senderPromise, so the caller imports
	// it as a pipelinable promise.
	promise, resolver := capnp.NewLocalPromise[resolve_disembargo.CallSequence]()

	s.mu.Lock()
	s.target = &target
	s.promise = promise
	s.resolver = resolver
	s.mu.Unlock()

	res, err := call.AllocResults()
	if err != nil {
		return err
	}
	// Hand back the promise as the `promise` result capability. SetPromise ->
	// CapTable().Add STEALS the client reference it is given, so pass a fresh
	// AddRef() and keep our own `s.promise` reference alive for the connection.
	if err := res.SetPromise(promise.AddRef()); err != nil {
		return err
	}
	return nil
}

// ResolveNow fulfills the promise returned by the most recent reflect() with the
// retained target, forwarding any parked pipelined calls back to the caller.
func (s *ReflectorServer) ResolveNow(ctx context.Context, call resolve_disembargo.Reflector_resolveNow) error {
	s.mu.Lock()
	target := s.target
	resolver := s.resolver
	// Clear only the resolver so a double resolveNow() is a no-op; KEEP the
	// retained target and promise references (released only at connection
	// teardown) so the resolved cap stays alive for later direct calls.
	s.resolver = nil
	s.mu.Unlock()

	if target == nil || resolver == nil {
		return errors.New("resolveNow called before reflect()")
	}

	// resolveNow() has empty results (); allocating them is not required.

	// Fulfill the promise with the retained target. This resolves the promise
	// export to the caller's CallSequence and replays every parked pipelined
	// getNumber by forwarding it back out to that CallSequence, which triggers
	// the caller's Disembargo handshake.
	//
	// Fulfill -> CapTable().Add STEALS the reference it is given, so hand it a
	// fresh AddRef() and keep our own retained `target` reference alive.
	resolver.Fulfill(target.AddRef())
	return nil
}

// Shutdown releases the references retained across calls. It runs when the
// Reflector bootstrap capability is released at connection teardown, by which
// point all calls through the resolved promise have completed.
func (s *ReflectorServer) Shutdown() {
	s.mu.Lock()
	target := s.target
	promise := s.promise
	s.target = nil
	s.promise = resolve_disembargo.CallSequence{}
	s.resolver = nil
	s.mu.Unlock()

	if target != nil {
		target.Release()
	}
	capnp.Client(promise).Release()
}

var _ resolve_disembargo.Reflector_Server = (*ReflectorServer)(nil)
