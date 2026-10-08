package servers

import (
	"context"
	"errors"
	"sync/atomic"

	"e2e-rpc-test/internal/cap_passing"
)

// TokenHostServer serves the pass_back and pipelined_params scenarios (the
// SERVER side); the client picks the flow.
//
// mint() hands out host-owned Tokens. check() calls the Token it is given and
// reports the tag it got and whether that call reached one of our own Tokens:
// every hostToken counts its calls in `calls`. A Token the client passes back
// arrives as `receiverHosted` and go-capnp resolves it to our own export; a
// Token pipelined from an unanswered mint() arrives as `receiverAnswer` and
// resolves through that answer. When the pipelined answer failed (mintFail()),
// the Token is an error client and check() fails with mintFail()'s error.
type TokenHostServer struct {
	calls atomic.Uint64
}

// mintFailReason is the error mintFail() returns. Clients look for
// "deliberate mint failure" in it.
const mintFailReason = "pipelined_params: deliberate mint failure"

// NewTokenHostClient returns a TokenHost bootstrap client backed by a fresh
// TokenHostServer.
func NewTokenHostClient() cap_passing.TokenHost {
	return cap_passing.TokenHost_ServerToClient(&TokenHostServer{})
}

type hostToken struct {
	host *TokenHostServer
	tag  uint32
}

func (t *hostToken) Tag(ctx context.Context, call cap_passing.Token_tag) error {
	t.host.calls.Add(1)
	res, err := call.AllocResults()
	if err != nil {
		return err
	}
	res.SetTag(t.tag)
	return nil
}

func (s *TokenHostServer) Mint(ctx context.Context, call cap_passing.TokenHost_mint) error {
	res, err := call.AllocResults()
	if err != nil {
		return err
	}
	return res.SetToken(cap_passing.Token_ServerToClient(&hostToken{host: s, tag: call.Args().Tag()}))
}

func (s *TokenHostServer) Check(ctx context.Context, call cap_passing.TokenHost_check) error {
	token := call.Args().Token().AddRef()
	defer token.Release()

	// Release the receive slot so the tag() call below can be serviced.
	call.Go()

	before := s.calls.Load()
	fut, release := token.Tag(ctx, nil)
	defer release()
	tagRes, err := fut.Struct()
	if err != nil {
		return err
	}

	res, err := call.AllocResults()
	if err != nil {
		return err
	}
	res.SetTag(tagRes.Tag())
	res.SetLocal(s.calls.Load() == before+1)
	return nil
}

func (s *TokenHostServer) Echo(ctx context.Context, call cap_passing.TokenHost_echo) error {
	res, err := call.AllocResults()
	if err != nil {
		return err
	}
	// SetToken steals the reference it is given.
	return res.SetToken(call.Args().Token().AddRef())
}

func (s *TokenHostServer) MintFail(ctx context.Context, call cap_passing.TokenHost_mintFail) error {
	return errors.New(mintFailReason)
}

var (
	_ cap_passing.TokenHost_Server = (*TokenHostServer)(nil)
	_ cap_passing.Token_Server     = (*hostToken)(nil)
)
