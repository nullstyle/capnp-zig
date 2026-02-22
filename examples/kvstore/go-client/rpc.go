package main

import (
	"context"
	"fmt"
	"net"

	tea "github.com/charmbracelet/bubbletea"

	capnp "capnproto.org/go/capnp/v3"
	"capnproto.org/go/capnp/v3/rpc"

	"github.com/example/kvstore-go-client/kvstore"
)

// RPCClient wraps a Cap'n Proto RPC connection to the kvstore server.
type RPCClient struct {
	conn   *rpc.Conn
	client kvstore.KvStore
	ctx    context.Context
	cancel context.CancelFunc
}

// Close shuts down the RPC connection.
func (c *RPCClient) Close() {
	if c.cancel != nil {
		c.cancel()
	}
	c.client.Release()
	if c.conn != nil {
		c.conn.Close()
	}
}

// connectRPC dials the server and bootstraps the KvStore capability.
func connectRPC(addr string) (*RPCClient, error) {
	netConn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	transport := rpc.NewStreamTransport(netConn)
	conn := rpc.NewConn(transport, nil)

	ctx, cancel := context.WithCancel(context.Background())

	bootstrapClient := conn.Bootstrap(ctx)
	client := kvstore.KvStore(bootstrapClient)

	return &RPCClient{
		conn:   conn,
		client: client,
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

// notifierServer implements KvClientNotifier_Server and sends bubbletea messages.
type notifierServer struct {
	program *tea.Program
}

func (n *notifierServer) KeysChanged(_ context.Context, call kvstore.KvClientNotifier_keysChanged) error {
	args := call.Args()
	changes, err := args.Changes()
	if err != nil {
		return err
	}

	var results []WriteResult
	for i := 0; i < changes.Len(); i++ {
		change := changes.At(i)
		key, _ := change.Key()
		switch change.Which() {
		case kvstore.WriteOpResult_Which_put:
			entry, _ := change.Put()
			value, _ := entry.Value()
			valueCopy := make([]byte, len(value))
			copy(valueCopy, value)
			results = append(results, WriteResult{
				Key:   key,
				IsPut: true,
				Entry: &ListEntry{
					Key:     key,
					Value:   valueCopy,
					Version: entry.Version(),
				},
			})
		case kvstore.WriteOpResult_Which_delete:
			deleted := change.Delete()
			results = append(results, WriteResult{
				Key:     key,
				IsPut:   false,
				Deleted: deleted,
			})
		}
	}

	n.program.Send(KeysChangedMsg{Changes: results})
	return nil
}

func (n *notifierServer) StateResetRequired(_ context.Context, call kvstore.KvClientNotifier_stateResetRequired) error {
	args := call.Args()
	n.program.Send(StateResetRequiredMsg{
		RestoredBackupID: args.RestoredBackupId(),
		NextVersion:      args.NextVersion(),
	})
	return nil
}

// createNotifierClient creates a KvClientNotifier client backed by the notifier server.
func createNotifierClient(program *tea.Program) kvstore.KvClientNotifier {
	srv := &notifierServer{program: program}
	return kvstore.KvClientNotifier_ServerToClient(srv)
}

// subscribeWithNotifier calls Subscribe on the KvStore with a notifier capability.
func subscribeWithNotifier(client *RPCClient, notifier kvstore.KvClientNotifier) error {
	f, release := client.client.Subscribe(client.ctx, func(p kvstore.KvStore_subscribe_Params) error {
		return p.SetNotifier(notifier)
	})
	defer release()
	_, err := f.Struct()
	return err
}

// setWatchedKeysRPC calls SetWatchedKeys on the KvStore.
func setWatchedKeysRPC(client *RPCClient, keys []string) error {
	f, release := client.client.SetWatchedKeys(client.ctx, func(p kvstore.KvStore_setWatchedKeys_Params) error {
		keyList, err := capnp.NewTextList(p.Segment(), int32(len(keys)))
		if err != nil {
			return err
		}
		for i, k := range keys {
			if err := keyList.Set(i, k); err != nil {
				return err
			}
		}
		return p.SetKeys(keyList)
	})
	defer release()
	_, err := f.Struct()
	return err
}
