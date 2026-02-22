package main

import (
	tea "github.com/charmbracelet/bubbletea"

	"github.com/example/kvstore-go-client/kvstore"
)

// connectCmd returns a tea.Cmd that connects to the RPC server.
func connectCmd(addr string) tea.Cmd {
	return func() tea.Msg {
		client, err := connectRPC(addr)
		if err != nil {
			return ConnectErrorMsg{Err: err}
		}
		_ = client // stored via ConnectedMsg handler
		return connectedWithClient{client: client}
	}
}

// connectedWithClient carries the RPCClient to the model.
type connectedWithClient struct {
	client *RPCClient
}

// subscribeCmd returns a tea.Cmd that subscribes for notifications.
func subscribeCmd(client *RPCClient, program *tea.Program) tea.Cmd {
	return func() tea.Msg {
		notifier := createNotifierClient(program)
		err := subscribeWithNotifier(client, notifier)
		if err != nil {
			notifier.Release()
		}
		return SubscribeResultMsg{Err: err}
	}
}

// getCmd returns a tea.Cmd that performs a Get RPC.
func getCmd(client *RPCClient, key string) tea.Cmd {
	return func() tea.Msg {
		f, release := client.client.Get(client.ctx, func(p kvstore.KvStore_get_Params) error {
			return p.SetKey(key)
		})
		defer release()

		res, err := f.Struct()
		if err != nil {
			return GetResultMsg{Key: key, Err: err}
		}

		found := res.Found()
		if !found {
			return GetResultMsg{Key: key, Found: false}
		}

		entry, err := res.Entry()
		if err != nil {
			return GetResultMsg{Key: key, Err: err}
		}

		entryKey, _ := entry.Key()
		value, _ := entry.Value()
		valueCopy := make([]byte, len(value))
		copy(valueCopy, value)

		return GetResultMsg{
			Key:     entryKey,
			Value:   valueCopy,
			Version: entry.Version(),
			Found:   true,
		}
	}
}

// listCmd returns a tea.Cmd that performs a List RPC.
func listCmd(client *RPCClient, prefix string, limit uint32) tea.Cmd {
	return func() tea.Msg {
		f, release := client.client.List(client.ctx, func(p kvstore.KvStore_list_Params) error {
			if err := p.SetPrefix(prefix); err != nil {
				return err
			}
			p.SetLimit(limit)
			return nil
		})
		defer release()

		res, err := f.Struct()
		if err != nil {
			return ListResultMsg{Err: err}
		}

		entries, err := res.Entries()
		if err != nil {
			return ListResultMsg{Err: err}
		}

		var result []ListEntry
		for i := 0; i < entries.Len(); i++ {
			e := entries.At(i)
			key, _ := e.Key()
			value, _ := e.Value()
			valueCopy := make([]byte, len(value))
			copy(valueCopy, value)
			result = append(result, ListEntry{
				Key:     key,
				Value:   valueCopy,
				Version: e.Version(),
			})
		}

		return ListResultMsg{Entries: result}
	}
}

// writeBatchCmd returns a tea.Cmd that performs a WriteBatch RPC.
func writeBatchCmd(client *RPCClient, ops []BatchOp) tea.Cmd {
	return func() tea.Msg {
		f, release := client.client.WriteBatch(client.ctx, func(p kvstore.KvStore_writeBatch_Params) error {
			opList, err := kvstore.NewWriteOp_List(p.Segment(), int32(len(ops)))
			if err != nil {
				return err
			}
			for i, op := range ops {
				wo := opList.At(i)
				if err := wo.SetKey(op.Key); err != nil {
					return err
				}
				if op.IsPut {
					if err := wo.SetPut(op.Value); err != nil {
						return err
					}
				} else {
					wo.SetDelete()
				}
			}
			return p.SetOps(opList)
		})
		defer release()

		res, err := f.Struct()
		if err != nil {
			return WriteBatchResultMsg{Err: err}
		}

		applied := res.Applied()
		nextVersion := res.NextVersion()
		results, err := res.Results()
		if err != nil {
			return WriteBatchResultMsg{Err: err}
		}

		var writeResults []WriteResult
		for i := 0; i < results.Len(); i++ {
			r := results.At(i)
			key, _ := r.Key()
			switch r.Which() {
			case kvstore.WriteOpResult_Which_put:
				entry, _ := r.Put()
				value, _ := entry.Value()
				valueCopy := make([]byte, len(value))
				copy(valueCopy, value)
				writeResults = append(writeResults, WriteResult{
					Key:   key,
					IsPut: true,
					Entry: &ListEntry{
						Key:     key,
						Value:   valueCopy,
						Version: entry.Version(),
					},
				})
			case kvstore.WriteOpResult_Which_delete:
				deleted := r.Delete()
				writeResults = append(writeResults, WriteResult{
					Key:     key,
					IsPut:   false,
					Deleted: deleted,
				})
			}
		}

		return WriteBatchResultMsg{
			Results:     writeResults,
			Applied:     applied,
			NextVersion: nextVersion,
		}
	}
}

// createBackupCmd returns a tea.Cmd that performs a CreateBackup RPC.
func createBackupCmd(client *RPCClient, flush bool) tea.Cmd {
	return func() tea.Msg {
		f, release := client.client.CreateBackup(client.ctx, func(p kvstore.KvStore_createBackup_Params) error {
			p.SetFlushBeforeBackup(flush)
			return nil
		})
		defer release()

		res, err := f.Struct()
		if err != nil {
			return CreateBackupResultMsg{Err: err}
		}

		backup, err := res.Backup()
		if err != nil {
			return CreateBackupResultMsg{Err: err}
		}

		return CreateBackupResultMsg{
			BackupID:    backup.BackupId(),
			Timestamp:   backup.Timestamp(),
			Size:        backup.Size(),
			NumFiles:    backup.NumFiles(),
			BackupCount: res.BackupCount(),
		}
	}
}

// listBackupsCmd returns a tea.Cmd that performs a ListBackups RPC.
func listBackupsCmd(client *RPCClient) tea.Cmd {
	return func() tea.Msg {
		f, release := client.client.ListBackups(client.ctx, nil)
		defer release()

		res, err := f.Struct()
		if err != nil {
			return ListBackupsResultMsg{Err: err}
		}

		backups, err := res.Backups()
		if err != nil {
			return ListBackupsResultMsg{Err: err}
		}

		var result []BackupEntry
		for i := 0; i < backups.Len(); i++ {
			b := backups.At(i)
			result = append(result, BackupEntry{
				BackupID:  b.BackupId(),
				Timestamp: b.Timestamp(),
				Size:      b.Size(),
				NumFiles:  b.NumFiles(),
			})
		}

		return ListBackupsResultMsg{Backups: result}
	}
}

// restoreCmd returns a tea.Cmd that performs a RestoreFromBackup RPC.
func restoreCmd(client *RPCClient, backupID uint32, keepLogs bool) tea.Cmd {
	return func() tea.Msg {
		f, release := client.client.RestoreFromBackup(client.ctx, func(p kvstore.KvStore_restoreFromBackup_Params) error {
			p.SetBackupId(backupID)
			p.SetKeepLogFiles(keepLogs)
			return nil
		})
		defer release()

		res, err := f.Struct()
		if err != nil {
			return RestoreResultMsg{Err: err}
		}

		return RestoreResultMsg{
			RestoredBackupID: res.RestoredBackupId(),
			NextVersion:      res.NextVersion(),
		}
	}
}

// setWatchedKeysCmd returns a tea.Cmd that calls setWatchedKeys.
func setWatchedKeysCmd(client *RPCClient, keys []string) tea.Cmd {
	return func() tea.Msg {
		err := setWatchedKeysRPC(client, keys)
		return SetWatchedKeysResultMsg{Err: err}
	}
}

// BatchOp represents a single write operation for the batch command.
type BatchOp struct {
	Key   string
	Value []byte
	IsPut bool // true=put, false=delete
}
