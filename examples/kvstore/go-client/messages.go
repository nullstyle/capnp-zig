package main

// All tea.Msg types for RPC responses and notifications.

// ConnectedMsg is sent when the RPC connection is established and bootstrapped.
type ConnectedMsg struct{}

// ConnectErrorMsg is sent when the connection fails.
type ConnectErrorMsg struct {
	Err error
}

// GetResultMsg is the result of a Get RPC call.
type GetResultMsg struct {
	Key     string
	Value   []byte
	Version uint64
	Found   bool
	Err     error
}

// ListResultMsg is the result of a List RPC call.
type ListResultMsg struct {
	Entries []ListEntry
	Err     error
}

// ListEntry is a single entry from a list result.
type ListEntry struct {
	Key     string
	Value   []byte
	Version uint64
}

// WriteBatchResultMsg is the result of a WriteBatch RPC call.
type WriteBatchResultMsg struct {
	Results     []WriteResult
	Applied     uint32
	NextVersion uint64
	Err         error
}

// WriteResult is a single result from a write batch.
type WriteResult struct {
	Key     string
	IsPut   bool
	Entry   *ListEntry // non-nil for put results
	Deleted bool       // for delete results
}

// SubscribeResultMsg is sent when subscribe completes.
type SubscribeResultMsg struct {
	Err error
}

// SetWatchedKeysResultMsg is sent when setWatchedKeys completes.
type SetWatchedKeysResultMsg struct {
	Err error
}

// KeysChangedMsg is sent from the notifier server when keys change.
type KeysChangedMsg struct {
	Changes []WriteResult
}

// StateResetRequiredMsg is sent from the notifier when a restore occurs.
type StateResetRequiredMsg struct {
	RestoredBackupID uint32
	NextVersion      uint64
}

// CreateBackupResultMsg is the result of a CreateBackup RPC call.
type CreateBackupResultMsg struct {
	BackupID    uint32
	Timestamp   int64
	Size        uint64
	NumFiles    uint32
	BackupCount uint32
	Err         error
}

// ListBackupsResultMsg is the result of a ListBackups RPC call.
type ListBackupsResultMsg struct {
	Backups []BackupEntry
	Err     error
}

// BackupEntry is a single backup info entry.
type BackupEntry struct {
	BackupID  uint32
	Timestamp int64
	Size      uint64
	NumFiles  uint32
}

// RestoreResultMsg is the result of a RestoreFromBackup RPC call.
type RestoreResultMsg struct {
	RestoredBackupID uint32
	NextVersion      uint64
	Err              error
}
