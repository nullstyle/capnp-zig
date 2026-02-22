package main

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
)

const (
	maxLogLines      = 12
	defaultListLimit = 25
	maxHistoryItems  = 200
)

// Mode is the UI mode.
type Mode int

const (
	ModeRepl    Mode = iota
	ModeBrowser Mode = iota
)

// Model is the bubbletea model. All methods use pointer receivers so that
// *Model satisfies tea.Model and the program reference is shared.
type Model struct {
	// Connection
	rpc        *RPCClient
	program    *tea.Program
	connected  bool
	subscribed bool
	pendingRPC bool
	addr       string

	// UI mode
	mode Mode

	// REPL
	textInput  textinput.Model
	history    []string
	histCursor int // -1 = not in history
	histDraft  string

	// Browser
	table   table.Model
	entries []BrowserEntry
	prefix  string
	limit   uint32

	// Inspector
	inspKey     string
	inspValue   []byte
	inspVersion uint64
	inspLoaded  bool

	// Pending actions
	pendingSyncWatchedKeys bool
	pendingRemoteReset     *PendingRemoteReset

	// Logs + status
	logs   []string
	status string
	width  int
	height int
}

// PendingRemoteReset stores a queued remote restore reset.
type PendingRemoteReset struct {
	RestoredBackupID uint32
	NextVersion      uint64
}

// NewModel creates a new Model with the given server address.
func NewModel(addr string) *Model {
	ti := textinput.New()
	ti.Prompt = "repl> "
	ti.Placeholder = "help | ls | get <k> | put/del/batch ... | backup/restore ..."
	ti.Focus()
	ti.CharLimit = 1024

	t := newBrowserTable(80)

	return &Model{
		addr:       addr,
		mode:       ModeRepl,
		textInput:  ti,
		table:      t,
		histCursor: -1,
		prefix:     "",
		limit:      defaultListLimit,
		status:     "initializing...",
	}
}

// Init implements tea.Model.
func (m *Model) Init() tea.Cmd {
	return tea.Batch(
		textinput.Blink,
		connectCmd(m.addr),
	)
}

// Update implements tea.Model.
func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {

	case tea.KeyMsg:
		return m.handleKey(msg)

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.table.SetColumns(browserColumns(msg.Width))
		return m, nil

	case connectedWithClient:
		m.rpc = msg.client
		m.connected = true
		m.log("connected to %s", m.addr)
		m.status = "connected"
		if m.program != nil {
			return m, subscribeCmd(m.rpc, m.program)
		}
		return m, listCmd(m.rpc, m.prefix, m.limit)

	case ConnectErrorMsg:
		m.log("connect error: %v", msg.Err)
		m.status = fmt.Sprintf("connect error: %v", msg.Err)
		return m, nil

	case SubscribeResultMsg:
		if msg.Err != nil {
			m.log("subscribe error: %v", msg.Err)
			m.status = fmt.Sprintf("subscribe error: %v", msg.Err)
		} else {
			m.subscribed = true
			m.log("subscribed to notifications")
			m.status = "subscribed"
		}
		return m, listCmd(m.rpc, m.prefix, m.limit)

	case GetResultMsg:
		m.pendingRPC = false
		if msg.Err != nil {
			m.log("get error: %v", msg.Err)
			m.status = fmt.Sprintf("get error: %v", msg.Err)
		} else if !msg.Found {
			m.clearInspector()
			m.log("GET %q -> not found", msg.Key)
			m.status = "get: key not found"
		} else {
			m.setInspector(msg.Key, msg.Value, msg.Version)
			m.log("GET %q -> version %d", msg.Key, msg.Version)
			m.status = fmt.Sprintf("get: found %q", msg.Key)
		}
		return m, m.completePending()

	case ListResultMsg:
		m.pendingRPC = false
		if msg.Err != nil {
			m.log("list error: %v", msg.Err)
			m.status = fmt.Sprintf("list error: %v", msg.Err)
		} else {
			m.entries = make([]BrowserEntry, len(msg.Entries))
			for i, e := range msg.Entries {
				m.entries[i] = BrowserEntry{
					Key:     e.Key,
					Version: e.Version,
					Preview: formatValuePreview(e.Value, 32),
					Value:   e.Value,
				}
			}
			m.table.SetRows(entriesToRows(m.entries))
			m.log("LIST -> %d entries", len(m.entries))
			m.status = fmt.Sprintf("list loaded %d entries", len(m.entries))
			if m.subscribed {
				m.pendingSyncWatchedKeys = true
			}
		}
		return m, m.completePending()

	case WriteBatchResultMsg:
		m.pendingRPC = false
		if msg.Err != nil {
			m.log("writeBatch error: %v", msg.Err)
			m.status = fmt.Sprintf("writeBatch error: %v", msg.Err)
		} else {
			for _, r := range msg.Results {
				if r.IsPut && r.Entry != nil {
					m.log("BATCH PUT %q -> version %d", r.Key, r.Entry.Version)
				} else {
					m.log("BATCH DELETE %q -> found=%v", r.Key, r.Deleted)
				}
			}
			updated := m.applyChanges(msg.Results)
			m.status = fmt.Sprintf("writeBatch applied=%d nextVersion=%d visibleUpdated=%d",
				msg.Applied, msg.NextVersion, updated)
		}
		return m, m.completePending()

	case KeysChangedMsg:
		if !m.connected {
			return m, nil
		}
		applied := m.applyChanges(msg.Changes)
		if applied > 0 {
			m.log("NOTIFY -> applied %d update(s)", applied)
			m.status = fmt.Sprintf("applied %d remote update(s)", applied)
		}
		return m, nil

	case StateResetRequiredMsg:
		if !m.connected {
			return m, nil
		}
		return m, m.handleRemoteReset(msg.RestoredBackupID, msg.NextVersion)

	case CreateBackupResultMsg:
		m.pendingRPC = false
		if msg.Err != nil {
			m.log("createBackup error: %v", msg.Err)
			m.status = fmt.Sprintf("createBackup error: %v", msg.Err)
		} else {
			m.log("BACKUP id=%d ts=%d size=%d files=%d count=%d",
				msg.BackupID, msg.Timestamp, msg.Size, msg.NumFiles, msg.BackupCount)
			m.status = fmt.Sprintf("backup created id=%d count=%d", msg.BackupID, msg.BackupCount)
		}
		return m, m.completePending()

	case ListBackupsResultMsg:
		m.pendingRPC = false
		if msg.Err != nil {
			m.log("listBackups error: %v", msg.Err)
			m.status = fmt.Sprintf("listBackups error: %v", msg.Err)
		} else {
			m.log("BACKUPS -> %d backup(s)", len(msg.Backups))
			for _, b := range msg.Backups {
				m.log("  id=%d ts=%d size=%d files=%d", b.BackupID, b.Timestamp, b.Size, b.NumFiles)
			}
			m.status = fmt.Sprintf("listed %d backup(s)", len(msg.Backups))
		}
		return m, m.completePending()

	case RestoreResultMsg:
		m.pendingRPC = false
		if msg.Err != nil {
			m.log("restore error: %v", msg.Err)
			m.status = fmt.Sprintf("restore error: %v", msg.Err)
		} else {
			m.clearInspector()
			m.log("RESTORE -> backup id=%d, nextVersion=%d", msg.RestoredBackupID, msg.NextVersion)
			m.status = fmt.Sprintf("restored backup id=%d", msg.RestoredBackupID)
			return m, listCmd(m.rpc, m.prefix, m.limit)
		}
		return m, m.completePending()

	case SetWatchedKeysResultMsg:
		m.pendingRPC = false
		if msg.Err != nil {
			m.status = fmt.Sprintf("setWatchedKeys error: %v", msg.Err)
		}
		return m, m.completePending()
	}

	return m, nil
}

// View implements tea.Model.
func (m *Model) View() string {
	var b strings.Builder

	// Header
	b.WriteString(titleStyle.Render("KVStore REPL + Browser"))
	b.WriteByte('\n')

	// Status bar
	connStr := statusDisconnected.Render("disconnected")
	if m.connected {
		connStr = statusConnected.Render("connected")
	}
	modeStr := "repl"
	if m.mode == ModeBrowser {
		modeStr = "browser"
	}
	pendingStr := "no"
	if m.pendingRPC {
		pendingStr = "yes"
	}
	b.WriteString(statusLabel.Render(fmt.Sprintf(
		"Connection: %s | Mode: %s | Pending RPC: %s",
		connStr, modeStr, pendingStr)))
	b.WriteByte('\n')

	b.WriteString(statusLabel.Render(fmt.Sprintf("Status: %s", m.status)))
	b.WriteByte('\n')

	b.WriteString(helpStyle.Render(
		"Keys: Tab autocomplete/switch mode | Up/Down history | Enter execute | Ctrl+C quit"))
	b.WriteString("\n\n")

	// Browser
	b.WriteString(sectionTitle.Render("Browser"))
	b.WriteByte('\n')
	b.WriteString(m.table.View())
	b.WriteString("\n\n")

	// Inspector
	b.WriteString(sectionTitle.Render("Value Inspector"))
	b.WriteByte('\n')
	b.WriteString(renderInspector(m.inspKey, m.inspValue, m.inspVersion, m.inspLoaded))
	b.WriteString("\n\n")

	// Recent Log
	b.WriteString(sectionTitle.Render("Recent Log"))
	b.WriteByte('\n')
	start := 0
	if len(m.logs) > maxLogLines {
		start = len(m.logs) - maxLogLines
	}
	for _, line := range m.logs[start:] {
		b.WriteString(logStyle.Render("- " + line))
		b.WriteByte('\n')
	}

	// Command input
	b.WriteByte('\n')
	b.WriteString(sectionTitle.Render("Command"))
	b.WriteByte('\n')
	b.WriteString(m.textInput.View())

	return b.String()
}

// handleKey processes a key event.
func (m *Model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyCtrlC:
		if m.rpc != nil {
			m.rpc.Close()
		}
		return m, tea.Quit

	case tea.KeyCtrlL:
		m.logs = nil
		m.status = "logs cleared"
		return m, nil
	}

	if m.mode == ModeRepl {
		return m.handleReplKey(msg)
	}
	return m.handleBrowserKey(msg)
}

// handleReplKey processes keys in REPL mode.
func (m *Model) handleReplKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyTab:
		input := m.textInput.Value()
		if strings.TrimSpace(input) == "" {
			m.toggleMode()
			return m, nil
		}
		completed, n := autocomplete(input, m.entries)
		if n == 0 {
			m.status = "autocomplete: no matches"
		} else if n > 1 {
			m.status = fmt.Sprintf("autocomplete: %d matches", n)
		}
		m.textInput.SetValue(completed)
		m.textInput.CursorEnd()
		return m, nil

	case tea.KeyUp:
		m.historyPrev()
		return m, nil

	case tea.KeyDown:
		m.historyNext()
		return m, nil

	case tea.KeyEnter:
		return m.submitInput()
	}

	// Let the text input handle other keys.
	if m.histCursor >= 0 {
		switch msg.Type {
		case tea.KeyRunes, tea.KeyBackspace, tea.KeyDelete:
			m.histCursor = -1
			m.histDraft = ""
		}
	}

	var cmd tea.Cmd
	m.textInput, cmd = m.textInput.Update(msg)
	return m, cmd
}

// handleBrowserKey processes keys in browser mode.
func (m *Model) handleBrowserKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyTab:
		m.toggleMode()
		return m, nil

	case tea.KeyEnter:
		return m.openSelectedEntry()

	case tea.KeyRunes:
		if msg.String() == "q" {
			if m.rpc != nil {
				m.rpc.Close()
			}
			return m, tea.Quit
		}
	}

	var cmd tea.Cmd
	m.table, cmd = m.table.Update(msg)
	return m, cmd
}

// toggleMode switches between REPL and browser modes.
func (m *Model) toggleMode() {
	if m.mode == ModeRepl {
		m.mode = ModeBrowser
		m.textInput.Blur()
		m.table.Focus()
		m.status = "browser mode"
	} else {
		m.mode = ModeRepl
		m.table.Blur()
		m.textInput.Focus()
		m.status = "repl mode"
	}
}

// submitInput processes the current REPL input.
func (m *Model) submitInput() (tea.Model, tea.Cmd) {
	input := strings.TrimSpace(m.textInput.Value())
	if input == "" {
		return m, nil
	}

	m.pushHistory(input)
	m.log("> %s", input)
	m.textInput.SetValue("")

	return m.executeCommand(input)
}

// executeCommand parses and executes a REPL command.
func (m *Model) executeCommand(line string) (tea.Model, tea.Cmd) {
	cmd, rest := parseCommand(line)
	cmdLower := strings.ToLower(cmd)

	switch cmdLower {
	case "help":
		m.log("commands: help, ls [prefix] [limit], get <key>, put <key> <value>, del <key>, batch <ops>")
		m.log("          backup [noflush], backups, restore <latest|id> [keep-logs], open [idx], quit")
		m.log("batch ops: put <k> <v>; del <k>; puthex <k> <hex>")
		m.status = "help printed"
		return m, nil

	case "quit", "exit":
		if m.rpc != nil {
			m.rpc.Close()
		}
		return m, tea.Quit

	case "open":
		return m.cmdOpen(rest)

	case "ls", "list":
		return m.cmdList(rest)

	case "get":
		return m.cmdGet(rest)

	case "put":
		return m.cmdPut(rest)

	case "del", "delete":
		return m.cmdDel(rest)

	case "batch":
		return m.cmdBatch(rest)

	case "backup":
		return m.cmdBackup(rest)

	case "backups":
		return m.cmdListBackups()

	case "restore":
		return m.cmdRestore(rest)

	default:
		m.status = fmt.Sprintf("unknown command: %s", cmd)
		return m, nil
	}
}

func (m *Model) cmdOpen(rest string) (tea.Model, tea.Cmd) {
	var idx int
	if rest != "" {
		token, _ := nextToken(rest)
		v, err := parseUint32(token)
		if err != nil {
			m.status = "open: invalid index"
			return m, nil
		}
		idx = int(v)
	} else {
		idx = m.table.Cursor()
	}

	if idx < 0 || idx >= len(m.entries) {
		m.status = "open: index out of range"
		return m, nil
	}

	return m.issueGet(m.entries[idx].Key)
}

func (m *Model) cmdList(rest string) (tea.Model, tea.Cmd) {
	prefix := m.prefix
	limit := m.limit

	if rest != "" {
		token, rest2 := nextToken(rest)
		prefix = token
		if rest2 != "" {
			ltoken, _ := nextToken(rest2)
			if ltoken != "" {
				v, err := parseUint32(ltoken)
				if err != nil {
					m.status = "ls: invalid limit"
					return m, nil
				}
				limit = v
			}
		}
	}

	m.prefix = prefix
	m.limit = limit
	return m.issueList()
}

func (m *Model) cmdGet(rest string) (tea.Model, tea.Cmd) {
	key, _ := nextToken(rest)
	if key == "" {
		m.status = "get: missing key"
		return m, nil
	}
	return m.issueGet(key)
}

func (m *Model) cmdPut(rest string) (tea.Model, tea.Cmd) {
	key, rest2 := nextToken(rest)
	if key == "" {
		m.status = "put: missing key"
		return m, nil
	}
	value := strings.TrimLeft(rest2, " \t")
	if value == "" {
		m.status = "put: missing value"
		return m, nil
	}
	return m.issueWriteBatch([]BatchOp{{Key: key, Value: []byte(value), IsPut: true}})
}

func (m *Model) cmdDel(rest string) (tea.Model, tea.Cmd) {
	key, _ := nextToken(rest)
	if key == "" {
		m.status = "del: missing key"
		return m, nil
	}
	return m.issueWriteBatch([]BatchOp{{Key: key, IsPut: false}})
}

func (m *Model) cmdBatch(rest string) (tea.Model, tea.Cmd) {
	spec := strings.TrimLeft(rest, " \t")
	if spec == "" {
		m.status = "batch: expected operations"
		return m, nil
	}
	ops, err := parseBatchSpec(spec)
	if err != nil {
		m.status = fmt.Sprintf("batch parse failed: %v", err)
		return m, nil
	}
	return m.issueWriteBatch(ops)
}

func (m *Model) cmdBackup(rest string) (tea.Model, tea.Cmd) {
	flush := true
	if rest != "" {
		token, _ := nextToken(rest)
		if strings.EqualFold(token, "noflush") {
			flush = false
		} else {
			m.status = "backup: expected optional `noflush`"
			return m, nil
		}
	}

	if !m.ensureCanSend("createBackup") {
		return m, nil
	}
	m.pendingRPC = true
	m.status = fmt.Sprintf("CREATE BACKUP flush=%v", flush)
	return m, createBackupCmd(m.rpc, flush)
}

func (m *Model) cmdListBackups() (tea.Model, tea.Cmd) {
	if !m.ensureCanSend("listBackups") {
		return m, nil
	}
	m.pendingRPC = true
	m.status = "LIST BACKUPS"
	return m, listBackupsCmd(m.rpc)
}

func (m *Model) cmdRestore(rest string) (tea.Model, tea.Cmd) {
	target, rest2 := nextToken(rest)
	if target == "" {
		m.status = "restore: expected `latest` or backup id"
		return m, nil
	}

	var backupID uint32
	if strings.EqualFold(target, "latest") {
		backupID = 0
	} else {
		v, err := parseUint32(target)
		if err != nil {
			m.status = "restore: invalid backup id"
			return m, nil
		}
		backupID = v
	}

	keepLogs := false
	if rest2 != "" {
		token, _ := nextToken(rest2)
		if strings.EqualFold(token, "keep-logs") {
			keepLogs = true
		} else {
			m.status = "restore: expected optional `keep-logs`"
			return m, nil
		}
	}

	if !m.ensureCanSend("restoreFromBackup") {
		return m, nil
	}
	m.pendingRPC = true
	m.status = fmt.Sprintf("RESTORE backupId=%d keepLogFiles=%v", backupID, keepLogs)
	return m, restoreCmd(m.rpc, backupID, keepLogs)
}

// issueGet sends a Get RPC.
func (m *Model) issueGet(key string) (tea.Model, tea.Cmd) {
	if !m.ensureCanSend("get") {
		return m, nil
	}
	m.pendingRPC = true
	m.status = fmt.Sprintf("GET %s", key)
	return m, getCmd(m.rpc, key)
}

// issueList sends a List RPC.
func (m *Model) issueList() (tea.Model, tea.Cmd) {
	if !m.ensureCanSend("list") {
		return m, nil
	}
	m.pendingRPC = true
	m.status = fmt.Sprintf("LIST prefix=%q limit=%d", m.prefix, m.limit)
	return m, listCmd(m.rpc, m.prefix, m.limit)
}

// issueWriteBatch sends a WriteBatch RPC.
func (m *Model) issueWriteBatch(ops []BatchOp) (tea.Model, tea.Cmd) {
	if !m.ensureCanSend("writeBatch") {
		return m, nil
	}
	m.pendingRPC = true
	m.status = fmt.Sprintf("WRITE BATCH ops=%d", len(ops))
	return m, writeBatchCmd(m.rpc, ops)
}

// ensureCanSend checks that we're connected and not already waiting for an RPC.
func (m *Model) ensureCanSend(name string) bool {
	if !m.connected || m.rpc == nil {
		m.status = fmt.Sprintf("%s: not connected", name)
		return false
	}
	if m.pendingRPC {
		m.status = "another RPC is in flight"
		return false
	}
	return true
}

// completePending runs any queued actions after an RPC completes.
func (m *Model) completePending() tea.Cmd {
	if m.pendingRemoteReset != nil {
		pr := m.pendingRemoteReset
		m.pendingRemoteReset = nil
		return m.doRemoteReset(pr.RestoredBackupID, pr.NextVersion)
	}

	if m.pendingSyncWatchedKeys && m.subscribed && m.connected && m.rpc != nil {
		m.pendingSyncWatchedKeys = false
		keys := make([]string, len(m.entries))
		for i, e := range m.entries {
			keys[i] = e.Key
		}
		m.pendingRPC = true
		return setWatchedKeysCmd(m.rpc, keys)
	}

	return nil
}

// handleRemoteReset processes a remote restore notification.
func (m *Model) handleRemoteReset(backupID uint32, nextVersion uint64) tea.Cmd {
	if m.pendingRPC {
		m.pendingRemoteReset = &PendingRemoteReset{
			RestoredBackupID: backupID,
			NextVersion:      nextVersion,
		}
		m.log("REMOTE RESTORE -> backup id=%d, nextVersion=%d (queued)", backupID, nextVersion)
		m.status = fmt.Sprintf("remote restore id=%d; waiting for RPC", backupID)
		return nil
	}
	return m.doRemoteReset(backupID, nextVersion)
}

// doRemoteReset clears state and reloads.
func (m *Model) doRemoteReset(backupID uint32, nextVersion uint64) tea.Cmd {
	m.entries = nil
	m.table.SetRows(nil)
	m.clearInspector()
	m.pendingSyncWatchedKeys = false

	m.log("REMOTE RESTORE -> backup id=%d, nextVersion=%d", backupID, nextVersion)
	m.status = fmt.Sprintf("remote restore id=%d; reloading", backupID)

	if !m.connected || m.rpc == nil {
		return nil
	}
	m.pendingRPC = true
	return listCmd(m.rpc, m.prefix, m.limit)
}

// applyChanges applies write results to the browser entries.
func (m *Model) applyChanges(changes []WriteResult) int {
	entries, applied := applyChangesToEntries(m.entries, changes, m.prefix, m.limit)
	m.entries = entries
	m.table.SetRows(entriesToRows(m.entries))

	for _, change := range changes {
		if m.inspLoaded && change.Key == m.inspKey {
			if change.IsPut && change.Entry != nil {
				m.setInspector(change.Key, change.Entry.Value, change.Entry.Version)
			} else {
				m.clearInspector()
			}
		}
	}

	if applied > 0 && m.subscribed {
		m.pendingSyncWatchedKeys = true
	}

	return applied
}

// openSelectedEntry gets the value for the currently selected browser entry.
func (m *Model) openSelectedEntry() (tea.Model, tea.Cmd) {
	idx := m.table.Cursor()
	if idx < 0 || idx >= len(m.entries) {
		m.status = "no selected entry"
		return m, nil
	}
	return m.issueGet(m.entries[idx].Key)
}

// setInspector updates the inspector with a value.
func (m *Model) setInspector(key string, value []byte, version uint64) {
	m.inspKey = key
	m.inspValue = make([]byte, len(value))
	copy(m.inspValue, value)
	m.inspVersion = version
	m.inspLoaded = true
}

// clearInspector resets the inspector.
func (m *Model) clearInspector() {
	m.inspKey = ""
	m.inspValue = nil
	m.inspVersion = 0
	m.inspLoaded = false
}

// History management.

func (m *Model) pushHistory(line string) {
	if line == "" {
		return
	}
	if len(m.history) > 0 && m.history[len(m.history)-1] == line {
		m.histCursor = -1
		m.histDraft = ""
		return
	}
	m.history = append(m.history, line)
	if len(m.history) > maxHistoryItems {
		m.history = m.history[1:]
	}
	m.histCursor = -1
	m.histDraft = ""
}

func (m *Model) historyPrev() {
	if len(m.history) == 0 {
		return
	}
	if m.histCursor < 0 {
		m.histDraft = m.textInput.Value()
		m.histCursor = len(m.history) - 1
	} else if m.histCursor > 0 {
		m.histCursor--
	}
	m.textInput.SetValue(m.history[m.histCursor])
	m.textInput.CursorEnd()
}

func (m *Model) historyNext() {
	if m.histCursor < 0 {
		return
	}
	if m.histCursor < len(m.history)-1 {
		m.histCursor++
		m.textInput.SetValue(m.history[m.histCursor])
		m.textInput.CursorEnd()
		return
	}
	m.histCursor = -1
	m.textInput.SetValue(m.histDraft)
	m.textInput.CursorEnd()
	m.histDraft = ""
}

// log appends a message to the log.
func (m *Model) log(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	m.logs = append(m.logs, line)
	if len(m.logs) > maxLogLines*2 {
		m.logs = m.logs[len(m.logs)-maxLogLines*2:]
	}
}
