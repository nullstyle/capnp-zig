package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/lipgloss"
)

// BrowserEntry represents a single row in the browser table.
type BrowserEntry struct {
	Key     string
	Version uint64
	Preview string
	Value   []byte
}

// newBrowserTable creates a configured table for the browser.
func newBrowserTable(width int) table.Model {
	columns := browserColumns(width)
	t := table.New(
		table.WithColumns(columns),
		table.WithRows(nil),
		table.WithFocused(false),
		table.WithHeight(12),
	)

	s := table.DefaultStyles()
	s.Header = s.Header.
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("240")).
		BorderBottom(true).
		Bold(true)
	s.Selected = s.Selected.
		Foreground(lipgloss.Color("229")).
		Background(lipgloss.Color("57")).
		Bold(false)
	t.SetStyles(s)
	return t
}

// browserColumns returns column definitions for a given width.
func browserColumns(width int) []table.Column {
	if width < 60 {
		width = 60
	}
	keyW := width * 30 / 100
	verW := 10
	prevW := width - keyW - verW - 6 // padding
	if prevW < 10 {
		prevW = 10
	}
	return []table.Column{
		{Title: "Key", Width: keyW},
		{Title: "Version", Width: verW},
		{Title: "Preview", Width: prevW},
	}
}

// entriesToRows converts BrowserEntry slices to table rows.
func entriesToRows(entries []BrowserEntry) []table.Row {
	rows := make([]table.Row, len(entries))
	for i, e := range entries {
		rows[i] = table.Row{e.Key, fmt.Sprintf("%d", e.Version), e.Preview}
	}
	return rows
}

// formatValuePreview returns a human-readable preview of a byte value.
func formatValuePreview(value []byte, maxLen int) string {
	if isPrintableASCII(value) {
		if len(value) > maxLen {
			return fmt.Sprintf(`"%s..." (%d bytes)`, string(value[:maxLen]), len(value))
		}
		return fmt.Sprintf(`"%s" (%d bytes)`, string(value), len(value))
	}

	shown := maxLen / 2
	if shown > len(value) {
		shown = len(value)
	}
	hex := fmt.Sprintf("%x", value[:shown])
	if len(value) > shown {
		return fmt.Sprintf("0x%s... (%d bytes)", hex, len(value))
	}
	return fmt.Sprintf("0x%s (%d bytes)", hex, len(value))
}

// isPrintableASCII checks if all bytes are printable ASCII.
func isPrintableASCII(b []byte) bool {
	for _, c := range b {
		if c < 32 || c > 126 {
			return false
		}
	}
	return true
}

// findEntryIndex finds the index of the entry with the given key.
func findEntryIndex(entries []BrowserEntry, key string) int {
	for i, e := range entries {
		if e.Key == key {
			return i
		}
	}
	return -1
}

// applyChangesToEntries applies write results to the browser entries list.
// Returns the updated entries and a count of changes applied.
func applyChangesToEntries(entries []BrowserEntry, changes []WriteResult, prefix string, limit uint32) ([]BrowserEntry, int) {
	applied := 0

	for _, change := range changes {
		idx := findEntryIndex(entries, change.Key)

		if change.IsPut && change.Entry != nil {
			preview := formatValuePreview(change.Entry.Value, 32)
			if idx >= 0 {
				// Update existing entry.
				entries[idx].Version = change.Entry.Version
				entries[idx].Preview = preview
				entries[idx].Value = change.Entry.Value
				applied++
			} else {
				// Try to insert if it matches the prefix and we're under limit.
				if !strings.HasPrefix(change.Key, prefix) {
					continue
				}
				newEntry := BrowserEntry{
					Key:     change.Key,
					Version: change.Entry.Version,
					Preview: preview,
					Value:   change.Entry.Value,
				}
				entries = append(entries, newEntry)
				// Sort by key.
				sort.Slice(entries, func(i, j int) bool {
					return entries[i].Key < entries[j].Key
				})
				// Trim to limit.
				if limit > 0 && uint32(len(entries)) > limit {
					entries = entries[:limit]
				}
				applied++
			}
		} else {
			// Delete.
			if idx >= 0 {
				entries = append(entries[:idx], entries[idx+1:]...)
				applied++
			}
		}
	}

	return entries, applied
}
