package main

import (
	"fmt"
	"strings"
)

// renderInspector returns a string rendering of the value inspector.
func renderInspector(key string, value []byte, version uint64, loaded bool) string {
	if !loaded {
		return inspectorLabel.Render("No value loaded. Use `get <key>` or `open` from browser.")
	}

	var b strings.Builder

	b.WriteString(inspectorLabel.Render("Key: "))
	b.WriteString(inspectorKey.Render(key))
	b.WriteByte('\n')

	b.WriteString(inspectorLabel.Render(fmt.Sprintf("Version: %d", version)))
	b.WriteByte('\n')

	b.WriteString(inspectorLabel.Render(fmt.Sprintf("Bytes: %d", len(value))))
	b.WriteByte('\n')

	if isPrintableASCII(value) {
		shown := 120
		if shown > len(value) {
			shown = len(value)
		}
		if len(value) > shown {
			b.WriteString(inspectorLabel.Render(fmt.Sprintf(`Text: "%s..."`, string(value[:shown]))))
		} else {
			b.WriteString(inspectorLabel.Render(fmt.Sprintf(`Text: "%s"`, string(value))))
		}
	} else {
		b.WriteString(inspectorLabel.Render("Text: <binary>"))
	}
	b.WriteByte('\n')

	shown := 48
	if shown > len(value) {
		shown = len(value)
	}
	hex := fmt.Sprintf("%x", value[:shown])
	if len(value) > shown {
		b.WriteString(inspectorLabel.Render(fmt.Sprintf("Hex: 0x%s...", hex)))
	} else {
		b.WriteString(inspectorLabel.Render(fmt.Sprintf("Hex: 0x%s", hex)))
	}

	return b.String()
}
