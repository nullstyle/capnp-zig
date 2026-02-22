package main

import (
	"flag"
	"fmt"
	"os"

	tea "github.com/charmbracelet/bubbletea"
)

func main() {
	host := flag.String("host", "127.0.0.1", "server host")
	port := flag.Int("port", 9000, "server port")
	flag.Parse()

	addr := fmt.Sprintf("%s:%d", *host, *port)

	model := NewModel(addr)
	p := tea.NewProgram(model, tea.WithAltScreen())

	// Store program reference so the notifier server can use program.Send().
	// Since model is a *Model (pointer receiver), mutations are shared.
	model.program = p

	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
