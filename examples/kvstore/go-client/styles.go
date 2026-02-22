package main

import "github.com/charmbracelet/lipgloss"

var (
	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("170"))

	statusConnected = lipgloss.NewStyle().
			Foreground(lipgloss.Color("42"))

	statusDisconnected = lipgloss.NewStyle().
				Foreground(lipgloss.Color("196"))

	statusLabel = lipgloss.NewStyle().
			Foreground(lipgloss.Color("241"))

	helpStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("241"))

	logStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("245"))

	sectionTitle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("69"))

	inspectorKey = lipgloss.NewStyle().
			Foreground(lipgloss.Color("220"))

	inspectorLabel = lipgloss.NewStyle().
			Foreground(lipgloss.Color("241"))

	promptRepl = lipgloss.NewStyle().
			Foreground(lipgloss.Color("42")).
			Bold(true)

	promptBrowser = lipgloss.NewStyle().
			Foreground(lipgloss.Color("69")).
			Bold(true)

	errorStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("196"))
)
