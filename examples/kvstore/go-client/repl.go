package main

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

var commandCandidates = []string{
	"help", "ls", "list", "get", "put", "del", "delete",
	"backup", "backups", "restore", "open", "batch", "quit", "exit",
}

var batchOpCandidates = []string{
	"put", "puthex", "del", "delete",
}

// parseCommand parses a REPL line into a command name and remaining args.
func parseCommand(line string) (cmd string, rest string) {
	line = strings.TrimSpace(line)
	idx := strings.IndexAny(line, " \t")
	if idx < 0 {
		return line, ""
	}
	return line[:idx], strings.TrimLeft(line[idx:], " \t")
}

// nextToken extracts the next whitespace-delimited token.
func nextToken(s string) (token, rest string) {
	s = strings.TrimLeft(s, " \t")
	if s == "" {
		return "", ""
	}
	idx := strings.IndexAny(s, " \t")
	if idx < 0 {
		return s, ""
	}
	return s[:idx], s[idx:]
}

// parseBatchSpec parses a semicolon-separated batch specification.
func parseBatchSpec(spec string) ([]BatchOp, error) {
	var ops []BatchOp

	segments := strings.Split(spec, ";")
	for _, raw := range segments {
		segment := strings.TrimSpace(raw)
		if segment == "" {
			continue
		}

		opName, rest := nextToken(segment)
		opLower := strings.ToLower(opName)

		switch opLower {
		case "put":
			key, rest2 := nextToken(rest)
			if key == "" {
				return nil, fmt.Errorf("batch put: missing key")
			}
			value := strings.TrimLeft(rest2, " \t")
			if value == "" {
				return nil, fmt.Errorf("batch put: missing value")
			}
			ops = append(ops, BatchOp{Key: key, Value: []byte(value), IsPut: true})

		case "puthex":
			key, rest2 := nextToken(rest)
			if key == "" {
				return nil, fmt.Errorf("batch puthex: missing key")
			}
			hexStr := strings.TrimLeft(rest2, " \t")
			if hexStr == "" {
				return nil, fmt.Errorf("batch puthex: missing hex value")
			}
			hexStr = strings.TrimPrefix(hexStr, "0x")
			hexStr = strings.TrimPrefix(hexStr, "0X")
			decoded, err := hex.DecodeString(hexStr)
			if err != nil {
				return nil, fmt.Errorf("batch puthex: invalid hex: %w", err)
			}
			ops = append(ops, BatchOp{Key: key, Value: decoded, IsPut: true})

		case "del", "delete":
			key, _ := nextToken(rest)
			if key == "" {
				return nil, fmt.Errorf("batch del: missing key")
			}
			ops = append(ops, BatchOp{Key: key, IsPut: false})

		default:
			return nil, fmt.Errorf("batch: unknown op %q", opName)
		}
	}

	if len(ops) == 0 {
		return nil, fmt.Errorf("batch: no operations specified")
	}
	return ops, nil
}

// autocomplete returns completions for the current input.
func autocomplete(input string, entries []BrowserEntry) (completed string, nMatches int) {
	trimmed := strings.TrimRight(input, " \t")
	if trimmed == "" {
		return input, 0
	}

	// Count tokens.
	tokens := strings.Fields(trimmed)
	tokenCount := len(tokens)
	if tokenCount == 0 {
		return input, 0
	}

	trailingSpace := len(trimmed) != len(input)
	completingIndex := tokenCount
	if !trailingSpace {
		completingIndex = tokenCount - 1
	}

	// Get the prefix being completed.
	var prefix string
	var base string
	if trailingSpace {
		prefix = ""
		base = input
	} else {
		lastSpace := strings.LastIndexAny(input, " \t")
		if lastSpace < 0 {
			prefix = input
			base = ""
		} else {
			prefix = input[lastSpace+1:]
			base = input[:lastSpace+1]
		}
	}

	var matches []string

	if completingIndex == 0 {
		// Complete command names.
		for _, c := range commandCandidates {
			if strings.HasPrefix(strings.ToLower(c), strings.ToLower(prefix)) {
				matches = append(matches, c)
			}
		}
	} else {
		cmd := strings.ToLower(tokens[0])

		// Batch sub-commands.
		if cmd == "batch" && !strings.Contains(input, ";") {
			for _, c := range batchOpCandidates {
				if strings.HasPrefix(strings.ToLower(c), strings.ToLower(prefix)) {
					matches = append(matches, c)
				}
			}
		}

		// Key completion for get/put/del/ls.
		if completingIndex == 1 {
			switch cmd {
			case "get", "put", "del", "delete", "ls", "list":
				for _, e := range entries {
					if strings.HasPrefix(e.Key, prefix) {
						// Deduplicate.
						found := false
						for _, m := range matches {
							if m == e.Key {
								found = true
								break
							}
						}
						if !found {
							matches = append(matches, e.Key)
						}
					}
				}
			}
		}
	}

	if len(matches) == 0 {
		return input, 0
	}

	if len(matches) == 1 {
		return base + matches[0] + " ", 1
	}

	// Find longest common prefix.
	lcp := matches[0]
	for _, m := range matches[1:] {
		i := 0
		for i < len(lcp) && i < len(m) && lcp[i] == m[i] {
			i++
		}
		lcp = lcp[:i]
	}

	if len(lcp) <= len(prefix) {
		return input, len(matches)
	}

	return base + lcp, len(matches)
}

// parseHexBytes parses a hex string (with optional 0x prefix) into bytes.
func parseHexBytes(s string) ([]byte, error) {
	s = strings.TrimPrefix(s, "0x")
	s = strings.TrimPrefix(s, "0X")
	if s == "" {
		return nil, fmt.Errorf("empty hex string")
	}
	return hex.DecodeString(s)
}

// parseUint32 parses a string as uint32.
func parseUint32(s string) (uint32, error) {
	v, err := strconv.ParseUint(s, 10, 32)
	return uint32(v), err
}
