package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	capnp "capnproto.org/go/capnp/v3"
	"capnproto.org/go/capnp/v3/rpc"

	"e2e-rpc-test/internal/servers"
)

func main() {
	host := flag.String("host", "0.0.0.0", "listen host")
	port := flag.Int("port", 4001, "listen port")
	schema := flag.String("schema", "gameworld", "schema to serve: gameworld, chat, inventory, matchmaking, resolve_disembargo")
	flag.Parse()

	addr := fmt.Sprintf("%s:%d", *host, *port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	fmt.Println("READY")
	log.Printf("go rpc server listening on %s (schema=%s)", addr, *schema)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-stop
		_ = ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok && !opErr.Temporary() {
				break
			}
			log.Printf("accept: %v", err)
			continue
		}

		go handleConn(conn, *schema)
	}
}

func handleConn(c net.Conn, schema string) {
	defer c.Close()

	var bootstrap capnp.Client
	// reflectorServer is non-nil only for resolve_disembargo; its disconnectNow
	// handler needs the rpc.Conn (created below) to close the transport.
	var reflectorServer *servers.ReflectorServer

	switch schema {
	case "gameworld":
		bootstrap = capnp.Client(servers.NewGameWorldClient())
	case "chat":
		bootstrap = capnp.Client(servers.NewChatServiceClient())
	case "inventory":
		bootstrap = capnp.Client(servers.NewInventoryServiceClient())
	case "matchmaking":
		bootstrap = capnp.Client(servers.NewMatchmakingServiceClient())
	case "resolve_disembargo":
		client, srv := servers.NewReflectorClientWithServer()
		bootstrap = capnp.Client(client)
		reflectorServer = srv
	default:
		log.Printf("unknown schema: %s", schema)
		return
	}

	if reflectorServer != nil {
		// Wire the raw socket so disconnectNow() can close the transport,
		// making the caller's outstanding call fail with a disconnect-class
		// error.
		reflectorServer.SetConn(c)
	}

	rpcConn := rpc.NewConn(rpc.NewStreamTransport(c), &rpc.Options{
		BootstrapClient: bootstrap,
	})
	<-rpcConn.Done()
	if err := rpcConn.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "rpc close: %v\n", err)
	}
}
