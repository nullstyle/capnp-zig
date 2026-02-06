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
	schema := flag.String("schema", "gameworld", "schema to serve: gameworld, chat, inventory, matchmaking")
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

	switch schema {
	case "gameworld":
		bootstrap = capnp.Client(servers.NewGameWorldClient())
	case "chat":
		bootstrap = capnp.Client(servers.NewChatServiceClient())
	case "inventory":
		bootstrap = capnp.Client(servers.NewInventoryServiceClient())
	case "matchmaking":
		bootstrap = capnp.Client(servers.NewMatchmakingServiceClient())
	default:
		log.Printf("unknown schema: %s", schema)
		return
	}

	rpcConn := rpc.NewConn(rpc.NewStreamTransport(c), &rpc.Options{
		BootstrapClient: bootstrap,
	})
	<-rpcConn.Done()
	if err := rpcConn.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "rpc close: %v\n", err)
	}
}
