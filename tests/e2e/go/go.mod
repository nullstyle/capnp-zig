module e2e-rpc-test

go 1.23

require (
	capnproto.org/go/capnp/v3 v3.0.1-alpha.2
	github.com/colega/zeropool v0.0.0-20230505084239-6fb4a4f75381 // indirect
	golang.org/x/exp v0.0.0-20240604190554-fc45aab8b7f8 // indirect
	golang.org/x/sync v0.7.0 // indirect
)

replace capnproto.org/go/capnp/v3 => ../../../vendor/ext/go-capnp
