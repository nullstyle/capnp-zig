CAPNPC_GO := ../../../vendor/ext/go-capnp/capnpc-go/capnpc-go
CAPNP_STD := ../../../vendor/ext/go-capnp/std

.PHONY: build run generate clean

build:
	go build -o kvstore-client .

run: build
	./kvstore-client

generate: $(CAPNPC_GO)
	capnp compile -I $(CAPNP_STD) -ogo:kvstore kvstore.capnp

$(CAPNPC_GO):
	cd ../../../vendor/ext/go-capnp/capnpc-go && go build -o capnpc-go .

clean:
	rm -f kvstore-client
