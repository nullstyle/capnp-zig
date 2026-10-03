const capnpc = @import("capnpc-zig");
const common = @import("common.zig");

comptime {
    _ = capnpc.canonical;
    _ = capnpc.rpc.peer.Peer;
}

// Default-root surface beyond common.zig's shared set. See common.zig for why
// each line takes an `&`, and RELEASING.md for the rule that keeps this list
// current.
//
// slcp's overlay keeps its own TCP reader/writer only because this transport
// did not compile on its toolchain (slcp-zig docs/upstream 05 and 07 F4). Its
// adoption needs: init on an already-accepted socket, blocking and deadline
// reads, writes, and the queued writer thread. v0.18.0 broke `read` on tagged
// Zig 0.17.0 and this root did not notice, because nothing here named the TCP
// transport.
comptime {
    const tcp = capnpc.rpc.transport.tcp;
    _ = &tcp.Transport.init; // slcp (overlay adoption: wraps an accepted fd)
    _ = &tcp.Transport.initWithOptions; // slcp (overlay adoption: queue bounds, observer)
    _ = &tcp.Transport.read; // slcp (overlay adoption: blocking reader thread)
    _ = &tcp.Transport.readTimeout; // slcp (overlay adoption: Hello handshake deadline)
    _ = &tcp.Transport.write; // slcp (overlay adoption: synchronous Hello write)
    _ = &tcp.Transport.enqueueWrite; // slcp (overlay adoption: queued sends)
    _ = &tcp.Transport.startWriter; // slcp (overlay adoption: per-connection writer thread)
    _ = &tcp.Transport.stopWriter; // slcp (overlay adoption)
    _ = &tcp.Transport.shutdown; // slcp (overlay adoption: unblock the reader)
    _ = &tcp.Transport.deinit; // slcp (overlay adoption)
    _ = &tcp.Listener.init; // slcp (upstream 05: runtime.Listener)
    _ = &tcp.Listener.acceptFd; // slcp (upstream 07 F4: accepted-socket entry point)
    _ = &tcp.Listener.close; // slcp (upstream 05: runtime.Listener)
    _ = &tcp.createListenSocket; // slcp (upstream 05: createListenSocket)
    _ = &tcp.closeFd; // slcp (upstream 05: socket teardown)
    _ = &tcp.connect; // slcp (upstream 05: client.connect)
    _ = &tcp.ClientSession.run; // slcp (upstream 05: client.connect drives a Connection read loop)
    _ = &tcp.ClientSession.deinit; // slcp (upstream 05: client.connect)
}

pub fn main() !void {
    try common.exerciseSerialization();
}
