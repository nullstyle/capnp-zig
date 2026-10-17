# Native C ABI (Experimental)

`native` is a sans-IO Cap'n Proto RPC connection with a C ABI, for C and
Swift hosts that own their sockets. capnp-swift built it as its core and
moved it here (capnp-swift handoff H7), so it now changes in one place, with
capnp-zig's own tests. All three library roots export it as `native`; it
needs only `capnpc-zig-core`.

- `native.conn` is the Zig API: a `Conn` is a detached `rpc.peer.Peer`, a
  framer and an effect queue.
- `native.abi` is the C ABI over it: `export fn capnp_*`, declared by
  `src/native/include/capnp_core.h`. The header is the reference for every
  function, struct, constant and error code.

Everything here is Experimental: the module, its Zig API and the C ABI may
change in any release. Pin an exact version. The ABI has its own version,
`CAPNP_CORE_ABI_VERSION` (now 1), which a host checks at connect.

## The model

The core never touches a socket and never calls the host, except through the
panic hook. The host:

- pushes received bytes in (`capnp_conn_push_bytes`), in either framing:
  the standard segment-table stream (TCP, Unix sockets, TLS) or the QUIC
  baseline's little-endian u32 length prefix (`CAPNP_FRAMING_U32_LE`;
  `capnp_core_quic_alpn()` returns the frozen ALPN);
- drives time (`capnp_conn_tick`) and reports the end of its transport
  (`capnp_conn_transport_closed`);
- after every call, drains effects (`capnp_conn_next_effect` /
  `capnp_conn_commit_effect`): frames to send, a close request, one RETURN
  per question, inbound calls to answer, dropped exports, and observer
  events. One effect is in flight at a time, and its payload is borrowed
  until its commit.

Params and results cross the ABI as standalone messages whose capability
pointers index a `caps[]` table of imports, exports, promised answers and
null caps. One caller at a time per connection, from any thread.

## Returns and tail calls

Every question ends with exactly one RETURN effect. This table gives the
RETURN for each Return variant that the remote can send:

| Return from the remote | RETURN |
|---|---|
| `results` | `RESULTS` |
| `exception` | `EXCEPTION`, or `DISCONNECTED` for a local disconnect |
| `canceled` | `CANCELED` |
| `takeFromOtherQuestion` (a tail call), named call has cap-free results | `RESULTS`: the named call's results |
| `takeFromOtherQuestion`, named call fails | `EXCEPTION`: the named call's exception |
| `takeFromOtherQuestion`, named call's results carry capabilities | `EXCEPTION` (`unimplemented`), reason `unfollowed_tail_call_reason` |
| `takeFromOtherQuestion`, the remote finishes the named call before the host answers it | `EXCEPTION` (`failed`), reason `rpc.peer.tail_call_finished_reason` |
| `takeFromOtherQuestion`, the named call waits on a promise export that the remote releases before it resolves (see below) | `EXCEPTION` (`failed`), reason `rpc.peer.promise_export_released_reason` |
| `takeFromOtherQuestion`, the named call is pipelined on a tail-called answer (see below) | `EXCEPTION` (`failed`), reason `rpc.peer.tail_call_pipelining_reason`, or the tail call's exception |
| `takeFromOtherQuestion`, the named call was forwarded back to the remote (see below) | `EXCEPTION` (`unimplemented`), reason `unfollowed_tail_call_reason`, when the call is forwarded |
| `takeFromOtherQuestion` that names no call with `sendResultsTo = yourself` | `EXCEPTION` (`unimplemented`), reason `unfollowed_tail_call_reason` |
| `takeFromOtherQuestion` past a bound (see below) | `EXCEPTION` (`unimplemented`), reason `unfollowed_tail_call_reason` |
| `takeFromOtherQuestion`, the core cannot give the question the named call's answer (the fallback rule, see below) | `EXCEPTION` (`unimplemented`), reason `unfollowed_tail_call_reason`, at once |
| `resultsSentElsewhere` | `EXCEPTION` (`unimplemented`), reason `unsupported_return_reason` |
| `awaitFromThirdParty` that reaches the shim | `EXCEPTION` (`unimplemented`), reason `unsupported_return_reason` |

A tail call works like this. The remote sends an INBOUND_CALL whose results
go back to the remote (`sendResultsTo = yourself`). Then it answers one of
your questions with a Return that names that call (`takeFromOtherQuestion`).
The C++ reference sends the two together. When the host answers the call
(`capnp_return_results` or `capnp_return_exception`), the core does two
things:

- It sends the remote `resultsSentElsewhere`, also for an exception, as
  rpc.capnp requires.
- It gives your question the call's results or exception as its RETURN.

The question has no RETURN until then. `capnp_cancel` still ends it with one
RETURN CANCELED, also before the redirect arrives; the call's answer then
goes to no question. The order does not matter: an answer that comes before
the redirect is kept until the redirect arrives. If the remote never sends
that redirect (rpc.capnp allows this), the answer is dropped when the remote
sends a new call with the same question id, so it never reaches a later
question.

Fallback rule: if the core cannot give a waiting question the named call's
answer, the question gets, at once, the RETURN for a tail call the core
cannot follow: one RETURN EXCEPTION (`unimplemented`) with the reason
`unfollowed_tail_call_reason`. That happens when the call is forwarded back
to the remote (below), when it is queued on an answer whose Return already
went out without failing it, past a bound, and when the core runs out of
memory as it builds or delivers the answer. The remote's Finish of the
named call is the one other end: the question gets
`rpc.peer.tail_call_finished_reason`. A waiting question otherwise waits as
long as the call waits for you: answer every INBOUND_CALL, or `capnp_cancel`
the question.

A tail call can also name a call to a promise export
(`capnp_promise_export`) that is not resolved yet. The call is parked, and
the question waits for it too. After `capnp_resolve_promise` to one of your
exports, the call reaches the host as an INBOUND_CALL, and its answer goes
to the question. After `capnp_reject_promise`, the question gets one RETURN
EXCEPTION with the reason `promise broken`. After `capnp_resolve_promise` to
an import, the core forwards the call back to the remote, and its results
never come to this side. So the question gets one RETURN EXCEPTION
(`unimplemented`) with the reason `unfollowed_tail_call_reason` when the
call is forwarded. The same holds for a call to a promise export that
already resolved to an import: the core forwards it when it arrives, and
the question gets that RETURN when the redirect arrives. The question does
not wait for the forwarded call to return: the C++ reference answers that
call only when a redirect names it or when its Finish arrives, and it
finishes the call it sent only after you finish the question. When that
Finish comes, the core relays it to the forwarded call, and the remote's
call gets `resultsSentElsewhere` if the forwarded call has not returned
yet. If the remote releases the promise export before it resolves (it
drops its last reference, as the C++ reference does after a tail call on
a promise the host passed it), the export is gone and the call can never
run: the remote gets `resultsSentElsewhere`, and the question gets one
RETURN EXCEPTION (`failed`) with the reason
`rpc.peer.promise_export_released_reason`. Every call the remote parked on
a promise export gets an exception Return with that reason when the remote
releases the export first. A call that the core refuses before the host
sees it (an unknown capability, a broken promise) also gives the question
its exception, and the remote `resultsSentElsewhere`.

Pipelining on a tail-called answer is not supported: that answer's results
went to the remote's question, not to a pipeline on this side. The C++
reference bounces a call that you pipelined on a question it answered with
a tail call back to this side, as another tail call on the tail call's
answer. That call fails, so your pipelined question gets one RETURN
EXCEPTION (`failed`) with the reason `rpc.peer.tail_call_pipelining_reason`,
or the tail call's exception when the tail call failed. This happens when
the host answers the tail call or the core forwards it back to the remote,
or at once when the bounced call comes after that. The core keeps the
tail call's id in use until the remote finishes it.

If `capnp_return_results` fails, answer the call with
`capnp_return_exception`. Usually the results did not build, and the
question gets that exception. If the core built the results but could not
send `resultsSentElsewhere` (out of memory), a question that was waiting
already has the results and gets no second RETURN; otherwise the exception
replaces the kept results. Either way the remote gets one
`resultsSentElsewhere`.

Two bounds apply. The core keeps at most
`PeerLimits.max_loopback_result_stash` (4096) answers that wait for their
redirect, and holds at most `PeerLimits.max_send_results_to_yourself` (4096)
redirects that wait for their call. Past either bound, or when the core
runs out of memory as it keeps one, the question ends with one RETURN
EXCEPTION (`unimplemented`) and the reason `unfollowed_tail_call_reason`.
The same RETURN ends a waiting question when the core runs out of memory
as it gives the question the call's answer. If the core runs out of memory
as it replays or fails a call that waited on a promise (a promise export,
or a promised answer), that call still gets one exception Return, and a
question whose tail call names it gets that exception or the RETURN above.

Limit: the core cannot give a question tail-call results that carry
capabilities. Such a question ends with one RETURN EXCEPTION (`unimplemented`)
and the reason `capnp-swift core: cannot follow this tail call (...)`. The
results' capabilities take no references, so nothing leaks. Every call the
core sends has `sendResultsTo = caller`, so `resultsSentElsewhere` is a
remote error.

## Using it from a C or Swift host

Build a static library whose root module references `native.abi`. The
`capnp_*` symbols are emitted only in a compilation that references it, so
plain capnp-zig users get no exported symbols. This is the `build.zig` of the
consumer `zig build package-preflight` builds and runs from the packaged
archive:

<!-- verbatim: tests/package_consumer/build.zig -->
```zig
// The host owns every socket: build capnp-zig without fd passing, the fd
// closer threads and the AF_UNIX transport.
const capnp = b.dependency("capnpc_zig", .{
    .target = target,
    .optimize = optimize,
    .@"fd-passing" = false,
});
// The library root references `native.abi`, so the `capnp_*` symbols
// land in the archive.
const core = b.addLibrary(.{
    .name = "capnp_core",
    .linkage = .static,
    .root_module = b.createModule(.{
        .root_source_file = b.path("src/native_lib.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        .imports = &.{
            .{ .name = "capnpc-zig-core", .module = capnp.module("capnpc-zig-core") },
        },
    }),
});
// The program that links the archive brings its own compiler-rt.
core.bundle_compiler_rt = false;
// The header ships in the package, next to the module.
core.installHeader(capnp.path("src/native/include/capnp_core.h"), "capnp_core.h");
b.installArtifact(core);
```

The header's path in the package is `src/native/include/capnp_core.h`
(`capnp.path(...)` above). The library root:

<!-- verbatim-file: tests/package_consumer/src/native_lib.zig -->
```zig
//! Root of the native consumer's static library (docs/native-abi.md): what a
//! C or Swift host's own library root needs to ship capnp-zig's C ABI.

const std = @import("std");
const capnp = @import("capnpc-zig-core");

// Referencing `native.abi` is what emits the `capnp_*` symbols.
comptime {
    _ = capnp.native.abi;
}

/// The allocator behind every connection.
pub const capnp_core_allocator: std.mem.Allocator = std.heap.c_allocator;

/// What `capnp_core_version()` reports: your version and the capnp-zig
/// package you pin.
pub const capnp_core_version_string: [:0]const u8 = "core 0.0.0 / capnp-zig package-preflight / native-consumer";
```

abi.zig reads two declarations from the root module of the compilation
(`@import("root")`). Both are optional, except that a library which does
not link libc must declare `capnp_core_allocator`:

| Declaration | Type | Without it |
|---|---|---|
| `capnp_core_allocator` | `std.mem.Allocator` | in a test build, a counting wrapper around `std.testing.allocator`; else the C allocator when libc is linked; else a compile error in a library, and `std.heap.page_allocator` (at least one page per allocation) in an executable |
| `capnp_core_version_string` | `[:0]const u8` | `"core unknown / capnp-zig unknown / unknown"` |

`capnp_core_debug_trap` (a test hook) traps in `src/native/abi.zig` on the
line marked `CAPNP_CORE_DEBUG_TRAP_LINE`, so crash tooling can check that a
Zig frame symbolicates. In a library built from the package, the debug info
names that file under the package directory, which changes with every pin
(`zig-pkg/capnpc_zig-<version>-<hash>/src/native/abi.zig`): derive the
directory from the pinned package, or match the suffix
`src/native/abi.zig:<line>`.

A library for iOS also needs the std overrides that
[build-integration.md](build-integration.md#ios-the-core-as-a-static-library-experimental)
lists. Zig code can skip the C ABI and use `native.conn.Conn` directly.

## Gates

- `zig build test` runs the shim's own tests in the `capnpc-zig-core` test
  root: `src/native/conn_test.zig` (two connections back to back, the
  capability round trip, disconnect and OOM paths, malformed input, both
  framings, tail calls), the tests in `abi.zig` (a counting allocator must reach 0 live
  bytes after every export ran), and the header gate,
  `src/native/abi_header_test.zig`: every `pub export fn capnp_*` has a
  prototype in the header with the same calling convention, arity, scalar
  widths and struct layouts, every prototype is exported, and the `CAPNP_*`
  constants match.
- `zig build test-native-abi` (also in `test`) calls the ABI through the
  translated header, linked from a host static library
  (`tests/native/abi_lib_root.zig`); it never imports abi.zig.
- `zig build fuzz-native-abi -- --seconds N [--seed S]` drives the ABI with
  random operation sequences and checks its invariants (one RETURN per
  question, well-formed payloads, 0 live bytes after free). A seeded
  3-second run is part of `test-fuzz-smoke`; Nightly runs it for 10 minutes
  in ReleaseSafe.
- `zig build check-ios` compiles the ABI into the iOS, simulator and macOS
  (fd passing off) libraries; `check-fd-passing-off-symbols` reads the macOS
  one. `zig build package-preflight` builds the consumer above, with a C
  program that includes the header, and runs it.
- `zig build hardening` scans `src/native/`. An ABI change shows in review
  twice: in the header and in `docs/api-snapshot-experimental.txt`, where
  every export and C struct field has a line.

## Provenance

Moved from capnp-swift at `1aceb01` (its `core/src` and
`core/include/capnp_core.h`). capnp-swift keeps its Apple glue: the library
root with the panic hook and std overrides (`apple_root.zig`), the Clang
module map and the XCFramework build.
