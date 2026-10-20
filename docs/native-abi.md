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

A pipelined param names a question of the caller (a `receiverAnswer`
descriptor). The core resolves it when the pipelined call dispatches, not
when it arrives. A native-ABI host answers every call after its dispatch,
so the param fails whenever the host has not answered the named call by
then: the core refuses the call with an exception, reason
`PromisedCapUnsupported`, and the host never sees it. A call to a
capability that the host exports dispatches as soon as it arrives, so this
always happens when both calls arrive in one `capnp_conn_push_bytes`. A
call that is parked behind the named answer dispatches only after the host
answers it. In the `pipelined_params` e2e flow, the C++, Go, Rust and
Python reference clients in `tests/e2e` all send such a param in a call
to the host's bootstrap capability. See the pending-answer limit in
docs/supported-surface.md ("A pipelined capability in call params resolves
only once its answer has returned").

## Answer ids

The host answers each INBOUND_CALL once, with `capnp_return_results`,
`capnp_return_exception` or `capnp_return_canceled` ("Answer finished").
Until that answer, the host owes it, and its id stays reserved: the core
never hands the host a second INBOUND_CALL with an id the host still owes.

A remote that sends a new call with such an id breaks the protocol. The core
sends Abort and queues CLOSE_REQUESTED, in either framing.
`capnp_conn_push_bytes` returns `CAPNP_E_PROTOCOL`, and
`capnp_conn_take_error` names `DuplicateQuestionId`. What comes before the
Abort depends on what the core still knows of the old call:

- The remote has not sent Finish for the old call, or it has and the core
  keeps a record of that Finish. Then the Peer refuses the new call itself,
  and no Return for it goes out. A record of the Finish also refuses a
  Bootstrap with the id.
- The core keeps no record of the old call's Finish, because its records
  were full (see below). Then the core answers the new call with an
  exception Return (`DuplicateQuestionId`) before the Abort.

A call that the core kept queued (on a promise export, or on an answer) can
reach this check later: in `capnp_resolve_promise` or
`capnp_return_results`. That call returns `CAPNP_OK`, and the exception
Return, the Abort and CLOSE_REQUESTED follow in the effects.

The records of finished calls are bounded by `max_active_inbound_questions`.
When they are full, the core can promise only that no second INBOUND_CALL
reaches the host. It does not catch a reused id on a Bootstrap, or on a Call
that it answers itself because the call never reaches a host export (for
example, a call to an export id that does not exist). The core answers such
a call, the connection stays up, and the host's late answer for the old call
then goes out with the same id.

After the host's answer, the remote may reuse the id at once (the C++
reference hands out the lowest free question id). So a second answer for one
INBOUND_CALL is a host bug: it gets `CAPNP_E_BAD_ID` only until the remote
reuses the id, and after that it answers the new call.

The host owes at most `max_active_inbound_questions` answers (default 4096).
A call beyond that gets an exception Return (`OwedAnswerLimitExceeded`) and
never reaches the host. The connection stays up.

## Answer finished

A host can learn that the caller finished a call that the host has not
answered, and stop the work (capnp-swift handoff H13). It is an opt-in:

- Check `capnp_core_features() & CAPNP_FEATURE_ANSWER_FINISHED`. A core
  without that bit ignores the field below.
- Set `capnp_conn_opts.answer_finished = 1`. 0 is off, and any other value
  makes `capnp_conn_new` return `CAPNP_E_INVAL`. A host built against an
  older header passes a smaller `struct_size`, so it gets 0.

When the remote sends Finish for an INBOUND_CALL that the host owes, the
core queues one `CAPNP_EFFECT_ANSWER_FINISHED` (6). Its `id` is the answer
id, `export_id` and `host_tag` are those of the INBOUND_CALL, and every
other field is 0. The core queues it at most once per call. Its node is
reserved when the INBOUND_CALL is queued, so a lack of memory cannot lose it.

The host stops the work and answers once. Normally it calls
`capnp_return_canceled(conn, answer_id)`: the core sends `Return{canceled}`,
and the caller can reuse its question id. That call returns
`CAPNP_E_BAD_ID` for an answer the host does not owe. It returns
`CAPNP_E_INVAL` when no ANSWER_FINISHED was queued for the answer: the
opt-in is off, no Finish came, or a tail-call hold withholds it. On any
other error it sends nothing, and the host still owes the answer. Instead of
`capnp_return_canceled`, the host may answer with `capnp_return_results` or
`capnp_return_exception`. A `sendResultsTo = yourself` call also gets
`Return{canceled}`, not `resultsSentElsewhere`.

**Drain order.** Apply effects in drain order. An ANSWER_FINISHED can be
stale: the host answered the call after the Finish came, but before it
drained the effect. The effect then comes before that answer's Return
OUT_FRAME, and the remote can reuse the id only after it gets that Return.
So an ANSWER_FINISHED for an id that the host already answered is stale:
ignore it (`capnp_return_canceled` returns `CAPNP_E_BAD_ID` for it).

**Export ids.** `export_id` and `host_tag` are those of the INBOUND_CALL.
The remote can release that export before it sends Finish, so an
ANSWER_FINISHED can come after the EXPORT_DROPPED of its export. Find the
work by its answer id, not by `export_id` or `host_tag`.

**The late answer.** From the Finish to the host's answer, the core keeps a
record of the Finish. While that record exists, a Call or a Bootstrap that
reuses the id closes the connection before any Return for it ("Answer ids").
So after ANSWER_FINISHED, the host's late answer can never reach another
call.

**When no effect comes.** The core reports a Finish only when it can record
it. The records are bounded by `max_active_inbound_questions`. When they are
full, or when the record cannot get memory, no ANSWER_FINISHED comes,
`capnp_return_canceled` returns `CAPNP_E_INVAL`, and the host's ordinary
answer goes out. The bound on owed answers ("Answer ids") does not keep a
record free for each of them: other inbound calls use the same records. A
call that the core still holds queued (on a promise export, or on an
answer) takes one when the remote finishes it with
`requireEarlyCancellationWorkaround` set, and keeps it until the call is
delivered or its promise settles. So with promises that never settle, the
records can stay full for the life of the connection, and then no call gets
ANSWER_FINISHED. That flag defaults to true. The C++ reference (1.0 and
later) sends false, but go-capnp never sets it, so with a go-capnp caller,
each Finish of a queued call takes a record. Such a queued call reaches the
host after its Finish and gets no ANSWER_FINISHED: answer it as usual.

**Tail calls.** While a question X waits for the answer to a call Y ("Tail
calls" below), a Finish of Y does not queue ANSWER_FINISHED: X still wants
Y's answer. If the wait ends without Y's answer (`capnp_cancel` of X, X's
deadline, or the shutdown drain bound), the core queues ANSWER_FINISHED for
Y then, before X's RETURN. A transport close queues none. The core never
makes X wait for a Y that it already reported: X then gets RETURN EXCEPTION
(`unimplemented`).

**Loopback calls.** A call from this connection to one of its own exports
is an INBOUND_CALL too. When the host cancels the question of such a call,
or the question's deadline passes, the core queues ANSWER_FINISHED for the
call before the question's RETURN (CANCELED or EXCEPTION). Answer it as any
other; the ended question absorbs the answer.

## Tail calls

The remote can answer a question with `Return.takeFromOtherQuestion`: the
question's results are those of a call Y that the remote sent to us with
`sendResultsTo = yourself`. The C++ reference does this when it forwards a
parked call back to us. The core follows such a redirect in two cases:

- **The host answered Y already, and the core kept Y's answer.** The
  question gets Y's cap-free results or Y's exception at once. The core
  keeps that answer until the remote's Finish of Y, for at most 4096 calls
  at a time (the Peer's `limits.max_loopback_result_stash`), and it keeps
  nothing when memory runs out.
- **Y is an INBOUND_CALL that the host has not answered.** The question
  waits. The host's `capnp_return_results` for Y becomes the question's
  RETURN RESULTS (a copy of the same message, with NONE capabilities as
  null pointers), and its
  `capnp_return_exception` becomes RETURN EXCEPTION with the same type and
  reason. The remote gets `resultsSentElsewhere` for Y either way.

While a question waits, it is an open question with no RETURN yet:

- `capnp_cancel` sends its Finish and ends it with RETURN CANCELED.
- Its deadline ends it on a tick with RETURN EXCEPTION (overloaded), after
  its Finish.
- `capnp_finish` returns `CAPNP_E_BUSY`, as for any question with no
  RETURN yet.
- `capnp_conn_transport_closed` ends it with RETURN DISCONNECTED.
- `capnp_conn_shutdown` waits for it, as for any open question, and
  `shutdown_drain_timeout_ms` ends it with RETURN DISCONNECTED, after its
  Finish.

As for other questions, a cancel, a deadline or the drain bound ends the
question even when its Finish cannot be sent, and `capnp_finish` then
refuses its id.

The remote's Finish of Y does not end the wait: the host still answers Y,
and ANSWER_FINISHED for Y waits too ("Answer finished"). A remote that
reuses Y's id for a new call before the host answered Y
closes the connection (see "Answer ids" above). The question then ends at
transport close, with RETURN DISCONNECTED.

Every other redirect ends the question at once with RETURN EXCEPTION
(`unimplemented`, "capnp-swift core: unsupported Return variant"). That
covers results of Y that point at a capability (the waiting case instead
gives `unimplemented`, "capnp-swift core: tail call results carry
capabilities", which is `native.conn.tail_call_caps_reason`), a Y that the
host answered but whose answer the core did not keep, Y still held by the
core (parked on a promise export, queued on an answer, forwarded back to
the remote), a call without `sendResultsTo = yourself`, an id that names
no call, a second
redirect to the same Y, and the bootstrap question. The C++ reference also
follows those. capnp-zig's own design and termination proof:
[notes/tail-call-hold-design.md](notes/tail-call-hold-design.md).

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
  framings), the tests in `abi.zig` (a counting allocator must reach 0 live
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
  question, well-formed payloads, no INBOUND_CALL for an id the host still
  owes, ANSWER_FINISHED only as described above, 0 live bytes after free).
  One session in 4 is clean: no junk bytes and no promises, so calls reach
  the hosts. A seeded
  3-second run is part of `test-fuzz-smoke`; Nightly runs it for 10 minutes
  in ReleaseSafe.
- `zig build check-ios` compiles the ABI into the iOS, simulator and macOS
  (fd passing off) libraries; `check-fd-passing-off-symbols` reads the macOS
  one. `zig build package-preflight` builds the consumer above, with a C
  program that includes the header (compiled with `-Wall -Wextra -Werror`),
  and runs it.
- `zig build hardening` scans `src/native/`. An ABI change shows in review
  twice: in the header and in `docs/api-snapshot-experimental.txt`, where
  every export and C struct field has a line.

## Provenance

Moved from capnp-swift at `1aceb01` (its `core/src` and
`core/include/capnp_core.h`). capnp-swift keeps its Apple glue: the library
root with the panic hook and std overrides (`apple_root.zig`), the Clang
module map and the XCFramework build.
