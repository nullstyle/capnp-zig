# Sprint plan: Frozen shape, local door, warm restart

**Owner decision (2026-10-04): option B, feature sprint.** The order is fixed:
1. The generated-shape freeze gate.
2. Cap'n Proto RPC over Unix-domain sockets, with FD passing.
3. 0-RTT after a server crash-restart, through a persisted session-ticket key.

The sprint ends with a v0.20.0 release candidate. The owner approves the tag.

**Forecast (tell the owner on day 1).** v0.20.0 will probably ship the shape gate, the Unix transport, drain mode and the ticket key. FD passing (items 10-15) gets into v0.20.0 only if item 14 closes by day 8. If it does not, it targets v0.21.0. The order of the work stays the same.

**Theme.** Prove each step before the next step starts.
- Freeze the shape of generated code on all three OSes. This includes interface code and wire constants.
- Ship a Unix-socket transport that closes every fd it did not ask for. It never closes a received fd on the reader thread. Then add FD passing on a branch, behind a security review.
- Let a restarted QUIC server accept 0-RTT, behind a forward-secrecy review.
- Keep `docs/api-snapshot.txt` byte-identical. All new API is Experimental.

## Winner, grafts, scores

**Winner: proof-first.** It ran its own probes, fixed two survey errors, and gates every export behind a review. It took these grafts from the other plans.

**From headline-first:**
- The C++ Unix-path lane moves from P2 into week 1. Survey runs A and B already pass, so the lane costs little.
- A hard rule: `Listener.acceptFd` keeps its error set, because that set feeds the frozen `ServerSession.accept`.
- An ablation that proves the e2e lane is red on zero TAP lines. The runner already returns `FAIL(NO_TESTS)` (`tools/e2e_runner.zig:838-841`) [V].
- A minimal `rpc_fd.capnp` replaces all of upstream `test.capnp`.
- A measured budget for Windows headroom.

**From consumer-first:**
- The `rpc.transport.unix` namespace. It returns the existing `tcp.Listener` and `*tcp.ClientSession`.
- Examples that CI runs, and compiled doc snippets.
- Forced package-consumer references. `core.zig` asserts that the namespace is absent.
- A release hook that also fails a patch bump that has shape or API drift.
- A closure rule for Stable shape lines.
- A walker seam for slcp, configured by a table.
- `WarmRedialClient.Outcome.zero_rtt_generations`.
- The day-1 `.cancel_failure` handoff to downstream repos.
- The events-enum decision (D3).

| Plan | Fidelity | Value | Evidence | Feasibility | Risk control | Mean |
|---|---|---|---|---|---|---|
| Proof-first | 9 | 8 | 9 | 7 | 9 | **8.4** |
| Headline-first | 9 | 9 | 8 | 6 | 7 | 7.8 |
| Consumer-first | 9 | 8 | 8 | 7 | 6 | 7.6 |

- **Proof-first.** It verified exact-boundary fd attribution on three platforms. It refuted the connect-probe reclaim and the "abort on CTRUNC" policy. Its weak point: it put the C++ Unix-path lane too late.
- **Headline-first.** It has the strongest cross-implementation story. But it carries the heaviest load (a C++ fd e2e by day 7), it depends on the per-OS anchor rule, and its stale-file reclaim is racy. It also repeats the wrong ReleaseSafe line number.
- **Consumer-first.** It has the best adoption package, and it found a real downstream break. But it defaults to unlinking stale files after a connect probe, it uses the anchor rule, and it puts the gate in the Windows Test job.

## State verified today (2026-10-04)

Marks:
- **[V]** I re-read or re-ran it today.
- **[S]** A survey, planner or reviewer ran it. I did not re-run it.
- **[I]** Inferred.

**Repo, CI, release**
- main is at d1f658a and is clean [V]. `[Unreleased]` is empty (`CHANGELOG.md:8`) [S].
- Push CI 37217232914 and the last 4 Nightlies are green [S].
- macOS is a CI leg. The Test matrix (`ci.yml:91`) and the Hardening matrix (`ci.yml:476`) both list `macos-latest` [V].
- **Windows has little headroom.**
  - Test (windows) takes 2175 s. The job cap is 45 minutes (`ci.yml:87`), and the "Run tests" step has its own 35-minute cap (`ci.yml:148`) [S; caps V].
  - ReleaseSafe hardening (windows) takes 2099 s of a 45-minute cap (`ci.yml:632-636`) [S; cap V].
  - The Hardening job has a 25-minute cap (`ci.yml:472`). Its Windows leg takes about 4 minutes [S].
  - The reflection-conformance job has a 35-minute cap (`ci.yml:31`) [V]. Item 15 adds work there.
- `just release-preflight` (`Justfile:337-344`) has no shape check [V]. `release-tag` is at `Justfile:353`, and `verify-release-hash` is at `:368` [V].
- The bump table is `RELEASING.md:25-31`. The additive row is `:30` [V].
- Nightly runs on a daily cron and on `workflow_dispatch` (`nightly.yml:3-7`) [V]. Its only arm job runs `tools/fuzz_evidence.zig` and no test suites (`nightly.yml:95-120`) [V]. The aarch64 cross-target job is compile-only (`ci.yml:443-446`) [S].
- **Suite lists.**
  - The TSan lane links libc (`build/build_impl.zig:1506`). Its suite list is at `:1512-1523` [V].
  - The ReleaseSafe suites start near `:1388` [S].
  - The surveys call `:1514-1516` the ReleaseSafe list. That is the TSan list.
- The test runner sets `testing.log_level = .warn` before each test (`lib/compiler/test_runner.zig:287`) [V].

**Generated shape**
- `check-generated` diffs bytes only (`Justfile:296-310`). It runs on Linux only (`ci.yml:171-177`) [V].
- `generated_paths` goes to `zig fmt --check` (`Justfile:303`). The committed request precedent, `addressbook.request.bin`, is in the `git diff` list at `:310`, not in `generated_paths` [V].
- `RELEASING.md:33-41` says that `check-api` cannot see generated shape. It uses da60cb6 as the example [V].
- **Stale docs.**
  - `supported-surface.md:63` calls "generated interface code" Stable (frozen, CI-gated). No gate freezes it [V].
  - `supported-surface.md:62` says reflection is "(unreleased)", and `:96` says the generated mutable APIs are unreleased. Both shipped in v0.19.0 [V].
  - `Justfile:304-308` says the experimental snapshot is never diffed and is target-dependent. `ci.yml:362-371` says it is platform-stable between Linux and macOS, and CI checks it strictly [V].
- **The experimental API snapshot has 1030 `rpc.generated` lines** [V]:
  - 884 lines from rpc.capnp structs;
  - 128 from `Persistent`, the only interface;
  - 17 from `stream`.

  CI fails when they are stale (`ci.yml:415-419`, `:382-384`; `--strict-experimental` at `build/build_impl.zig:1052`) [V]. Nothing classifies their changes for a release.
- **The renderer prints a const as its type only** (`tools/api_snapshot.zig:679`). For example, `Persistent.interface_id: const u64` and `Save.ordinal: const u16` (`api-snapshot-experimental.txt:604,617`) [V]. A changed interface ID or ordinal would stay green.
- Generated guarded getters use inferred error sets (`tests/golden/union_group_enum.zig:132-133`) [V]. A getter whose inferred set loses all its errors still compiles on 0.17 (`survey/claims/inferred.zig`, re-run) [V].
- `interface_gen.zig:96` emits `release` once for each non-generic interface [V].
- **A scratch `@typeInfo` walker passed four ablations [S]:**
  - an inherited-name rename: 180 lines red, while the byte gate stayed green;
  - a `release` receiver change: 1 line red (on a one-interface surface);
  - an equivalent body rewrite: green;
  - a da60cb6 replay: a compile failure.
- The walker and closure code that item 1 must move sit at `tools/api_snapshot.zig:382-420` (`isContainer`, `containerKind`, `foreignType`, `contains`), `:505` (`defaultSuffix`), `:639` (`walk`), `:755-812` (`collectTypes`, `peel`, `tierOfType`, `collectClosure`) and `:1028-1046` (`platform_type_aliases`, `canonicalizePlatformTypes`) [V].
- **D1(a) Stable families name Experimental types.** Each of these has 0 Stable lines and some Experimental lines [V]:
  - `rpc.peer.CallOptions`, taken by `callXWithOptions` (`examples/pingpong.zig:380`);
  - `reflection.SchemaRef`, the type of `capnpSchema` (`:197`);
  - `DeferredHandler`, the type of the `VTable` field `x_deferred` (`:210`, `:524`). It names the generated `ReturnSender` (`:277`);
  - `generated_helpers.ReaderStorage`, taken by `asReader`.

**Frozen RPC code**
- Eight Stable **prefix** rules cover RPC code [V]:
  - `rpc.wire.protocol` (`tools/api_snapshot.zig:226`)
  - `rpc.wire.framing` (`:227`)
  - `rpc.caps.table` (`:230`). This includes `inbound`, `outbound` and `lifecycle` (`src/rpc/caps/table.zig:1-5`).
  - `ConnectOptions` (`:248`)
  - `ServeOptions` (`:262`)
  - `Connection.Options` (`:292`)
  - `PeerLimits` (`:354`)
  - `Export` (`:358`)

  A new field or pub decl under any of them turns the Stable gate red.
- So these are frozen [V]:
  - `encodeCallPayloadCaps*` and `encodeReturnPayloadCaps*` (`api-snapshot.txt:965-968`). The private `encodePayloadCaps` (`src/rpc/caps/outbound.zig:225`) runs only through them (`:324`, `:348`).
  - `InboundCapTable.init` (`:884`) and `resolveCapDescriptor` (`:899`). The import sites `caps/inbound.zig:164,178` run inside them.
  - Every `CapTable` field. They render at four alias paths.
- `Peer` is frozen **exactly**. Only the methods with their own `e()` rules are Stable (`tools/api_snapshot.zig:318-333`). New Peer methods and Peer fields are Experimental [V].
- `Connection.init` and `tcp.Transport.initWithOptions` both return `error{OutOfMemory}` only. The first is Stable (`api-snapshot.txt:1077`) and the second is Experimental (`api-snapshot-experimental.txt:3253`) [V].
- `tcp.Listener` fields are Experimental (`api-snapshot-experimental.txt:3188-3197`). Only `close`, `getAddress`, `init` and the struct alias are Stable (`api-snapshot.txt:1080-1083`) [V].
- `wasm-host` imports `Peer`, `protocol` and the cap table (`src/wasm/capnp_host_abi.zig:21-25`). CI builds it (`ci.yml:612`) [V].

**Unix transport**
- `Connection`, `Peer` and `ServerSession` already run over AF_UNIX. Interop with C++ works in both directions [S].
- **Debug panic on macOS.** `setTcpNoDelay` formats the errno with `{t}` (`src/rpc/transport/tcp/runtime.zig:200`) [V]. On a Unix fd, macOS returns errno 102, which std does not name, so a Debug build panics [S].
- `acceptFd` always calls `setTcpNoDelay`. Its inferred error set feeds the frozen `ServerSession.accept` (`runtime.zig:105-111`, `docs/api-snapshot.txt:1094`) [V].
- `Listener.initFd` hard-codes `.protocol = .tcp` and a fake `ip4` address (`runtime.zig:80-81`) [V].
- `sun_path` is 108 bytes on Linux (`std/os/linux.zig:7106`) [V] and 104 bytes on Darwin [S].
- **Accept wake-up on macOS.**
  - `shutdown` on a listening socket fails with ENOTCONN and does not wake `accept`. This is true for AF_UNIX **and** TCP (`survey/claims/unixaccept`, `tcpaccept`, re-run) [V].
  - `close` wakes it with ECONNABORTED (`survey/critic-completeness/accwake`, re-run) [V]. `Listener.close` calls both (`runtime.zig:134-137`) [V].
- **WorkerPool.**
  - `WorkerPool.init` takes a `net.IpAddress` only (`src/rpc/integration/worker_pool.zig:112-123`) [V]. So nobody can build a Unix pool today.
  - Shutdown loops on nudges, with no bound, until no acceptor is parked (`:258-261`). The nudge dials the listener's IP address (`:455`) [V].
  - The e2e server builds its listener through `WorkerPool.init` with `IpAddress.parse` and `.concurrency = 1` (`tests/e2e/zig/main_server.zig:1996-2005`) [V].
- `events.Source` (`src/rpc/events.zig:50-57`) and `events.Resource` (`:80`) are exhaustive [V]. `DisconnectCause` is non-exhaustive (`_` at `:155`) [V].
- **bind/listen race.** A connect between `bind` and `listen` gets `ECONNREFUSED` [V, re-run]. So a connect probe cannot prove that a socket file is stale.

**FD passing**
- `Transport.read` calls `net_read` with no control buffer (`src/rpc/transport/tcp/stream_transport.zig:259`, `:505-512`) [V]. `readTimeout` uses `ioReadVecTimeout` (`:280-284`) [V].
- `attached_fd` is decoded but never used, and sends set it to null (`src/rpc/wire/protocol.zig:65-110`, `:849-854`; `peer_promise_exports.zig:41,114,291`) [S].
- **macOS behaviour** (`survey/claims/fdprobe`, `emfile`; `fdmerge` rebuilt in `survey/finalizer/` from the same source and re-run) [V]:
  - A plain `read` installs the fds and leaks them.
  - A truncated control buffer still installs every fd. Only the fds whose numbers fit can be closed (4 sent, 1 visible, 4 installed).
  - One `recvmsg` never merges two sends that carry fds. A 512-slot buffer got 254 fds from 2×254 and 3×254 sends. So a peer cannot cause CTRUNC against a 512-slot buffer.
  - The per-`sendmsg` limit is 254; 255 gives EINVAL. Received fds are not CLOEXEC, and `MSG_CMSG_CLOEXEC` is not defined.
  - At EMFILE, the first `recvmsg` fails and installs nothing. The retry returns the data with `controllen = 0` and no CTRUNC, so the fds are lost.

  This contradicts the spec text at `src/rpc/capnp/rpc.capnp:1118-1124`.
- **Blocking close on Linux.** A peer can send a TCP socket that has `SO_LINGER {1, 3}` and unsent data. The final close then blocks the closing thread for 3 s. Re-run in the local `e2e-cpp-rpc` container (arm64) [V]:
  - `recvmsg` with no control buffer (today's path) took 3054 ms;
  - `close` of the received fd took 3021 ms.

  On macOS both take 0 ms [S].
- The default soft fd limit on this Mac is 256 (`launchctl limit maxfiles`) [V].
- With bulk reads, the kernels attach fds to different bytes: the last byte of the read on Linux, the first byte on macOS [S].
- **Exact-boundary reads** (header first, then body) put each fd in the frame whose bytes carried it. A hostile fd attached mid-frame stays inside that frame. Cases E1-E4, E3 and E3b pass on macOS [V, re-run] and on Linux [S].

**Ticket keys**
- A 48-byte key, set on `server.tls_ctx.inner` after `quic_zig.Server.init`, gives `.accepted` 0-RTT after a restart on quic v0.25.0 [S]. Both proof build logs end with `exit=0` [S].
- The proof declares `extern fn zbssl_SSL_CTX_set_tlsext_ticket_keys` itself. It does not use quic's `boringssl` module [S]. boringssl-zig exports its translate-c bindings as `pub const raw` (`src/root.zig:25`) [V].
- `listener.zig:70` is the only `quic_zig.Server.init` call. `peer_server.zig:184` calls the RPC-level `server_mod.Server` [V].
- `serverConfigFromOptions` is public (`src/rpc/transport/quic/mod.zig:175`). It returns a `quic_zig.Server.Config`, not a server, so it cannot install a key (`options.zig:523-526`) [V].
- `ServerProductionHardening` requires `retry_token_key`, and `new_token_key` is optional (`options.zig:459-476`) [V].
- `options.zig` has no ticket-key, `early_data_application_context` or `tls_context_override` field (grep) [S].

**Consumers**
- `Event.cancel_failure` (`events.zig:173`) came in 79d1ede, which is in v0.19.0 [S].
- **capnp-qmsg-demo.** `src/endpoint_metrics.zig:81-103` switches on `Event`. It has no `.cancel_failure` arm and no `else` [S]. It builds against live main by path [S], so it does not compile today [I]. mruby-quic has the same switch (`src/endpoint_metrics.zig:130`) [S].
- None of capnp-qmsg-demo, mruby-quic, qmsg, qmesh or prollytree switches on `events.Source` or `events.Resource` [S].
- No consumer asks for Unix sockets or FD passing [S].
- slcp drives `tcp.Transport` directly, so it does not go through `Connection` (`tests/package_consumer/src/default.zig:21-30`) [V].
- quic-zig is waiting for our ticket-key bridge [S]. slcp asks for a reusable snapshot walker [S].
- No `unreleased-after: v0.19.1` markers exist. The checker is `tools/docs_examples_smoke.zig:219` [V].

## Ordered items

### Week 1: the gate first, then a Unix seam that cannot leak or stall

**Schedule.**
- Items 1-3 run first, on days 1-3.
- From day 2, items 4-7 run in a second worktree. They touch no codegen.
- On day 1, beside item 1, write the docs-only handoff for capnp-qmsg-demo and mruby-quic: add an `else` arm or a `.cancel_failure` arm. Do not push to those repos.
- On day 1, item 2 produces the D1 closure list. D1 is due on day 2. D3 is due on day 3, before item 6 emits events.

1. **P0 M: Shape gate, step 1. Extract the walker and the renderer.**
   - **Change:** move these from `tools/api_snapshot.zig` to a new `tools/snapshot_render.zig`:
     - the renderers: `renderErrorSet` (`:428`), `renderFnType` (`:455`), `defaultSuffix` (`:505`), `renderValue` (`:518`), `fieldEntries` (`:539`), `normalizeLine` (`:1048`), `canonicalizePlatformTypes` and `platform_type_aliases` (`:1028-1046`);
     - the walker: `walk` (`:639`), `isContainer`, `containerKind`, `foreignType` and `contains` (`:382-420`);
     - the closure check: `collectTypes`, `peel`, `tierOfType` and `collectClosure` (`:755-812`).
   - **Seam:** configure the module with a table: tier rules, overrides, the root and the depth limit. It must not `@import("capnpc-zig")`. This is the walker seam that slcp asked for.
   - **Option:** `render_const_values: bool = false`. The library snapshots keep `false`, so their files do not change. Item 2 sets it to `true`.
   - **Accept when:**
     - all three `docs/api-snapshot*.txt` files are byte-identical;
     - `zig build check-api`, `check-api-experimental` and `api-closure` are green;
     - `zig build -Dquic=true check-api-experimental-quic` is green.
   - **Proof:** an empty diff.

2. **P0 M: Shape gate, step 2. Corpus, walker and two snapshots.**
   - **Corpus.** Commit `tests/generated_shape/requests/*.request.bin`. `just gen` writes them.
     - Add the `tests/generated_shape/requests` directory to the `git diff` list at `Justfile:310`, as the `addressbook.request.bin` precedent does. Do not add the files to `generated_paths`, because `zig fmt --check` fails on a `.bin` file [S].
     - Full profile with reflection: addressbook, kvstore, pingpong, `rpc_inherited_paths` with its external file, `inherited_method_collision`, streaming, `rpc_pipeline_paths`, `nested_interfaces`, `generic_rpc`, `enum_evolution_v1`, `union_member_guard_runtime`, `defaults` and rpc/persistent.
     - Also in the full profile, because renames happen there: `nested_collisions`, `nested_interface_collisions`, `zig_field_names`, `runtime_guard_names`, `edge_codegen`, the five `brand_*` fixtures, `generic_collections`, `generic_recursive`, `rpc_nested` and `annotations` (`ls tests/test_schemas`) [V].
     - Compact profile: kvstore and `generic_rpc`.
     - `--no-reflection`: kvstore and addressbook.
     - `tests/generated_shape/instances.zig` instantiates `Apply` and the generics.
   - **Tool.** Add `tools/generated_shape.zig`. It uses item 1's module with `render_const_values = true`.
     - Add the build steps `generated-shape` (writes) and `check-generated-shape` (strict on both files). Both set `has_side_effects = true`.
     - The host plugin runs on the committed requests with `--output-dir=`. The precedent is `build/build_impl.zig:358-373`. No schema compiler is needed.
     - `just gen` does **not** write the snapshot files. Only `zig build generated-shape` writes them.
   - **Walker rules:**
     - Hitting the depth limit is an error, not a silent stop.
     - Runtime type names are rewritten to their public paths.
     - Scalar const values render: ints, bools, enums, and strings up to 64 bytes. This freezes `interface_id`, method `ordinal`, `is_streaming` and schema const values.
     - The tier rules sit in a table (D1). Each rule must match at least one line.
     - A Stable shape line that names an Experimental type fails. This applies to runtime types and to generated types.
     - A census requires at least one of each: `StreamClient`, `PipelinedClient`, an inherited `From` family, `WhichTag`, a guarded group getter, a const, an `Apply` instance, a compact entry, a no-reflection entry, a collision-renamed decl and an escaped field name.
     - A corpus entry that does not compile fails, and the message names the entry. The message also says that runtime error-set changes move the file.
   - **Outputs:**
     - `docs/generated-shape.txt` (Stable, frozen);
     - `docs/generated-shape-experimental.txt` (strict staleness check).
   - **Day-1 checks:**
     - Render on macOS, Linux and Windows, then diff the outputs. If only platform type names differ, extend `platform_type_aliases`. If other lines differ, stop and redesign.
     - Run the walker with D1(a) and list every closure violation (see the D1(a) bullet in "State"). Give the list to the owner with D1.
   - **Ablations.** Record each one in the commit message.

     | Change | Expected |
     |---|---|
     | T1: `{s}From{s}` → `{s}Via{s}` at `src/capnpc-zig/generator.zig:999`, `:1038` | red, on Stable lines |
     | T2: `release(self: *const …)` at `interface_gen.zig:96` | red, one line per non-generic `Client` (the census count) |
     | T3: an equivalent guard rewrite at `struct_gen.zig:2572` | **green** |
     | T4: `if (true) return;` in `writeUnionMemberGuard` (`struct_gen.zig:2560-2573`) | red: the guarded getters' error sets lose `WrongUnionMember` |
     | T5: change the `interface_id` emit in `interface_gen.zig` | red, on Stable lines |
     | Replay da60cb6 | red, compile failure with the entry named |
     | Widen one generated `setX` error set | red |
     | Drop one corpus entry | census red |

3. **P0 S: Shape gate, step 3. CI, release hook and docs.**
   - **CI:** run `zig build check-generated-shape` in the Hardening job (`ci.yml:468-509`) on all three OSes. Keep it out of the Test job.
   - **Release hook:** add `just check-release-drift <prev-tag>`. Call it from `release-preflight` (`Justfile:337`) and from `release-tag` (`:353`).
     - It reads five files: `docs/api-snapshot.txt`, `docs/api-snapshot-experimental.txt`, `docs/api-snapshot-experimental-quic.txt`, `docs/generated-shape.txt` and `docs/generated-shape-experimental.txt`.
     - It fails when a Stable file (`api-snapshot.txt` or `generated-shape.txt`) removes or changes a line, and the release section has no `### Breaking`.
     - It fails when any of the five files changed, and the bump is a patch (`RELEASING.md:25-31`).
     - It warns when an experimental file loses lines and the release section has no Breaking (Experimental) entry.
     - If a file does not exist at `<prev-tag>`, all its lines count as added.
   - **Ablations.** Run them in a scratch worktree against a local throwaway tag. Never push that tag.
     - edit one Stable line → red;
     - add one line only → green;
     - make a patch bump with an added line → red;
     - make a patch bump with an experimental-only change → red.
   - **Docs:**
     - Rewrite `RELEASING.md:33-41` to point at the gate.
     - Add a section "What is frozen in generated code" to `docs/generated-api.md`.
     - Narrow `supported-surface.md:63` to the D1 Stable families, and fix `:62` and `:96`.
     - Fix the stale comment at `Justfile:304-308`.
     - Add a `stability.md` row and a CHANGELOG Added entry.
   - **Handoff (docs only):** tell slcp-zig about the walker seam and the snapshot file to diff.

4. **P0 S: Fix the macOS Debug panic.**
   - **Fix:**
     - At `runtime.zig:200`, log `@intFromEnum(err)`.
     - Skip NODELAY on sockets that are not IP sockets. If `getsockname` fails, ignore it, so the `acceptFd` error set does not change.
   - **Red-first test:** `tests/rpc/transport/unix/rpc_unix_regression_test.zig`.
     - Set `std.testing.log_level = .debug` **inside the test body**. The runner resets it to `.warn` before each test (`test_runner.zig:287`).
     - The test accepts a connection on a Unix listener built with `Listener.initFd`.
     - It is red on `macos-latest` today.
   - **Ablation:** put `{t}` back, and macOS panics. Linux stays green either way.
   - **CHANGELOG:** a Fixed entry.

5. **P0 S: Kernel-semantics test (FD-0) and fork handoffs.**
   - **Test:** `tests/rpc/transport/unix/unix_kernel_semantics_test.zig`, skipped on Windows. It pins these behaviours:
     - an fd round-trip, with FD_CLOEXEC set on Linux (with `MSG_CMSG_CLOEXEC`) and not set on macOS;
     - exact-boundary attribution: E1-E4, E3 and E3b;
     - the bulk-read anchor per OS (this records why D2 picks exact-boundary reads);
     - the macOS leaks T3 and T4;
     - on macOS, a truncated control buffer installs every fd, and one `recvmsg` never merges sends that carry fds;
     - the per-`sendmsg` limit: 253 on Linux, 254 on macOS;
     - EMFILE: Linux delivers a partial list with CTRUNC. macOS fails the first `recvmsg` with EMFILE and installs nothing; the retry returns the data with no fds and no CTRUNC;
     - on Linux, the final close of a received lingering socket blocks the closing thread; on macOS it does not;
     - a fixed `cmsg` byte buffer decodes to the expected fd numbers. This is the byte-order proof, because the powerpc64 job only compiles.
   - **Registration:** register the test in the Test suites and in the TSan list (`build_impl.zig:1512-1523`). The TSan lane covers the glibc `cmsghdr` layout.
   - **Accept when:** green on `macos-latest`, ubuntu and the TSan lane. No CI lane runs tests on aarch64 Linux. So run the test once in the local arm64 container, and record the output in the commit message.
   - **Ablation:** flip the expected result for one OS at a time. Each OS goes red.
   - **Handoffs.** Docs only, never filed upstream. Each one includes its scratch repro and the exact output.
     - `docs/upstream/handoff-zig-fork-unix-address.md`:
       - `UnixAddress.max_len` is 108, but Darwin's `sun_path` is 104 bytes. Long paths panic out of bounds (`Threaded.zig:14391`).
       - Abstract names get an extra trailing NUL (`:14392-14396`).
       - `createPair` accepts IP families only (`net.zig:1250-1251`).
       - Darwin errno 102 is `EOPNOTSUPP` (SDK `sys/errno.h:251`), but std maps `OPNOTSUPP` to 45 (`c/darwin.zig:1392`), so `c.darwin.E` has no value for 102 [S].
     - `docs/upstream/handoff-zig-fork-scm-rights.md`:
       - `cmsg.Iterator` drops a truncated header (`net.zig:1537-1540`).
       - `recvmsg` maps EMFILE to `unexpectedErrno` (`Threaded.zig:12965-12981`).
       - `sendmsg` maps EBADF and EINVAL to `errnoBug` (`:13434-13460`).

6. **P0 L: Raw fd reads, drain mode on every AF_UNIX connection, and off-thread close.**
   - **New module:** `src/rpc/transport/unix/fd_io.zig`, with `recvWithFds` on `posix.system`.
     - The control buffer is `cmsg.space(512 * @sizeOf(fd_t))`. On macOS this size is the truncation defense, because one `recvmsg` carries at most one send's 254 fds [V].
     - It uses its own clamped parser, not `cmsg.Iterator`.
     - It reads fd ints in native byte order.
     - It uses `MSG_CMSG_CLOEXEC` where the platform defines it. Otherwise it calls `fcntl(FD_CLOEXEC)` right after `recvmsg`.
     - It returns typed errors. It handles EINTR and EAGAIN itself and never reaches `errnoBug`. It maps new errors into the existing `Transport.read` error set (`api-snapshot-experimental.txt:3258`).
     - **EMFILE:** emit the drop event from the first EMFILE, then retry once. On macOS, the retry returns the data and the kernel drops the fds [V]. If the retry also fails with EMFILE, close the connection with a typed cause and an event. Never spin.
     - It is compiled out on Windows and WASI.
     - Every new pub fn has an explicit named error set.
   - **Drain mode:**
     - `Transport.initWithOptions` (`stream_transport.zig:223`) reads the socket family once, with `getsockname`. `Connection.init` reaches it (`connection.zig:258`), and so does slcp, which drives `Transport` directly.
     - On AF_UNIX, every read goes through `recvWithFds`, and every received fd goes to the closer.
     - If `getsockname` fails, use `recvWithFds` anyway. `recvmsg` also works on TCP. Both init error sets stay `error{OutOfMemory}`.
     - The TCP path does not change otherwise. No Stable signature changes.
     - `readTimeout` keeps its deadline: poll first, then `recvWithFds`.
   - **Off-thread close:**
     - Never close a received fd on the reader thread or the Peer thread. On Linux, the final close of a lingering socket blocks for its linger time [V].
     - Add one closer thread per process, started on first use. Readers hand fds to it. `Connection.deinit` never waits for a pending close.
     - Bound the closer queue by the process fd budget (item 13; `RLIMIT_NOFILE / 4` until item 13 lands). When the queue is full, close the connection that sent the fd with a typed cause, and stop reading from it.
     - Because AF_UNIX reads always pass a control buffer, the kernel no longer does the final close inside `recvmsg`.
     - Residual: at Linux EMFILE, the kernel drops the extra fds inside `recvmsg`, on our thread. Document it.
   - **Red-first tests:**
     - `tests/rpc/transport/unix/rpc_unix_fd_drain_test.zig`. A raw peer sends a valid frame plus a pipe write end. After close, the fd count is back at baseline, and the pipe reader sees EOF. It is red on macOS today.
     - `tests/rpc/transport/unix/rpc_unix_linger_test.zig` (Linux). A raw peer sends a TCP socket that has `SO_LINGER {1, 3}` and queued data, then a valid frame. The frame dispatches within 100 ms. `close` plus `deinit` finish within 1 s. It is red on Linux today (3054 ms inside `recvmsg`) [V].
   - **Accept when:**
     - Linux CTRUNC and EMFILE (force EMFILE with `setrlimit`): the fd count is back at baseline, and the connection survives.
     - macOS EMFILE: the connection survives, the event is emitted, and there is no busy loop.
     - macOS CTRUNC: it happens only with a small test-only buffer. The criterion is: every visible fd is closed, the leak equals sent minus visible, and the doc says so. It is not "back at baseline".
   - **Ablations:**
     - use a plain `read` on Darwin → the fd count grows;
     - use `cmsg.Iterator` with the small test buffer → the visible fds of the truncated header stay open, and the test goes red;
     - close on the reader thread → the Linux linger test goes red.
   - **CHANGELOG:** a `### Security` entry. Today an AF_UNIX `Connection` installs and leaks fds that a local peer sends (macOS), and a lingering fd can stall its reader (Linux). It names who is exposed: users of `Connection`, `Transport`, `Listener.initFd` or `ServerSession` over AF_UNIX. D4 decides whether a patch release carries this fix.

7. **P0 M: `rpc.transport.unix` listen and connect (Experimental, POSIX only).**
   - **Where:** add the namespace to the `Transport` factory (`src/rpc/mod_base.zig:35-60`). Only the full roots get it; core has no sockets.
   - **API.** All of it is additive.
     - `unix.listen(gpa, io, path, unix.ListenOptions) !tcp.Listener`. It returns the existing Listener, so the frozen `ServerSession.accept` works unchanged.
     - `unix.connect(gpa, io, path, unix.ConnectOptions) !*tcp.ClientSession`. `unix.ConnectOptions` has a `session: tcp.ConnectOptions` field and `connect_timeout_ms` (a non-blocking connect plus poll). On Linux, a blocking connect to a full backlog waits [I].
     - `tcp.Listener.unixPath() ?[]const u8`. New `Listener` fields are Experimental lines.
   - **Socket setup.** Use raw `socket`, `bind`, `chmod` and `listen`, not std's `UnixAddress`. This avoids the panic on 105-108-byte paths and the abstract-name NUL bug.
     - **Path length:** return `NameTooLong` when `path.len >= sun_path.len` (104 on Darwin, 108 on Linux).
     - **Abstract names:** a leading NUL returns `error.AbstractNameUnsupported`.
     - **Lock:** every `unix.listen` opens `<path>.lock` with `O_NOFOLLOW|O_CLOEXEC` and takes `flock(LOCK_EX|LOCK_NB)`. It holds the lock until `close()`. If another process holds the lock, return `AddressInUse`. Never unlink the lock file.
     - **Stale files:** `reclaim_stale: bool = false`. If it is true and we hold the lock, unlink the old socket file, then bind. If it is false, `bind` returns `AddressInUse`. Never use a connect probe. The doc says that every server on a path must use the lock (all servers built with `unix.listen` do).
     - **Permissions:** `socket_mode: u32 = 0o600`, applied between `bind` and `listen` [S]. Right after `bind`, record the path's (dev, ino) with `lstat`. Before and after `chmod`, check that the path still has that (dev, ino). The supported layout is a private 0700 directory, which removes the swap window.
     - **Close:** `close()` unlinks the path only if it still has our (dev, ino).
     - **Flags:** set CLOEXEC on the listening socket. std already sets CLOEXEC on accepted fds [S]. No NODELAY.
     - **Windows:** the calls return `error.UnixSocketsUnsupported`.
   - **Hard rules:**
     - Do not edit the eight frozen RPC prefixes (see "Frozen RPC code").
     - `acceptFd` keeps its error set (`runtime.zig:105-111`).
     - Every new pub fn has an explicit named error set.
   - **Package consumers:**
     - `default.zig` and `quic.zig` force `&unix.listen`, `&unix.connect` and `&tcp.Listener.unixPath`.
     - `core.zig` asserts that `transport.unix` is absent.
   - **Example:** `examples/rpc_pingpong_unix.zig` and `zig build example-rpc-unix`. CI runs it next to `ci.yml:194`, with `if: runner.os != 'Windows'`.
   - **Tests:** `tests/rpc/transport/unix/rpc_unix_session_test.zig`. Register it near `build_impl.zig:838` and in the TSan list. Use short paths under `/tmp`. Cases:
     - bootstrap plus one call;
     - a path of length `@typeInfo(@FieldType(posix.sockaddr.un, "path")).array.len` returns `NameTooLong`, and a path one byte shorter binds;
     - a stale file with reclaim off returns `AddressInUse`;
     - a stale file with reclaim on succeeds;
     - a reclaimer against a live listener gets `AddressInUse`, and the live listener still serves;
     - two racing reclaimers: exactly one wins, and the loser gets `AddressInUse` while the winner is alive;
     - the socket file has mode 0600;
     - `close` unlinks only our own inode;
     - `Listener.close` wakes a parked accept on both OSes (on macOS through the `close` half);
     - `connect_timeout_ms` returns a timeout against a full backlog (Linux);
     - Peers over `socketpair(AF_UNIX)`.
   - **Accept when:**
     - `docs/api-snapshot.txt` and `docs/generated-shape.txt` are byte-identical;
     - the experimental snapshots are refreshed, `-Dquic` steps first and the plain snapshot last;
     - the x86_64-windows, x86 and powerpc64 cross-compiles are green.
   - **Ablations:**
     - remove the length guard → macOS panics;
     - drop the `mod_base` export → package-preflight goes red;
     - remove the inode check → the unlink test goes red;
     - release the lock after `bind` → the racing-reclaimer test goes red.

8. **P1 S: e2e over Unix sockets, Zig to Zig and Zig to C++.**
   - **Binaries:** `tests/e2e/zig/main_server.zig` and `main_client.zig` accept `--host unix:/path`.
     - For a Unix path, `main_server` uses `unix.listen` and a single-threaded `Listener.accept` loop that calls the same `onAccept`. The pool runs with `.concurrency = 1` today (`main_server.zig:1998-2005`) [V], so concurrency does not change. This item does not depend on item 9 [I: `onAccept` fits].
   - **Zig to Zig:** add `zig build e2e-self-unix` on the Linux and macOS Test legs.
   - **Zig to C++:**
     - kj's `parseAddress` accepts `unix:` (`kj/async-io-unix.c++:1000`) [S]. The C++ side needs no change.
     - `tools/e2e_runner.zig` gains `--transport=unix`, for the C++ backend only.
     - Go, Python and Rust print `SKIP(unix: reference harness TCP-only)`.
     - Both peers run inside the e2e-cpp-rpc container. Build the Zig side for the image's architecture. Read it with `docker image inspect -f '{{.Architecture}}'`. CI (ubuntu-latest) needs `x86_64-linux-musl`, and the local image needs `aarch64-linux-musl` [V].
     - Add a step to the "Zig e2e interop" job (`ci.yml:542-578`).
   - **Accept when:** game_world, chat, inventory and matchmaking pass in both directions.
   - **Ablation:** point the Zig server at a wrong path. The lane goes red, not SKIP. This proves that the lane uses the runner's existing `FAIL(NO_TESTS)` path.

9. **P1 M: Unix WorkerPool.**
   - **Gap:** `WorkerPool.init` takes a `net.IpAddress` only (`worker_pool.zig:112-123`). Nobody can build a Unix pool today.
   - **Change:**
     - Add `WorkerPool.initListener` (Experimental). It takes a `tcp.Listener` from `unix.listen`.
     - On a Unix listener, park acceptors in `poll(listen_fd, wake_fd)` (the wake-door pattern). Shutdown writes the wake door.
     - Do not dial the path. If the path was unlinked or rebound, a path nudge never reaches us, and shutdown loops forever (`:258-261`).
     - Follow the wake-door contract: `Io.Threaded` panics on EAGAIN, so a non-blocking fd must never reach Io's accept [S].
     - Add the constructor to the package-consumer references and to the experimental snapshots.
   - **Tests:**
     - With 4 parked workers on a Unix listener, shutdown takes less than 2 s on macOS and Linux, with a margin of at least 300 ms.
     - A second case unlinks the path, then shuts down. It must also finish in under 2 s.
   - **Slip rule:** this is the first P1 to slip. Item 8 does not depend on it.

### Week 2: FD passing on a branch, 0-RTT, release candidate

**Branch rule.** Items 10-13 and 15 live on the branch `sprint/fd-passing`, in their own worktree. A draft PR to main runs CI on the branch (`ci.yml:3-7`). Rebase the branch on main every day. The branch merges only after item 14 closes. It merges as one unit, or not at all.

Item 16 touches only `src/rpc/transport/quic/` and `build/`. It runs in its own worktree from day 3 and lands on main after item 7.

10. **P0 M: Send path, with fds in the write queue.**
    - **`fd_io.sendWithFds`:**
      - rejects more than 253 fds before the syscall;
      - uses one `SCM_RIGHTS` cmsg and `MSG_NOSIGNAL`;
      - sends the control data with the first chunk only;
      - returns typed errors.
    - **Write queue:**
      - Items become `{bytes, fds}` (`stream_transport.zig:91-99`).
      - Keep one item per syscall (writer at `:386-410`).
      - Enqueue dups each fd with `F_DUPFD_CLOEXEC`. The dups count against the process fd budget (item 13).
      - The writer closes the dups after the send, on error, in `WriteQueue.drain` (`:193`) and in `stopWriter` (`:376-383`).
      - At most 256 fds can be in flight per connection. Over that, the caller gets a backpressure error.
    - **Accept when:**
      - the fd count is back at baseline in three cases: after N sends, after teardown with items still queued, and after a writer error mid-batch;
      - TSan is green;
      - `zig build wasm-host` is green.
    - **Ablation:** skip the close in `drain`, and the leak test goes red.

11. **P0 M: Exact-boundary reads when fd passing is on (D2).**
    - **Reads:** when `max_fds_per_message > 0`, the Unix transport reads one frame at a time: header, segment table, then body. Fds that arrive while frame F is read belong to F.
      - Check the segment count and `max_buffered_frame_bytes` (the frozen Framer's limits) from the header before any allocation.
      - Limit each read's iov to the bytes left in the current part, so a read never crosses a frame boundary.
    - **Policy:**
      - **More than one fd batch in a frame:** protocol error. Close all fds and disconnect.
      - **More fds than the cap:** close the extras (`rpc.capnp:1112-1167`).
      - **CTRUNC or EMFILE:** deliver the frame with zero fds, close the fds from that read, emit an event and keep the connection.
      - **A frame that is never dispatched:** close its fds. This also applies on reset and close.
      - All closes go through the item-6 closer.
    - **Default:** `max_fds_per_message = 0` keeps bulk reads in drain mode.
    - **Tests:**
      - replay E1-E4, E3 and E3b;
      - a fuzz test with random read splits, random fd placement and hostile headers asserts zero leaks, no cross-frame attribution and no allocation past the Framer limits.
    - **Ablations:**
      - use a bulk read with a first-byte anchor → Linux puts B's fd on frame A, and the tests go red;
      - remove the close on reset → the leak test goes red.

12. **P0 L: Peer seam for `attachedFd`, outside the frozen cap table.**
    - **Frozen code. Do not change it:** the encoders (`api-snapshot.txt:965-968`), `InboundCapTable.init` (`:884`), `resolveCapDescriptor` (`:899`), `ImportCap`, every `CapTable` field, `Export`/`addExport` (`:981-984`) and `PeerLimits`.
    - **Config:** a new `rpc.transport.unix.FdPassing{ max_fds_per_message: u8 = 0 (hard cap 253), max_live_imported_fds: u32 = 64 }`. It goes on `unix.ListenOptions` and `unix.ConnectOptions` only.
    - **Binding:** `TransportBinding` (Experimental; 0 Stable lines) gains `max_outbound_fds` and a send-with-fds hook. TCP and QUIC report 0.
    - **Fd type:** wrap the fd in an `FdHandle` struct, as `SocketFd` does. Where fd passing is compiled out, it is an empty type. This keeps signatures the same on all platforms (`runtime.zig:167-169`) and keeps `wasm-host` compiling.
    - **Side tables:** a new file `src/rpc/peer/peer_fds.zig`. `caps/table.zig` does not re-export it. It holds export id → borrowed fd, and import id → owned fd.
    - **Outbound:**
      - Add `Peer.setExportFd(export_id, FdHandle)` (Experimental). The fd stays borrowed.
      - After `encode*WithEffects` returns, a Peer post-pass walks the built cap-table list. For each `senderHosted` descriptor whose export has an fd, it sets `attachedFd` and adds the fd to the frame's fd list. The rules: only when the binding allows it, never past 253, and each fd on at most one descriptor (`rpc.capnp:1149-1150`).
      - The post-pass runs at the 7 encode sites and at Resolve:
        - `src/rpc/peer/call/peer_call_sender.zig:48,106,153,203`
        - `src/rpc/peer/return/peer_return_send.zig:68,181`
        - `src/rpc/peer/third_party/peer_third_party_routes.zig:164`
        - Resolve, through `protocol.zig:1118-1119`
      - If no existing builder accessor reaches the built list, put the helper in `peer_fds.zig`, not in `protocol.zig`.
    - **Inbound:**
      - Each frame owns its fds.
      - After `InboundCapTable.init` succeeds, a Peer pass reads each descriptor's `attached_fd` again (`protocol.zig:65-110`).
      - For `senderHosted` and `senderPromise` (`caps/inbound.zig:162-165`), it moves the fd into the import table only when that import has no fd. On a duplicate index, the first descriptor wins.
      - For `thirdPartyHosted` (`caps/inbound.zig:176-179`; `peer_resolve_inbound.zig:102`), `receiverHosted` and `receiverAnswer`, it does not attach the fd. The spec makes the `thirdPartyHosted` case optional (`rpc.capnp:1137-1146`), and it stays in Deferred.
      - The closer gets every fd that nothing takes, after dispatch.
    - **Close hooks:**
      - The Peer sends an import's fd to the closer when the import leaves `CapTable.imports`. Hook the Peer callers of `releaseImport`, `removeImportIfFullyReleased` and `releasePromiseImportRef` (`caps/lifecycle.zig:203,324,339`), and `Peer.deinit`.
      - A debug check after each release asserts that every id in the side table is still in `CapTable.imports`.
    - **App API:** `Peer.importFd(import_id) ?FdHandle` (Experimental).
      - The fd is borrowed and stays valid until the import is released. This matches C++ (`capability.h:278-288`).
      - A promise import returns null until it resolves.
    - **Tests:**
      - The server attaches a pipe write end to a returned cap. The client writes through `importFd`, and the server reads the data.
      - Both sides are back at their fd baseline after release.
      - Cases: duplicate index, out-of-range index, a second fd for the same import, over-cap, and a `thirdPartyHosted` descriptor with an fd (the fd is closed).
      - On TCP, `setExportFd` gives `attachedFd = 0xff`.
      - The wake-socketpair fds are never sent.
    - **Accept when:**
      - the Stable snapshot is byte-identical, and `zig build check-api` is green;
      - `zig build wasm-host` is green;
      - the experimental snapshots are refreshed.
    - **Ablations:**
      - red-first: add an fd field to `ImportCap` → `check-api` goes red;
      - remove each Peer close hook, one at a time → red each time;
      - remove the "only if missing" check → the duplicate test leaks.

13. **P1 S: Limits and fault injection.**
    - **Live-fd cap:** a per-connection cap. Over the cap, the runtime closes the fd and emits an event (D3).
    - **Process fd budget:** one atomic count in `fd_io`. The default is `RLIMIT_NOFILE / 4`, read at first use. (The macOS default soft limit is 256 [V], so four hostile connections at 64 each would fill it.)
      - Imported fds, queued dups and the closer queue count against it.
      - Over budget: a received fd goes to the closer with an event, and the connection stays up. A send gets a backpressure error.
    - **Injected faults:**
      - OOM at every allocation on the fd paths (`checkAllAllocationFailures`);
      - EMFILE;
      - a writer error;
      - peer deinit while imports are live;
      - Linux `ETOOMANYREFS`, which returns a typed backpressure error.
    - **Tests:** open N connections that each reach their per-connection cap. `accept` still works.
    - **Registration:** add the tests to the Hardening gate (`ci.yml:492-509`).
    - **Accept when:** no injected fault leaks an fd, and the connection survives EMFILE on both OSes.
    - **Ablation:** remove each close once. Each removal goes red.
    - **The OOM sweep is a precondition for the merge in item 14.** It cannot slip on its own.

14. **P0 S: FD security review, docs and example. This item gates the branch merge.**
    - **Gate:** nothing from items 10-13 or 15 reaches main until this item closes. It is never skipped.
    - **Docs:** `docs/rpc-unix-sockets.md`. It covers:
      - the path rules, a 0700 directory, the umask and the lock file;
      - `getAddress()` returns a fake address on a Unix listener, so use `unixPath()`;
      - the Windows status;
      - fd semantics, ownership, the platform matrix and the process fd budget;
      - a threat table.
    - **Threat table.** Each row names its proof:

      | Threat | Proof |
      |---|---|
      | fd flood | items 11 and 13 |
      | fd-table exhaustion, one connection | items 6 and 13 |
      | fd-table exhaustion across connections | item 13 budget test |
      | blocking close (`SO_LINGER` socket, tty, FUSE or NFS file) | item 6 linger test and closer |
      | a stuck closer fills the queue | connections that keep sending fds are closed; documented |
      | Linux EMFILE: the kernel drops fds inside `recvmsg` | accepted residual, documented |
      | macOS installs fds the receiver did not ask for | item 6 |
      | macOS truncation leak | buffer size (item 6); documented residual for small buffers |
      | fd attributed to the wrong frame | item 11 fuzz test |
      | hostile frame header | item 11 fuzz test |
      | CLOEXEC race on macOS | accepted residual window, documented |
      | fd-number reuse | item 10 |
      | use after release | item 12, plus doc |
      | received fd of an unexpected type | the app checks it with `fstat`; doc |
      | internal fds leaking out | item 12 wake-fd test |
      | fds over TCP or QUIC | item 12 |
      | SIGPIPE | item 10 |
      | byte order | FD-0 fixed-buffer decode test (the powerpc64 job only compiles) |
      | socket-file swap before `chmod` | item 7 inode check; 0700 directory |
      | stale-file reclaim race | item 7 lock, held for the listener's life |

    - **Example:** `examples/rpc_fd_passing.zig` and `zig build example-rpc-fd`. CI runs it on POSIX.
      - The server attaches a pipe write end.
      - The client calls `client.peer.importFd(client.cap_id)`. Both fields are public (`examples/pingpong.zig:361-366`) [S].
    - **Snippets:** add `tests/docs/rpc_unix_snippets_test.zig` to `test-docs-snippets`.
    - **Review:** run `/security-review` on the branch, then a separate critic pass. Then the owner reads the table.
    - **Fallback:** if this item is not closed by day 8, the branch does not merge before the tag. v0.20.0 ships the Unix transport and drain mode. FD passing targets v0.21.0.

15. **P0 M: C++ fd e2e (Linux), on the branch.**
    - **Where:** a new step `test-rpc-fd-cpp` in the reflection-conformance job (`ci.yml:28-75`). Measure the job against its 35-minute cap.
    - **Schema:** a new `tests/test_schemas/rpc_fd.capnp` with a minimal interface shaped like `writeToFd`. This avoids generating all of upstream `test.capnp`.
    - **Harness:**
      - fork, socketpair and exec, as in `tests/serialization/support/rpc_generic_cpp.cpp:94-104`;
      - the C++ side uses `TwoPartyServer::accept(stream, maxFds)` and `TwoPartyClient(stream, maxFds)`;
      - the Zig endpoint uses the real `Connection` and `Peer`.
    - **Ported tests** (`rpc-twoparty-test.c++:494-590`):
      - fill sizes 1 MiB, 64 KiB, 8 KiB and 0;
      - an fd on a pipelined cap;
      - a limit of 1 gives `secondFdPresent == false`;
      - both directions.
    - **Not on macOS:** kj's last-byte rule may pick the wrong message there [I].
    - **Ablation:** set the Zig `max_fds_per_message = 0`, and the second-fd check fails.

16. **P1 M: 0-RTT after a crash-restart.**
    - **16a S: forward-secrecy review, first.** Replace `docs/quic-transport.md:698-712`. The new text states:
      - **What a key-file thief can do:**
        - decrypt recorded 0-RTT data, including the Restore frames and the sturdy refs they carry;
        - impersonate the server to resuming clients until their tickets expire.
      - **What the thief cannot do:** read 1-RTT traffic, because only `psk_dhe_ke` is used.
      - **Rules for the key:**
        - the key is opt-in;
        - it lives in its own 48-byte file, mode 0600, written atomically;
        - it is never derived from the reset key, because the two keys have opposite sharing rules (`:574-598`);
        - persist `new_token_key` with it. With Retry on, a new `new_token_key` at each boot sends every restarted client a Retry, which drops its 0-RTT (`:696`);
        - a TLS-context reload must install the key again (quic `Server.zig:1692-1701`).
      - **Rotation:** a rotation is a restart with a new key. It costs one full handshake for each client, not an outage. State a cadence and the ticket lifetime that bounds the exposure.
      - **Anti-replay:** why the key is refused together with anti-replay.

      A critic and the owner review it before 16c merges. 16c never merges without it.
    - **16b S: build.** Import quic's exported `boringssl` module into the QUIC roots (`build/modules.zig:86-107`, `build_impl.zig:1365-1377`). Call the setter through `boringssl.raw` (boringssl `src/root.zig:25`). Note the coupling: the scratch proof called `zbssl_SSL_CTX_set_tlsext_ticket_keys` directly.
      - Accept when `zig build -Dquic=true check` and the cross-compiles are green, and the `raw` call links.
    - **16c M: the option.**
      - **Shape:** add `ServerOptions.session_ticket_key: ?*const [48]u8`, and the same field on `ServerProductionHardening`. `Listener.init` reads it once. The caller may zero it after that.
      - **Install:** right after `Server.init` (`listener.zig:70`, the only call) and before the loop starts, because BoringSSL's setter takes no lock [S]. Read the key back and compare.
      - **Other paths:** `serverConfigFromOptions` returns `error.InvalidConfig` when a key is set, because it cannot install one.
      - **Premise checks, first:**
        - with Retry on and no `new_token_key`, a restart drops the 0-RTT;
        - with a new `new_token_key` after the restart, the 0-RTT is also dropped;
        - find BoringSSL's default ticket lifetime, and check whether `raw` exposes the PSK-DHE timeout setter. If it does, add `session_ticket_lifetime_s`.
      - **Refuse:**
        - an all-zero key;
        - a key together with `.with_anti_replay`;
        - a key with Retry on and `new_token_key == null`.
      - **Helper:** `loadTicketKeyFile(path)` reads exactly 48 bytes. On POSIX it refuses a file that the group or others can read. The docs give Windows ACL guidance.
      - **Context:** forward `early_data_application_context`, built from the ALPN, the mode and `early_dispatch` (`options.zig:523-559`).
      - **Observability:** add `WarmRedialClient.Outcome.zero_rtt_generations` (`warm_redial.zig:108-118`).
    - **Tests:**
      - In `rpc_quic_transport_test.zig` near `:2622-2745`, the same key gives `.accepted`.
      - Permanent negatives, each `.rejected`:
        - no key;
        - another key name;
        - the same name with another secret;
        - a different `early_dispatch`;
        - the same key with a new `new_token_key`.
      - `serverConfigFromOptions` refuses a key.
      - A heal test near `rpc_quic_peer_test.zig:2871` reuses the same `new_token_key`: the 2nd generation gives `.accepted`. A companion run without the key heals but gives `.rejected`.
      - Use Windows margins of at least 300 ms.
    - **Accept when:**
      - the `-Dquic=true` legs are green on all three OSes, ReleaseSafe included;
      - the Experimental-quic snapshot is refreshed;
      - the hardening gate passes.
    - **Ablation:** delete the install line, and the `.accepted` test goes red [S in scratch].
    - **16d S: handoff and ledger.**
      - Write `docs/upstream/handoff-quic-zig-ticket-keys.md`. It is a document, not an issue. It asks for:
        - a config field installed in `buildServerContext`;
        - rotation on the loop thread that keeps the previous key;
        - a ticket-lifetime setting;
        - a fix to the `.override` advice at quic `Server.zig:1692-1701`.
      - Move rung 1 (`docs/quic-durable-caps-plan.md:42-48`) to the ledger.
      - Send a pointer to the http3-zig session.

17. **P2 S: Ticket-key soak.** On Nightly, run the abrupt-death soak with a key, a persisted `new_token_key` and `.restore_only`. Accept when at least one heal per death is accepted as 0-RTT.

18. **P0 S: v0.20.0 release candidate. The owner approves the tag.**
    - **Bump:** minor (`RELEASING.md:30`). There is no `### Breaking` while both Stable files are unchanged. The one exception is the D3 entry, if the owner chooses option A.
    - **Preconditions:**
      - Freeze the RC commit by day 8.
      - 2 green Nightly runs on the RC commit. Use the cron or `workflow_dispatch` against the RC ref (`nightly.yml:3-7`), so pushes from other sessions do not reset the count.
      - `just release-preflight` is green, including the drift hook;
      - FD-0 is green on macOS and ubuntu;
      - item 14 is closed and the branch has merged, or the fallback applies.
    - **Docs:**
      - CHANGELOG entries, including the item-6 Security entry.
      - Rows in `stability.md` and `supported-surface.md`:
        - Unix transport: Experimental, POSIX only;
        - FD passing: Experimental, Linux and macOS (only if the branch merged);
        - ticket key: Experimental-quic.
      - Remove any `unreleased-after` markers added this sprint. None exist today. `tools/docs_examples_smoke.zig:219` checks them.
    - **After the owner approves:**
      1. `just release-tag 0.20.0 "<theme>"`
      2. `just verify-release-hash 0.20.0`
      3. Run a real `zig fetch`.
      4. Build the default, core and quic consumers against the fetched tag.
    - **Handoffs.** Send them as files. No pushes.
      - slcp: the shape gate and the walker seam.
      - quic-zig and http3-zig: ticket keys.
      - capnp-qmsg-demo and mruby-quic: the switch fix.
      - qmsg: the quic v0.25.0 security pin.
      - prollytree and bucketlist: scratch builds.

**Slip order:** 17, then 9, then the C++ half of 8, then 16 (to v0.21.0). FD passing (10-15) moves as one unit. It merges whole after item 14, or it moves to v0.21.0.

**These are never skipped:** items 1-7, item 14 before any FD merge, the OOM sweep before any FD merge, item 16a before 16c, any red-first test, any ablation and the release hook. A date never forces the RC. Only green gates do.

## Week 1 results (2026-10-04)

**Landed on main:** items 1-7 and 16a-16d. Item 6 is the `### Security` fix. Pushed in two steps: `6c8a5d0` (shape gate, FD-0, Unix transport), then the ticket key.

**Owner decision (2026-10-04): 16a approved, option A.** The ticket key merges with the Retry gap documented. Our part of the gap gets two new items (16e, 16f). The quic-zig part goes out as the handoff `docs/upstream/handoff-quic-zig-ticket-keys.md`.

**Corrections to this plan:**
- **macOS also blocks** on the final close of a received lingering socket (FD-0, 12/12 runs). The "macOS 0 ms" line in State and the blocking-close threat row now read "Linux and macOS". The closer design already covers both.
- **Two closer lanes, not one.** Item 6 has a `.received` lane (bounded) and a `.socket` lane, so a hostile fd cannot hold another connection's socket close.
- **`unix.connect` timeout:** a non-blocking AF_UNIX connect on Linux returns EAGAIN, and poll does not wait. The code uses a blocking connect with a send timeout on Linux and refuses at once on macOS (`8b7833d`).
- **Item 16c, fifth negative:** "same key with a new `new_token_key` gives `.rejected`" is false on quic v0.25.0. The verdict stays `.accepted`; the server sends a Retry and the restore runs after the handshake. The test asserts that, against a same-port control that restores early.
- **The 0-RTT verdict does not prove an early restore.** `WarmRedialClient.Outcome` now splits `zero_rtt_generations` (no Retry) from `retried_generations`.

**New items:**
- **16e P1 S: Restart-safe NEW_TOKEN clock.** `Listener.nowUs` (`src/rpc/transport/quic/listener.zig:285-290`) counts from process start. quic-zig stamps NEW_TOKEN issue and expiry times with it, so a restarted listener reads its predecessor's tokens as not yet valid. Anchor the clock to the wall clock at init, and keep it monotonic within the process. Red-first test: a token issued by generation 1 skips Retry in generation 2.
- **16f P1 S: WarmRedialClient keeps its local port.** A NEW_TOKEN is valid only from the address and port that earned it. A heal that dials from the previous generation's port skips the Retry. If the port is taken, fall back to an ephemeral port and count a Retry.
- **Item 17 acceptance (amended):** at least one heal per death has `zero_rtt_generations >= 1`. It needs 16e and 16f.

**quic-zig v0.26.0** was released on 2026-10-04 (no security fix). We hold v0.25.0 until this sprint ends, then bump. Expected cost for capnp-zig: no code change (our NEW_TOKEN envelope is length-prefixed, `warm_state.zig:10`, and we never request key updates).

**Downstream:** capnp-qmsg-demo does not build on main today (two quic modules: qmsg pins v0.24.1) and needs an `else` arm for `.cancel_failure`. Its full-stack test also fails with `QmsgLaneTimedOut` on the v0.19.0 pins. Handoff sent to the owner.

**Agent environment:** the agent sandbox refuses `source wt-setup.sh` and `git -c protocol.file.allow`. Agents copied the submodule trees by hand. Full-suite counts differ between worktrees, so the merge gate on main is the count of record.

## Week 2 results (2026-10-05)

**Owner decisions (2026-10-05):**
- **D-A = A.** FD passing (items 10-15) ships in v0.20.0: Experimental, Linux and macOS.
- **D-B = A.** A small soft `RLIMIT_NOFILE` is an app contract (threat-table row 41 in `docs/rpc-unix-sockets.md`). The app raises its own soft limit, to 1024 or more, before its first AF_UNIX connection. The library never changes process limits. The security review had asked the library to raise the limit when the closer starts, or to refuse drain mode below a minimum limit. Both are rejected: a library must not change a process-wide limit, and a refused drain mode brings back the macOS fd leak.

**Landed on main.** The week-2 lanes through `1b6e510` are pushed. Local `main` is at `e416dd3`: the FD-passing merge and its three follow-ups are not pushed yet, so no CI run covers them. (Pushed later that day, with a green CI run: see "Week 2, part 2".)
- Item 8, e2e over Unix sockets (Zig to Zig, Zig to C++): `56c0666`, `53b4e53`.
- Item 9, `WorkerPool.initListener`: `f9b987c`, `20381cf`.
- Items 16e and 16f, the restart-safe NEW_TOKEN clock and a `WarmRedialClient` that keeps its port: `5f5b677`, `6a00b0b`. The F8 pin at our seam: `018a90d`.
- Item 17, the ticket-key soak on Nightly: `3581f07`.
- Week-1 follow-ups: macOS EMSGSIZE at the fd limit is the fd-quota drop (`4bdc628`), a reclaim-test fix (`9eaec21`), and the QUIC evidence steps under the stall watchdog (`4c822b4`).
- Items 10-15, FD passing, on `sprint/fd-passing` (forked from `4bdc628`). They merged as one unit through `sprint/fd-merge`: merge `236550d`, then `0774aff`, `2406b68` and `e416dd3`.

  | Item | Commits |
  |---|---|
  | 10, send path | `182e47e`, `44bd530`, `1f1c3fa` |
  | 11, exact-boundary reads | `5474814`, `73a519c` |
  | 12, Peer seam | `aeb506e`, `327bbdb` |
  | 13, limits and fault injection | `efe3f1f`, `8779565` |
  | 14, security review, guide, example | `23bf853`, `af04ec0`, `fe03765`, `979de11`, `78db26c` |
  | 15, C++ fd e2e (Linux) | `e7158dd`, `375df17` |

- `CHANGELOG.md` `[Unreleased]` records all of it. The release drift hook against v0.19.1, as a minor bump, reports OK with no warnings. No Stable line is removed or changed (`docs/api-snapshot.txt` is identical, and `docs/generated-shape.txt` is new in this cycle). FD passing adds Experimental lines only, so it needs no new Breaking entry.

**The item-14 security review: 6 high findings.** Two defects were each found twice, so the 6 findings name four defects. All four were on main before FD passing: three in week-1 drain mode, and one in `Listener.close`. `979de11` fixes all four, each with a red-first test and an ablation. The merge and its follow-ups extend two of the fixes.

| Defect | High finding | Fix | Threat-table row |
|---|---|---|---|
| MSG_OOB (979de11 #1) | `MSG_OOB` bypasses drain mode on Linux 5.15 and later. A normal `recvmsg` skips the out-of-band byte, and the kernel closes its fds inside the read, on the reader thread, outside the closer and its bound (measured: 3 s for a 3 s linger). | `SO_OOBINLINE` on every drain-mode socket before its first read (`fd_io.setOobInline`). If it cannot be set, every read fails. | 39 |
| Listener.close (979de11 #4) | `Listener.close` closes an AF_UNIX listening socket inline. Its final close disposes of the fds on the backlog's unread messages, so a lingering fd blocks the closing thread (3 s on Linux and macOS). | The final close runs on the closer's `.socket` lane, through a close-on-exec dup. `unix.listen` reserves the slot for it. The merge extends this to every AF_UNIX listener (`236550d`). `e416dd3` starts the closer threads before `listen` reserves the slot. | 40 |
| Socket lane (979de11 #3) | The `.socket` lane has no bound (row 8 was OPEN). Behind one stuck close, a peer that reconnects in a loop fills the fd table. | An accept gate: an AF_UNIX listener takes no connection while the lane holds `socketLaneBound()` jobs, with a `SocketCloseQueueFull` backpressure event. The merge adds the `WorkerPool` workers (`236550d`), and `2406b68` adds `Listener.initFd` on an AF_UNIX socket. | 8 (now Bounded) |
| Read claims (979de11 #2) | The closer's check takes no headroom for the read that follows it, so readers that wake together all pass it (measured without the fix: 16 readers put 4063 fds in a lane bounded at 16). | Read claims (`closer.claimRead`, `endRead`): each read claims 254 fds' worth, granted only while the limit has room, so the lane ends at most one read past its limit. The suggested RLIMIT change became D-B. | 7 |

The review's medium findings added two more fixes:
- A reader parked inside a blocking `recvmsg` skipped the closer's check. The read after `poll` is now non-blocking (`fd_io.tryRecvWithFds`, `979de11`, row 42).
- After a promise-pinned import's Release, a capability that reused the import id got the old capability's fd. An imported fd now closes when its Release goes out, even under a promise pin (`fe03765`, row 43).

The fix work also found row 44: on macOS a close of the other end of a socket whose `shutdown(SHUT_RD)` is stuck waits for it.

**The merge reconciliation (`236550d`).** Main (`20381cf`, item 9) and the branch (`979de11`) fixed the same `Listener.close` defect in two ways. Main's fix covered every AF_UNIX listener (found with `getsockname`), but its hand-off allocated. The branch's fix covered only `unix.listen` listeners, under a slot reserved at `listen`. The merge keeps one implementation, `closeListenSocket` in `src/rpc/transport/tcp/runtime.zig`, with main's reach and the branch's reservation.
- The branch's accept gate became `tcp.runtime.awaitSocketLane`. That is the merge's only new API line.
- `WorkerPool.initListener` accepts with raw syscalls, so it skipped the gate and ignored the listener's `fd_passing`. The merge closes both gaps.
- No test from either side was dropped, and no two tests pinned contradictory behavior.
- Three follow-ups fix what the gates and the merge review found. `0774aff`: the hardening gate flagged an unreviewed `.?` unwrap in `WorkerPool.acceptParked`. `2406b68` (merge review, medium): `Listener.initFd` on an AF_UNIX socket got the off-thread close but not the accept gate; the new Experimental field `Listener.non_ip_socket` selects the gate. `e416dd3` (merge review, two lows): `unix.listen` now starts the closer threads before it reserves its slot, and the docs now say that each pool worker checks the lane on its own (the lane can pass its bound by one teardown per worker, with one event per waiting worker).

**Corrections to this plan:**
- **macOS-to-C++ fd passing is lossy.** Item 15 said "Not on macOS" with a guess ([I]). The first item-15 commit (`e7158dd`) ran the test on macOS anyway, on the claim that kj reads a message's fds exactly. That claim is false. `TwoPartyVatNetwork` reads through kj's `BufferedMessageStream`, which reads in bulk and gives a read's fds to the message that holds the read's last byte. macOS anchors fds to a read's first byte, so kj can give a frame's fds to the next frame and drop them. Measured 16 runs at a time: 5% to 44% of runs failed on macOS, and 0 of 640 on Linux arm64. `375df17` makes the test Linux-only. The guide (`docs/rpc-unix-sockets.md`, "Fd passing") says: do not send fds to a C++ peer on macOS. capnp-zig's sender cannot prevent the loss. The other direction (C++ sends, capnp-zig receives) did not fail. This also settles the Risks line "The C++ macOS fd interop is not run".
- **The quic-zig v0.26.0 bump flips two tests.** Week 1 expected no code change. A trial bump makes two tests see `DisconnectCause.peer_close` where they expect `.handshake_timeout`: "quic Server on_session_accepted rejecting a session closes it and keeps serving" (`rpc_quic_transport_test.zig`) and "QUIC PeerServer on_accept error discards the session's peer, leaks nothing, and the next dial is served" (`rpc_quic_peer_test.zig`). The pin stays at v0.25.0 for v0.20.0. The bump after the sprint must update those two assertions. (Superseded the same day by the owner's option B: see "Week 2, part 2".)
- **quic-zig built our five asks.** Its `sprint/ticket-keys` branch (`fbf7fa1`) implements the five asks in `docs/upstream/handoff-quic-zig-ticket-keys.md`: a ticket-key config field, rotation that keeps the previous key, a ticket-lifetime setting, the `.override` advice fix, and a client that resends 0-RTT after a Retry. Our QUIC suite passed against that branch, except the three F8 flips that the handoff predicts (and the two v0.26.0 flips above). When quic-zig releases it, capnp-zig flips those three assertions and moves `session_ticket_key` onto the new config fields ("When quic-zig ships asks 1-3" in the handoff). (Done: quic-zig released it as v0.27.0, and v0.20.0 moves onto it. See "Week 2, part 2".)

### Week 2, part 2 (2026-10-05): quic-zig v0.27.0, stream-end hardening, RC docs

**Owner decision (2026-10-05): option B.** Move to quic-zig v0.27.0 before the RC, not after the sprint. quic-zig released our five asks as v0.27.0 (tag `9d2ab6e`) on 2026-10-05. The coordinated set (http3-zig, qmsg, nest) is on quic v0.26/v0.27, and a process can hold only one quic module. This decision replaces two corrections above: the pin does not stay at v0.25.0, and capnp-zig does not wait for a later quic-zig release. (Later on 2026-10-05 the owner moved the set on to quic-zig v0.28.0. See "Week 2, part 3".)

**The move is on main and pushed.** `origin/main` is `d86bb92`. CI run 37378767785 on `d86bb92` is green on every job, including the step "Fd passing against the C++ reference" in the reflection-conformance job. So the FD-passing merge now has a green CI run too. Before that, `940abe8` fixed a test that x86_64 ubuntu failed (run 37357958317): Linux reports a bulk read's fds on the whole read, and the test now accepts that.

| Commit | What it did |
|---|---|
| `881a64b` | Pins quic-zig v0.27.0. boringssl-zig (`ff30fe99`, 0.6.7) and the option map do not change. Five assertions flip, each where the handoff predicted: three F8 assertions (the restore now arrives in 0-RTT after a Retry) and two `.handshake_timeout` -> `.peer_close`. |
| `ebcab4a` | `connection_loop.closedForGood`: a client that a server refuses during its handshake ends within a round trip with `.peer_close`. Before, the frames it queued before the handshake (`connect` + Bootstrap) kept it waiting for its own handshake timeout (30 s by default). The pin found this. The two flipped tests had hidden it with short handshake timeouts. Red first; ablated. |
| `9c9c539` | The config field: `serverConfigFromOptions` copies the key into quic-zig's `Server.Config.session_ticket_key`. The bridge (the install on `Server.tls_ctx`, the read-back, `SessionTicketKeyInstallFailed`) and the library's `boringssl` import are gone. A key with Retry on and no `new_token_key` is now allowed: a Retry costs a round trip, not the early restore. |
| `8008684` | Rotation: `Server.rotateSessionTicketKey` (loop-thread check) and `Listener.rotateSessionTicketKey`, with the listener's clock. |
| `3bd3651` | `WarmRedialClient` reads `Connection.retryAccepted()` and counts a dial that rode 0-RTT behind a Retry in `zero_rtt_generations`. |
| `dde9a46` | A native warm restore stages five data frames against a remembered window of two streams, and all five arrive. On v0.26.0 the server closed that connection. |
| `294b13e`, `ab4fb41` | Docs for the config field, rotation and the closed Retry gap. The NEW_TOKEN clock test's witness is now the Retry count. |
| `d86bb92` | Soak gate teeth restored. After `3bd3651`, the `--ticket-key` gate read only the 0-RTT count, so a heal behind a Retry passed: Nightly no longer guarded port reuse (16f) or the restart-safe clock (16e). Now each death needs a heal that rode 0-RTT AND skipped the Retry (`assessZeroRttHeals`). Every Retry a healing client gets must be its first dial or a port fallback (`assessHealRetries`). Ablations, each red: 16f off fails every death (168 unexplained Retries), 16e off gives 104 and 106 unexplained Retries, and `--inject-ticket-key-rotation` gives no 0-RTT heal. |

**The stream-end audit.** quic-zig measured a trap that http3-zig reported: `Connection.tick` frees a stream once its receive half has ended. When the FIN or RESET arrives alone and `tick` runs before the read, the stream is gone, and a clean end looks like a cut one. The audit result:
- capnp-zig's own loops are safe: our client and our server service before they tick. A probe that puts a tick between the feed and the service reproduces the trap in native mode, in both directions.
- A host loop that runs `EmbeddedSession` and ticks first was exposed.
- Native mode reads `Stream.recv.final_size` directly. quic-zig's "repair A" (a stream counts as done only when the application saw its end) would stall native mode. We told quic-zig, and it ruled repair A out.

**The stream-end hardening** is on `sprint/stream-end-hardening` (`e2974e5`, `52a05e1`), under the RC-docs branch. Neither is on main yet.
- A native data frame completes when its stream is gone after every announced byte was read. The length came on the control stream, so nothing is missing.
- The embedded seat keeps a data stream's bytes past `.fin`, `.reset` and a stream-GC `.reaped`, and frees each ended stream once the engine has read it (it kept 9 buffers after 8 large frames).
- A data stream that the peer resets before its bytes are read fails the session at once with `DataStreamReset`, not at the completion deadline.
- Test builds get `rpc.transport.quic.testing.knobs.setTickBeforeService`, which puts the wrong order into the owned loops.
- `docs/quic-transport.md` gains "Embedder rules": feed, service, then tick; and on a shared socket, give each datagram to your own dials first.
- The review found one medium defect in `e2974e5`: on `.reset` the seat ignored the RESET's final size and error code. `52a05e1` reads them from quic-zig, so the seat gives the same results as the owned loops (`InvalidFrame` for a wrong final size, `DataStreamReset` for missing bytes). Each fix was ablated red.

**quic-zig holds four names for us.** The quic-zig session recorded our constraint as a hard one for any stream-end repair. It keeps these names and their meaning: `Server.tls_ctx`, `Connection.retry_accepted`, `boringssl.raw` (for the bridge) and `Stream.recv.final_size` (native mode reads it). It will also send us any change to when its GC reclaims a stream before it writes code. After the v0.27.0 move, the library no longer uses `Server.tls_ctx` or `retry_accepted`, and only one QUIC test root uses `boringssl.raw` (to read a ticket's lifetime). The stream-end hardening adds two reads that quic-zig does not hold yet: `Stream.recv.reset` (its `final_size` and `error_code`) and `Connection.streamRecvWasReaped`. The v0.28.0 move adds a third: `Connection.streamRecvEnd` and its `StreamRecvEnd` (`reset_code`, `final_size`, `stopped`), which the embedded seat and native mode now read.

**The Ubuntu 26 runner migration.** The `ubuntu-latest` jobs of CI run 37378767785 carry this annotation: "The ubuntu-latest label will migrate to Ubuntu 26 beginning October 19, 2026." Most CI and Nightly jobs run on `ubuntu-latest`. The reflection-conformance job (`ubuntu-24.04`) and the Nightly fuzz lane (`ubuntu-24.04-arm`) are pinned. A toolchain or kernel change on the new image can turn a lane red with no change in our code.

### Week 2, part 3 (2026-10-05): quic-zig v0.28.0

**Owner decision (2026-10-05): move to quic-zig v0.28.0 before the v0.20.0 cut.** A process holds one quic module, and the coordinated set will tag on v0.28.0. This replaces the v0.27.0 pin of option B. The release commit on `release/v0.20.0` (`88437e7`, on quic v0.27.0) is not used: the cut is done again after the move.

**The move is on `sprint/quic-v028`, not on main.** `8b914d9` pins v0.28.0 (tag `a9078d8`) and lets native mode settle a freed data stream from quic-zig's note of its end (`Connection.streamRecvEnd`). `8879ced` makes the embedded seat classify each stream end with `streamRecvEnd`. `a924b7f` updates the QUIC guide, the upgrade guide and the CHANGELOG. The review fixes after them (`abf8c68`, then a docs commit): a stream 0 that the host stops now closes the session (the peer answers a stop with RESET_STREAM, and the seat had dropped stream 0 without the session loss); a session whose connection was already closing keeps the cause of that close; and the docs no longer tell hosts to unwrap `streamRecvEnd` with `.?`.

**Blocker: quic-zig v0.28.0 does not compile for a 32-bit target.** A comptime size assert in `src/conn/RecvEndRing.zig` holds only where a `u64` aligns to 8 bytes. CI's cross-targets job runs `-Dquic=true check-compile check-test-compile -Dtarget=x86-linux-gnu` on every push, so it is red on this pin. quic-zig fixed it in `dd570d0` (branch `fix/record-size-32-bit`). On 2026-10-05 no tag carries the fix, and http3-zig pinned v0.28.0 and then went back to v0.27.0 because of it. Do not merge `sprint/quic-v028` to main, and do not cut v0.20.0 on it, until one of these is true:
- quic-zig tags the fix (v0.28.1) and capnp-zig moves its pin to it. Then change every `v0.28.0`, `a9078d8` and `quic-0.28.0-...` hash string (the `build.zig.zon` comment, the CHANGELOG pin entry and intro, the table, snippet and checklist of `docs/upgrading-to-0.20.0.md`, and the pin paragraph and "Current Limits" of `docs/quic-transport.md`), and delete the notes on the 32-bit defect (`build.zig.zon`, CHANGELOG, `docs/upgrading-to-0.20.0.md`, "Current Limits").
- The owner records a decision to accept a red cross-targets x86 job for v0.20.0.

**What remains: item 18, the v0.20.0 release candidate.**
- Done on `sprint/rc-docs`:
  - CHANGELOG `[Unreleased]`: the quic v0.27.0 move (a new Breaking (Experimental behavior) entry, the rotation entry, the bump with its Migration notes), the two deletions (the `SessionTicketKeyInstallFailed` Breaking entry and the "library roots import boringssl" Changed entry), the soak gate, and the stream-end entries.
  - `docs/upgrading-to-0.20.0.md`, linked from the CHANGELOG intro and from README.
  - `docs/supported-surface.md` rows for the Unix transport and FD passing. Both files already had the ticket-key row.
  - `docs/stability.md`: the "RPC fd passing" row now says that `test-rpc-fd-cpp` runs on Linux in CI, and that macOS is left out on purpose.
  - `just check-release-drift v0.19.1 0.20.0`: OK, 0 warnings.
- Merge `sprint/stream-end-hardening` and `sprint/rc-docs` to main, push, and get a green CI run on the RC commit.
- Freeze the RC commit before 2026-10-19 if you can, so that its CI and Nightly evidence is on the image it was tested on. If the RC commit runs on Ubuntu 26, read every red lane before you tag.
- Two green Nightly runs on the RC commit, dispatched against the RC ref. They are the first Nightlies with the Retry-free soak gate.
- `just release-preflight 0.20.0` green, including the drift hook.
- At the cut (the release ceremony, after the version sweep):
  - README: one `unreleased-after: v0.19.1` marker sits on the upgrade-guide line. docs-smoke fails on it after the bump, by design. Delete the sentence that says v0.20.0 is not tagged, and the marker.
  - `docs/upgrading-to-0.20.0.md`: delete the "Release candidate" note at the top, and put the real `capnpc_zig-0.20.0-...` hash in its table. docs-smoke does not scan this file.
  - The downstream quic pins: read the newest tags of http3-zig, qmsg, qmesh-zig and nest again, against the quic release that capnp-zig pins (v0.28.0, or v0.28.1 after the move in "Week 2, part 3"). Update the table in `docs/upgrading-to-0.20.0.md` ("One quic module per process") and the "One quic module per process" Migration note of the quic bump in CHANGELOG. On 2026-10-05, no tag and no `main` branch of http3-zig, qmsg or qmesh-zig pins quic v0.28.0. Their tags pin older releases (`v0.5.1` pins v0.26.0; `v0.7.0` and `0.2.1` pin v0.21.0), and their `main` branches pin v0.27.0 (http3-zig pinned v0.28.0, then went back to v0.27.0 because of the 32-bit defect).
- The owner approves the tag. Then `just release-tag 0.20.0`, `just verify-release-hash 0.20.0`, a real `zig fetch`, and the consumer builds.
- Handoffs (files, no pushes):
  - slcp: the shape gate and the walker seam.
  - http3-zig, qmsg, qmesh-zig, nest: tag a release that pins the same quic release as capnp-zig v0.20.0 (v0.28.0, or v0.28.1 after the move), with the same option map (one quic module per process). The owner moved the set from v0.27.0 to v0.28.0 ("Week 2, part 3"). On 2026-10-05 no tag and no `main` branch of theirs pins v0.28.0.
  - quic-zig: it can release `Server.tls_ctx`, `retry_accepted` and (outside one test root) `boringssl.raw`. Ask it to hold `Stream.recv.reset`, `Connection.streamRecvWasReaped` and `Connection.streamRecvEnd` with its `StreamRecvEnd` (`reset_code`, `final_size`, `stopped`) next to `Stream.recv.final_size`. Ask it to tag the 32-bit fix (`dd570d0`).
  - capnp-qmsg-demo and mruby-quic: the switch fix.
  - prollytree and bucketlist: scratch builds.

## Owner decisions

**Decided by the owner (2026-10-04): D1 (a), D3 (A), D4 (B).** The owner also took the defaults listed below. For the D1 closure list, the default is to carve out each member that names an Experimental type (keep it Experimental). Promoting the named runtime type to Stable is the alternative. The owner can veto per item when the day-1 list is ready.

1. **D1. What tier does a generated-shape line get by default?** Due on day 2, with the day-1 closure list.
   - **(a) Experimental by default, with named Stable families:**
     - Reader/Builder `get`/`set`/`init`/`has`/`clear`/`which`/`wrap`;
     - `WhichTag`, enums and consts (now with their values);
     - `Client` `init`/`release`/`fromBootstrap`/`callX`/`callXPipelined`;
     - `PipelinedClient` calls;
     - `Server`, the `VTable` fields and the `Method` enum;
     - `Response` with `unwrap`;
     - the `Handler`/`Callback`/`BuildFn` typedefs.
   - **(b) Stable by default, with named Experimental features.**
   - **Recommendation: (a).**
     - Freezing becomes a deliberate act (`tools/api_snapshot.zig:41-45`).
     - It keeps generator internals unfrozen, as you ruled.
     - Option (b) would freeze `CallContext`, `callBuild`, `pointer_indexes` and `_reader` by accident.
     - T1, T2 and T5 still land on Stable lines.
     - `supported-surface.md:63` narrows to these families.
   - **With (a), you also resolve the closure list by name.** Each item is either frozen or carved out. Known items today: `capnpSchema` (names `reflection.SchemaRef`), `callXWithOptions` (names `rpc.peer.CallOptions`), the `x_deferred` `VTable` field (names `DeferredHandler` and `ReturnSender`), and `asReader` (names `ReaderStorage`).
2. **D3. Events for Unix sockets and fds.** Due on day 3.
   - **(A)** Add `Source.unix` and `Resource.attached_fds`, and make both enums non-exhaustive now. That adds one Breaking (Experimental) entry with a Migration note. `DisconnectCause` set the precedent (`events.zig:132-155`).
   - **(B)** Change no enum. Unix connections report `.tcp`, and fd drops appear only in a stats getter.
   - **Recommendation: (A).** None of the consumers we checked switches on `Source` or `Resource` [S]. Option B gives wrong telemetry.
3. **D4. Does the drain-mode security fix get a patch release?** Due before item 6 merges.
   - **(A)** Backport item 6 (drain mode and off-thread close) as v0.19.2.
   - **(B)** Ship it in v0.20.0 only, with a `### Security` entry that names who is exposed.
   - **Recommendation: (B).** An attacker must be able to open the app's Unix socket. No known consumer runs AF_UNIX [S]. v0.20.0 is about two weeks away, and a mid-sprint patch costs a full release ceremony. If any consumer reports AF_UNIX use, cut v0.19.2 from item 6.

**Defaults this plan takes (veto any):**
- **D2:** exact-boundary reads when fd passing is on. The rule is the same on both OSes, and the probe re-ran today. It costs about twice the `recvmsg` calls, only on those connections. Bulk reads with a per-OS anchor can come later as an optimization, gated by FD-0.
- FD passing lives on `sprint/fd-passing` until item 14 closes. There is no build flag.
- The process fd budget defaults to `RLIMIT_NOFILE / 4`.
- One process-wide closer thread closes all received fds.
- The import owns a received fd, and the app borrows it through `importFd`. `takeImportFd` can come later without a break.
- `thirdPartyHosted` fds are closed in v1.
- The e2e Unix server uses a `Listener.accept` loop, not WorkerPool.
- POSIX only in v1, with stubs on Windows.
- FD passing launches on Linux and macOS together.
- One static ticket key, installed after init through `tls_ctx`, with no wait for upstream. No key pair this sprint.
- One v0.20.0 RC.
- Abstract socket names are rejected.
- `reclaim_stale` defaults to false.

## Refuted or already done

- **"RPC interface code has zero golden coverage."** Partly wrong. `Persistent`'s generated interface (128 lines) is strictly drift-checked as Experimental lines (`ci.yml:415-419`, `:382-384`). Nothing classifies its changes for a release [V].
- **"Four prefixes are frozen":** there are eight RPC prefixes. They include `rpc.wire.protocol` and `rpc.caps.table`, which cover all of item 12's original edit sites (`tools/api_snapshot.zig:226-354`) [V].
- **"Item 8 can use the existing server, and item 9 can slip freely":** wrong. The e2e server needs a Unix listener, and `WorkerPool.init` takes an IP address only (`main_server.zig:1996-2005`; `worker_pool.zig:112-123`) [V]. Item 8 now uses a `Listener.accept` loop.
- **"A Unix pool would hang on macOS because shutdown does not wake a Unix accept":** imprecise. On macOS, `shutdown` on a listening socket fails with ENOTCONN for TCP too. TCP already depends on the nudge. `close` wakes `accept` [V].
- **"The e2e runner must learn to fail on zero TAP lines":** already done (`tools/e2e_runner.zig:838-841`) [V].
- **"`cmsg.Iterator` brings back the macOS truncation leak":** not with the 512-slot buffer. macOS never truncates it from one send [V]. The ablation needs a small test buffer.
- **"Back at baseline after macOS CTRUNC":** impossible. macOS installs fds whose numbers did not fit [V].
- **"T4 gives a compile failure":** wrong. An emptied inferred error set still compiles [V]. The da60cb6 replay is the compile-failure ablation.
- **"T2 changes exactly 1 line":** only on a one-interface surface. With the corpus, it changes one line per non-generic `Client` [V].
- **"Probe the family in `Connection.init`":** it cannot report a failure (`error{OutOfMemory}`, `api-snapshot.txt:1077`), and slcp skips it [V]. The probe moves to `Transport.initWithOptions`.
- **"Green on the Nightly arm leg":** that leg runs only the fuzzer (`nightly.yml:95-120`) [V].
- **"The OS discards and closes fds the receiver did not ask for"** (`rpc.capnp:1118-1124`): false on macOS [V]. On Linux, the discard can block the receiving thread [V].
- **Audit finding 4, "name-level only":** fixed for the library surface by 43150c9 [S].
- **"Ticket keys need a newer quic pin":** refuted by the proof on v0.25.0 [S].
- **"Unix sockets need a new transport":** refuted. The existing stack runs over AF_UNIX and talks to C++ [S].
- **"Add `fd_passing` to `Connection.Options`":** that struct is a frozen prefix (`api_snapshot.zig:292`) [V].
- **"Keep the fd-framer state in `framing.Framer`":** that module is frozen (`api_snapshot.zig:227`) [V].
- **"Abort the connection on MSG_CTRUNC":** wrong policy. Linux sets CTRUNC at RLIMIT_NOFILE. Drop the fds and keep the connection [S].
- **"Reclaim a stale socket file with a connect probe":** racy. A server between `bind` and `listen` also refuses connects [V].
- **"`.restore_only` must always require `new_token_key`":** that would break existing users. The rule applies only when a key is set and Retry is on.
- **"The ReleaseSafe list is at `build_impl.zig:1514-1516`":** that is the TSan list (`:1512-1523`) [V].
- **"`worker_pool.zig` lives under `transport/tcp/`":** it is `src/rpc/integration/worker_pool.zig` [V].
- **"`tls_context_override` is the hook":** it drops the TLS 1.3 pin, ALPN, the early-data flag, anti-replay and the mTLS-deny hook. It is also refused together with `client_ca_pem` [S].
- **"The ticket key is 80 bytes":** BoringSSL takes 48 [S].
- **"macOS is gated only locally":** wrong. See `ci.yml:91` [V].

## Deferred

| Item | Reason |
|---|---|
| struct_gen split | Next sprint. The shape gate will prove that the split changes no shape |
| Windows AF_UNIX parity | No `SCM_RIGHTS`; 3 AFD quirks; Windows CI headroom |
| Abstract socket namespace | No permission model; std trailing-NUL bug |
| Peer credentials (`SO_PEERCRED`, `getpeereid`) | No std wrapper; v1 documents the 0700 directory |
| Fds through proxies, `thirdPartyHosted` fds, L3 Unix VatNetwork | Size L; a documented parity gap with C++. v1 closes `thirdPartyHosted` fds |
| Generated `Client.fd()` helper | Needs a codegen ABI bump; `client.peer.importFd(client.cap_id)` works today |
| Bulk-read anchor optimization | Only if fd throughput needs it; gated by FD-0 |
| Ticket-key pair or ring (callback form) | BoringSSL's setter takes one 48-byte key. A pair needs the callback form and new crypto glue. Add it if field data asks, or adopt quic-zig's field |
| Adopting quic-zig's ticket-key config field; ticket keys in embedded mode | Done in v0.20.0 ("Week 2, part 2"): the key goes through quic-zig v0.27.0's config, so an embedder that builds its server from `serverConfigFromOptions` gets it too |
| Sturdy-ref proof format | Open question in `quic-durable-caps-plan.md:376` |
| quic v0.26.0 pin; QuicVatNetwork rung 2 | The pin is done: v0.20.0 moves to v0.28.0 (owner's decision, "Week 2, part 3"; first v0.27.0 by option B, "Week 2, part 2"). Rung 2 stays deferred |
| TCP loop off `poll(2)` | Same loop that the `recvmsg` seam extends |
| Module-root restructure; compat/0.18 sweep | Moves Stable paths; path-dependency consumers |
| Devtools package for slcp | Item 1 builds the seam; publish it next sprint |
| Go, Python and Rust over Unix sockets | Their harnesses are TCP-only |
| Downstream canary recipe; fanout re-measure | No overlap; next sprint |
| FreeBSD fd passing | Compiled out |
| Stale worktrees, `wf_*` branches, broken `elated-panini-929a49` | The owner's call |

## Risks

- **Windows headroom.**
  - The gate runs in Hardening, not in Test.
  - New suites still compile on Windows when they skip.
  - The Test "Run tests" step has a 35-minute cap (`ci.yml:148`), tighter than the job's 45. ReleaseSafe (windows) uses 2099 s of 45 minutes [S].
  - Measure each Windows job on the first push. The budget is 2 minutes or less per job. The bigger shape corpus counts against Hardening.
  - Item 15 adds work to the reflection-conformance job (35-minute cap). Measure it too.
- **Kernel fd behaviour is not specified.**
  - FD-0 runs on every POSIX leg.
  - The CI macOS image is not macOS 27, so a difference shows up as red.
  - No CI lane runs tests on aarch64 Linux. A local container run is the only evidence.
- **Blocking close.** A tty, FUSE or NFS fd can hold the closer for a long time. While it is stuck, connections that keep sending fds are closed. This is documented, not solved.
- **`cmsghdr` layout.**
  - musl pads the header (`c.zig:4410`), and no lane covers musl.
  - glibc is covered through TSan.
- **Raw `recvmsg` and the wake-door contract.** `Io.Threaded` panics on EAGAIN. `fd_io` must handle EINTR and EAGAIN itself, and data sockets stay blocking. Item 9's poll-parked acceptors must keep non-blocking fds away from Io.
- **The writer thread owns the dup'd fds.** The close paths must be race-free. TSan runs items 6 and 10.
- **The FD branch drifts from main.** Rebase it every day. A merge conflict in `stream_transport.zig` is likely, because item 6 lands on main first.
- **The shape gate depends on the runtime.**
  - A runtime error-set change turns it red, and the failure message must say so.
  - Rendering may differ between OSes. The day-1 check in item 2 settles this.
- **Signatures can differ between macOS and Linux.** The experimental snapshot is checked on Linux only. Give every new pub fn a named error set, and use `FdHandle`, not `posix.fd_t`.
- **The ticket key goes in through quic's internal `Server.tls_ctx` field.**
  - A rename fails at compile time.
  - A change in meaning is caught by the 16c tests.
  - A TLS-context reload drops the key. The docs say so.
- **A leaked key file exposes sturdy refs.** The key is opt-in only, and the preset never sets it.
- **The C++ macOS fd interop is not run** [I].
- **Dead tests.**
  - Put every new test under `tests/` and register it.
  - Ablate each test once to prove it is collected.
- **Path-dependency consumers break on a red main.** capnp-qmsg-demo is probably broken already [I]. Its handoff goes out on day 1.
- **Other sessions share main.** Edit through the assigned worktree path. Coordinate before you push. Their pushes can reset the Nightly count, so dispatch the RC Nightlies against the RC ref.
- **quic v0.26.0 may land mid-sprint.** Hold the v0.25.0 pin unless a security fix forces a move.
- **Capacity.** This sprint is new OS-level and crypto work. FD passing is likely to move to v0.21.0. Follow the slip order. v0.20.0 is gated on evidence, not on a date.

## Review notes

I re-checked the high-severity findings myself (marked [V] in "State"). Probes ran on macOS and in the local arm64 `e2e-cpp-rpc` container. The rebuilt `fdmerge` probe is in `survey/finalizer/`.

**Applied:**
- Claims H1 / completeness 1 (frozen cap table): confirmed at `tools/api_snapshot.zig:226,230` and `api-snapshot.txt:884,899,965-968`. Item 12 is redesigned around Peer side tables and a post-encode pass, with a red-first `check-api` ablation.
- Claims H2 (item 8 needs item 9): confirmed at `main_server.zig:1996-2005` and `worker_pool.zig:112-123`. I took the second fix: a `Listener.accept` loop in `main_server`.
- Completeness 2 (blocking close): confirmed. I re-ran both Linux linger probes (3054 ms and 3021 ms). Item 6 adds the closer, a red-first test and a Security entry.
- Completeness 3 (FD export gate): confirmed (`mod_base.zig:35-60`; Peer is exported). I chose the branch. A build flag would need comptime stubs and another snapshot variant, and CI already runs on PRs to main.
- Completeness 4 (process fd budget): confirmed `maxfiles 256`. It is now in item 13.
- Completeness 5 (const values): confirmed at `api_snapshot.zig:679`. Added the walker option and T5.
- Completeness 6 (closure violations): confirmed (0 Stable lines for each named type). They now go into D1.
- Claims M1 (macOS CTRUNC and EMFILE): confirmed by re-running `fdprobe` and `emfile`, and the rebuilt `fdmerge`. The original `fdmerge` binary segfaulted; I rebuilt it from the same source.
- Claims M2/M3/M5/M6/M7/M8/M9, completeness 7-8 and 10-25, and the claims "Low" list: applied where confirmed.

**Partly applied:**
- Completeness 8 says "four `brand_*` fixtures". There are five (`ls tests/test_schemas`). I added all five.
- Completeness 9 (ticket-key pair): rejected for this sprint. BoringSSL's setter takes one 48-byte key, so a pair needs the callback form and new crypto glue that needs its own review. A rotation costs one full handshake per client, not an outage. Applied: rotation cadence, a lifetime premise check, and `loadTicketKeyFile`. The pair is in Deferred.
- Completeness 15 (D4): added as an owner decision. But I recommend (B), not (A). The attacker needs access to the app's Unix socket, no known consumer runs AF_UNIX, and v0.20.0 is close.
- Completeness 19 (EMFILE spin): on macOS the retry does end. It returns the data and drops the fds [V]. I kept the cheap guard for a second EMFILE.
- Completeness 25 (`chmod` swap): no fd-based `chmod` exists for a bound socket file [I]. Applied an inode check plus the 0700-directory layout.
- Claims M9 (arm leg): I dropped the criterion and did not add a test step to the arm fuzz job. That lane's settings are load-bearing [S].
- Claims M8: I took fix 1 (expect error-set drift). The da60cb6 replay stays as the compile-failure ablation.

**Rejected:** none outright. Every high-severity finding held when I re-checked it.