/*
 * capnp_core.h -- the capnp-swift C ABI (Clang module CapnpCore).
 *
 * A sans-IO Cap'n Proto RPC core: the host (Swift) owns every socket, pushes
 * received bytes in, and pulls outbound frames and events out. The core never
 * touches a socket and never calls into the host except through the panic
 * hook. See docs/native-abi.md in capnp-zig; the design is capnp-swift's
 * docs/plan-2026-10-06.md sections 4 and 4.1.
 *
 * Implemented in Zig (src/native/abi.zig in the capnp-zig package); built
 * into an embedder's static library (capnp-swift: CapnpCore.xcframework).
 * Every function declared here must be exported there with the same shape
 * (enforced by `zig build test` in capnp-zig; docs/native-abi.md).
 */
#ifndef CAPNP_CORE_H
#define CAPNP_CORE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Must equal capnp_core_abi_version(); the host checks it at connect. */
#define CAPNP_CORE_ABI_VERSION 1

/* ---- Version and features ---------------------------------------------- */

/* The ABI version of the linked core (CAPNP_CORE_ABI_VERSION when it built). */
uint32_t capnp_core_abi_version(void);

/* Feature bits. A host checks a bit before it uses what the bit names; a
 * core reports 0 for each bit it does not know. */
/* Plain integer literals, not UINT64_C(): Swift imports a macro that is a
 * plain constant expression, but not one that calls a function-like macro. */
#define CAPNP_FEATURE_ANSWER_FINISHED (1ULL << 0) /* capnp_conn_opts.answer_finished,
                                                     CAPNP_EFFECT_ANSWER_FINISHED and
                                                     capnp_return_canceled */
uint64_t capnp_core_features(void);

/* "core <version> / capnp-zig <pinned version> / <pinned package hash>".
 * Static, NUL-terminated, never freed. */
const char *capnp_core_version(void);
const char *capnp_core_quic_alpn(void);             /* the QUIC baseline ALPN
                                                       capnp-zig freezes
                                                       ("capnp-rpc/1", plan §4) */

/* The name of a Peer observer event tag (capnp_effect.event_tag): "connection",
 * "frame", "backpressure", "resource_rejection", "protocol_error", "close",
 * "timeout", "pressure", "call_latency", "cancel_failure"; "unknown" for a
 * tag this core does not define. Static, NUL-terminated. */
const char *capnp_core_event_name(uint8_t tag);

/* ---- Panic hook -------------------------------------------------------- */

/* Called once when the core panics, then the core executes a trap
 * instruction (the process ends). `msg` is not NUL-terminated and is valid
 * only during the call. The hook must not call back into the core. */
typedef void (*capnp_core_panic_hook)(const char *msg, size_t len);

/* Installs the process-wide panic hook; NULL clears it. Any thread. */
void capnp_core_set_panic_hook(capnp_core_panic_hook hook);

/* ---- Test hook (not part of the supported API) ------------------------- */

/* TEST HOOK ONLY. Executes a trap instruction inside a known Zig frame
 * (src/native/abi.zig in the capnp-zig package) so tooling can prove crash
 * symbolication works. It does not run the panic hook. Never call it from
 * production code. */
__attribute__((noreturn)) void capnp_core_debug_trap(void);

/* TEST HOOK ONLY. Runs a bootstrap + call round trip between two in-process
 * connections inside the core, with the C allocator. Returns 0 on success;
 * otherwise -1 and, when `failure` is not NULL, stores a static,
 * NUL-terminated error name in *failure (NULL on success). */
int32_t capnp_core_debug_selftest(const char **failure);

/* ---- Connection API (C ABI v1) ------------------------------------------
 *
 * One capnp_conn is one RPC connection (a sans-IO capnp-zig Peer). The host
 * owns the socket: it pushes received bytes in (capnp_conn_push_bytes), drives
 * time (capnp_conn_tick), reports the socket's end (capnp_conn_transport_closed)
 * and drains effects (capnp_conn_next_effect / capnp_conn_commit_effect) after
 * every call into the core.
 *
 * Threading: one caller at a time per connection, any thread. The core never
 * calls into the host except through the panic hook. In safe builds a
 * re-entrant or concurrent call on one connection panics.
 *
 * Ownership: every pointer argument is borrowed for the duration of the call.
 * Effect payloads are borrowed until capnp_conn_commit_effect.
 * ------------------------------------------------------------------------ */

/* Return codes. Every int32_t-returning function returns CAPNP_OK (0) or a
 * negative code, except capnp_conn_tick, capnp_conn_take_error and
 * capnp_conn_next_effect, whose non-negative results are documented below. */
#define CAPNP_OK            0
#define CAPNP_E_INVAL      -1   /* NULL where a pointer is required, a bad
                                   struct_size, an unsupported option, flag,
                                   framing, exception type or cap kind */
#define CAPNP_E_BAD_ID     -2   /* an unknown or spent question, import,
                                   export or answer id, or a caps[] index out
                                   of range */
#define CAPNP_E_BUSY       -3   /* capnp_conn_next_effect before the previous
                                   effect was committed; a Finish while the
                                   question is still in flight */
#define CAPNP_E_CLOSED     -4   /* the connection is closed: protocol failure,
                                   remote Abort, or the transport is gone */
#define CAPNP_E_LIMIT      -5   /* a Peer limit refused the operation */
#define CAPNP_E_PROTOCOL   -6   /* push_bytes: the bytes were not a valid
                                   frame or RPC message; the connection is now
                                   closed, an Abort OUT_FRAME and
                                   CLOSE_REQUESTED are queued */
#define CAPNP_E_NOMEM      -7
#define CAPNP_E_INTERNAL   -8   /* any other core failure */

typedef struct capnp_conn capnp_conn;

/* Framing of the byte stream the host pushes in. */
#define CAPNP_FRAMING_SEGMENT_TABLE 0   /* standard stream framing (TCP, Unix,
                                           TLS) */
#define CAPNP_FRAMING_U32_LE        1   /* QUIC baseline: every message one
                                           u32 little-endian length prefix +
                                           its standalone segment-table bytes */

/* Options for capnp_conn_new. Zero every field you do not set; 0 means "the
 * core's default". Set struct_size to sizeof(capnp_conn_opts): a core newer
 * than this header reads only the fields that fit in struct_size. */
typedef struct capnp_conn_opts {
    uint32_t struct_size;
    uint8_t  framing;                   /* CAPNP_FRAMING_SEGMENT_TABLE */
    uint8_t  observer;                  /* nonzero: queue an EVENT effect for
                                           every Peer observer event */
    uint32_t max_frame_bytes;           /* cap on one in-progress inbound frame */
    uint32_t default_call_timeout_ms;   /* deadline of every outbound call;
                                           0 = no deadline */
    uint32_t shutdown_drain_timeout_ms; /* default 5000 */
    uint32_t max_outbound_questions;    /* default 4096 */
    uint32_t max_retained_questions;    /* default 1024; size it to the app's
                                           in-flight calls (every call is
                                           retained until capnp_finish) */
    uint32_t max_active_inbound_questions; /* default 4096 */
    /* Added in M2 (a host built against the M1 header passes a smaller
     * struct_size and gets the defaults): */
    uint32_t max_pending_queued_calls;      /* calls waiting on a promise; default 8192 */
    uint32_t max_pending_queued_call_bytes; /* default 16 MiB */
    uint32_t max_resolved_answers;          /* default 4096 */
    uint32_t max_pending_promises;          /* default 4096 */
    uint32_t max_pending_export_promises;   /* default 4096 */
    uint32_t max_resolved_imports;          /* default 10000 */
    /* Added with CAPNP_FEATURE_ANSWER_FINISHED (a host built against an
     * older header passes a smaller struct_size and gets 0): */
    uint32_t answer_finished;               /* 1: queue CAPNP_EFFECT_ANSWER_FINISHED;
                                               0: off; any other value is
                                               CAPNP_E_INVAL */
} capnp_conn_opts;

/* One capability in a host payload's caps[] table (plan D5), and the target
 * of capnp_call. A capability pointer inside a standalone host message stores
 * an index into that table. */
#define CAPNP_CAP_NONE     0   /* a null capability */
#define CAPNP_CAP_IMPORT   1   /* id = an import: the remote's export, held by
                                  the host until capnp_release */
#define CAPNP_CAP_EXPORT   2   /* id = one of this connection's exports (also
                                  a promise export) */
#define CAPNP_CAP_PROMISED 3   /* a promised answer (pipelining): id = one of
                                  this side's questions that has not returned
                                  yet; ops/nops = the path into its results */
typedef struct capnp_cap {
    uint8_t         kind;   /* CAPNP_CAP_* */
    uint32_t        id;
    const uint16_t *ops;    /* PROMISED: pointer-field indices from the results
                               struct to the capability (NULL/0: the results
                               root is the capability). Borrowed for the call. */
    uint16_t        nops;
} capnp_cap;

/* Effects (plan §4). One is in flight at a time: capnp_conn_next_effect
 * borrows it, capnp_conn_commit_effect releases it. */
#define CAPNP_EFFECT_OUT_FRAME       0  /* msg/msg_len: a frame to send, in order */
#define CAPNP_EFFECT_CLOSE_REQUESTED 1  /* close the transport */
#define CAPNP_EFFECT_RETURN          2  /* id = question id; exactly one per question */
#define CAPNP_EFFECT_INBOUND_CALL    3  /* id = answer id: answer it once with
                                           capnp_return_results or
                                           capnp_return_exception */
#define CAPNP_EFFECT_EXPORT_DROPPED  4  /* id = export_id: the remote released
                                           it to zero (never for the bootstrap) */
#define CAPNP_EFFECT_EVENT           5  /* a Peer observer event (opts.observer):
                                           event_tag names it (capnp_core_event_name),
                                           reason holds its error name or "" */
#define CAPNP_EFFECT_ANSWER_FINISHED 6  /* opts.answer_finished: the caller
                                           finished an INBOUND_CALL that the host
                                           has not answered (the remote sent
                                           Finish or, for a call from this
                                           connection to one of its own exports,
                                           the host canceled that question or its
                                           deadline passed). id = its answer id;
                                           export_id and host_tag as in that
                                           INBOUND_CALL (it can come after that
                                           export's EXPORT_DROPPED: find the
                                           work by id). At most once per call.
                                           Stop the work and answer, normally
                                           with capnp_return_canceled. Stale if
                                           the host answered the call before it
                                           drained this effect: then ignore it.
                                           Apply effects in drain order. */

/* RETURN kinds (return_kind). */
#define CAPNP_RETURN_RESULTS      0  /* msg/caps: the results */
#define CAPNP_RETURN_EXCEPTION    1  /* exception_type (rpc.capnp Exception.Type:
                                        0 failed, 1 overloaded, 2 disconnected,
                                        3 unimplemented), reason */
#define CAPNP_RETURN_CANCELED     2  /* the host called capnp_cancel; exception_type
                                        and reason carry the synthesized exception */
#define CAPNP_RETURN_DISCONNECTED 3  /* this connection ended (transport closed,
                                        remote Abort, teardown); reason */

typedef struct capnp_effect {
    uint32_t struct_size;      /* in: sizeof(capnp_effect); out: bytes filled */
    uint32_t id;               /* RETURN: question id; INBOUND_CALL and
                                  ANSWER_FINISHED: answer id; EXPORT_DROPPED:
                                  export id */
    uint32_t export_id;        /* INBOUND_CALL, EXPORT_DROPPED, ANSWER_FINISHED */
    uint8_t  kind;             /* CAPNP_EFFECT_* */
    uint8_t  return_kind;      /* RETURN: CAPNP_RETURN_* */
    uint8_t  event_tag;        /* EVENT: the capnp-zig events.Event tag */
    uint16_t exception_type;   /* RETURN EXCEPTION / DISCONNECTED */
    uint16_t method_id;        /* INBOUND_CALL */
    uint64_t host_tag;         /* INBOUND_CALL, EXPORT_DROPPED, ANSWER_FINISHED */
    uint64_t interface_id;     /* INBOUND_CALL */
    const uint8_t *msg;        /* OUT_FRAME: the frame. RETURN RESULTS and
                                  INBOUND_CALL: a standalone message whose root
                                  is the results/params struct; its capability
                                  pointers index caps[]. NULL when msg_len is 0. */
    size_t msg_len;
    const capnp_cap *caps;     /* RETURN RESULTS, INBOUND_CALL: IMPORT entries
                                  are owned by the host until capnp_release */
    size_t ncaps;
    const char *reason;        /* RETURN EXCEPTION / DISCONNECTED: the reason
                                  text; EVENT: the error name ("" if none).
                                  Not NUL-terminated. */
    size_t reason_len;
} capnp_effect;

/* Create a connection. now_uptime_ns is the host's monotonic clock now (the
 * same clock every capnp_conn_tick passes); questions sent before the first
 * tick are timed from it. *out is NULL on failure. */
int32_t capnp_conn_new(const capnp_conn_opts *opts, int64_t now_uptime_ns, capnp_conn **out);

/* Free a connection. No effect is produced during free; the host resumes
 * every pending call itself. NULL is a no-op. */
void capnp_conn_free(capnp_conn *conn);

/* Feed transport bytes. Complete frames are dispatched before this returns;
 * drain the effects afterwards. CAPNP_E_PROTOCOL: the stream is invalid and
 * the connection is closed (an Abort OUT_FRAME, then CLOSE_REQUESTED, are
 * queued; capnp_conn_take_error has the cause). CAPNP_E_CLOSED: the connection
 * was already closed, or the remote sent an orderly Abort (take_error reports
 * RemoteAbort with the remote's reason; CLOSE_REQUESTED is queued; every open
 * question ends with RETURN DISCONNECTED once the host reports the transport
 * closed). */
int32_t capnp_conn_push_bytes(capnp_conn *conn, const uint8_t *bytes, size_t len);

/* Advance the clock and run deadlines. Returns the number of questions a
 * deadline ended (each gets a RETURN EXCEPTION), or a negative code. Call it
 * on a fixed interval (100 ms is fine). */
int32_t capnp_conn_tick(capnp_conn *conn, int64_t now_uptime_ns);

/* The host's transport is gone. Every open question ends with one RETURN
 * DISCONNECTED, queued before this returns. Idempotent. After it, calls
 * return CAPNP_E_CLOSED and capnp_finish / capnp_release are no-ops. */
void capnp_conn_transport_closed(capnp_conn *conn);

/* Take the last connection-level error (set by a protocol failure, a remote
 * Abort or a Peer error). Returns 1 and clears it, filling *code (a CAPNP_E_*
 * code), *name and *name_len (the Zig error name, static) and *detail and
 * *detail_len (for RemoteAbort: the remote's reason, valid until
 * capnp_conn_free; else empty); returns 0 and empties the outputs when there
 * is none. Every output pointer may be NULL. */
int32_t capnp_conn_take_error(capnp_conn *conn, int32_t *code, const char **name, size_t *name_len,
                              const char **detail, size_t *detail_len);

/* Borrow the oldest effect into *out (set out->struct_size first; the core
 * fills at most that many bytes and writes the count back). Returns 1 when
 * one was filled, 0 when the queue is empty, CAPNP_E_BUSY when the previous
 * effect was not committed. Payload pointers stay valid until
 * capnp_conn_commit_effect. */
int32_t capnp_conn_next_effect(capnp_conn *conn, capnp_effect *out);

/* Release the in-flight effect and its payload. A commit with nothing in
 * flight is a no-op. */
void capnp_conn_commit_effect(capnp_conn *conn);

/* Ask for the remote bootstrap capability. The RETURN's msg is a message
 * whose root is a capability pointer into caps[] (an IMPORT). The bootstrap
 * question is finished by the core itself: never capnp_finish its qid. */
int32_t capnp_bootstrap(capnp_conn *conn, uint32_t *out_qid);

/* Call method_id of interface_id on target: an IMPORT, or a PROMISED answer
 * (pipelining: the call goes out before that question returns, and costs no
 * extra round trip). msg is a standalone message whose root is the params
 * struct; its capability pointers index caps[0..ncaps) (NONE, IMPORT, EXPORT
 * or PROMISED entries). flags must be 0 (streaming lands later). The question
 * is retained: after its RETURN the host must capnp_finish it (or
 * capnp_cancel it before). CAPNP_E_BAD_ID for a PROMISED id that is not an
 * open question. */
int32_t capnp_call(capnp_conn *conn, capnp_cap target, uint64_t interface_id, uint16_t method_id,
                   const uint8_t *msg, size_t msg_len, const capnp_cap *caps, size_t ncaps,
                   uint32_t flags, uint32_t *out_qid);

/* Cancel an open question: the core sends Finish and ends it at once with
 * one RETURN CANCELED (a late Return from the remote is absorbed). After it
 * the question is gone: do not capnp_finish it. CAPNP_E_BAD_ID for a question
 * that already returned. A no-op once the connection is closed. */
int32_t capnp_cancel(capnp_conn *conn, uint32_t qid);

/* Set or replace the deadline of an open question, in ms from now. It fires
 * on a later capnp_conn_tick as RETURN EXCEPTION (overloaded). */
int32_t capnp_set_deadline(capnp_conn *conn, uint32_t qid, uint32_t timeout_ms);

/* Begin a graceful shutdown: new calls fail with CAPNP_E_CLOSED, while input
 * and ticks keep flowing so open questions can return. When none is left, or
 * when shutdown_drain_timeout_ms passes on a tick (the rest end with RETURN
 * DISCONNECTED), the core queues CLOSE_REQUESTED. Idempotent. */
void capnp_conn_shutdown(capnp_conn *conn);

/* Finish a returned question (the host dropped its last handle on it).
 * release_result_caps nonzero also releases the imports its results carried
 * (otherwise release them with capnp_release). CAPNP_E_BAD_ID for a question
 * the core already ended (deadline, cancel) or never knew; CAPNP_E_BUSY while
 * it is still in flight. A no-op once the connection is closed. */
int32_t capnp_finish(capnp_conn *conn, uint32_t qid, int32_t release_result_caps);

/* Release count wire references the host holds on import_id. CAPNP_E_BAD_ID
 * when the host holds fewer. A no-op once the connection is closed. */
int32_t capnp_release(capnp_conn *conn, uint32_t import_id, uint32_t count);

/* Export a host object. Calls on it arrive as INBOUND_CALL carrying host_tag;
 * EXPORT_DROPPED fires once when the remote has released it. */
int32_t capnp_export(capnp_conn *conn, uint64_t host_tag, uint32_t *out_export_id);

/* Make host_tag this connection's bootstrap object. Once per connection; it
 * lives until capnp_conn_free and never produces EXPORT_DROPPED. */
int32_t capnp_set_bootstrap(capnp_conn *conn, uint64_t host_tag, uint32_t *out_export_id);

/* Export a promise: a capability the host resolves later with
 * capnp_resolve_promise or capnp_reject_promise. Pass it in a payload as
 * {CAPNP_CAP_EXPORT, id}; calls on it queue in the core until it resolves.
 * It carries no host tag and never produces EXPORT_DROPPED. */
int32_t capnp_promise_export(capnp_conn *conn, uint32_t *out_promise_id);

/* Resolve promise export promise_id to `to`: one of this connection's EXPORTs,
 * or an IMPORT it holds. Once per promise (CAPNP_E_INVAL afterwards).
 * CAPNP_E_INVAL, with nothing sent, for the promise itself or a promise
 * export that resolved back to it: the promise stays unresolved. A no-op
 * once the connection is closed. */
int32_t capnp_resolve_promise(capnp_conn *conn, uint32_t promise_id, capnp_cap to);

/* Reject promise export promise_id: callers see an exception (type failed; a
 * typed rejection needs capnp-zig handoff H5). reason is not NUL-terminated. */
int32_t capnp_reject_promise(capnp_conn *conn, uint32_t promise_id, const char *reason, size_t reason_len);

/* Answer an INBOUND_CALL (answer_id = its id) with results: msg and caps as
 * for capnp_call. CAPNP_E_BAD_ID for an answer the host does not owe. An
 * answer after the remote's Finish of the call is still accepted, once. On a
 * refused payload (a bad caps[] entry) or a failed send nothing is sent and
 * the answer stays owed. The host answers each INBOUND_CALL once: after that
 * the remote may reuse the id at once, and a second answer then goes to the
 * new call. */
int32_t capnp_return_results(capnp_conn *conn, uint32_t answer_id, const uint8_t *msg, size_t msg_len,
                             const capnp_cap *caps, size_t ncaps);

/* Answer an INBOUND_CALL with an exception (exception_type: 0 failed,
 * 1 overloaded, 2 disconnected, 3 unimplemented; reason is not
 * NUL-terminated and may be empty). CAPNP_E_BAD_ID for an answer the host
 * does not owe. An answer after the remote's Finish of the call is still
 * accepted, once. On a failed send (for example CAPNP_E_NOMEM) nothing is
 * sent and the answer stays owed: answer it again. */
int32_t capnp_return_exception(capnp_conn *conn, uint32_t answer_id, uint16_t exception_type,
                               const char *reason, size_t reason_len);

/* Answer with Return{canceled} (CAPNP_FEATURE_ANSWER_FINISHED) an INBOUND_CALL
 * for which a CAPNP_EFFECT_ANSWER_FINISHED came (its caller finished it; see
 * that effect). CAPNP_E_BAD_ID for an answer the host does not owe;
 * CAPNP_E_INVAL unless a CAPNP_EFFECT_ANSWER_FINISHED was queued for it
 * (opts.answer_finished off, no Finish yet, a tail-call hold withholds it,
 * or the core did not report the Finish: its records were full or out of
 * memory, or the call was still queued in the core when the Finish came). On
 * another error nothing is sent and the answer stays owed. Instead of this,
 * the host may answer with capnp_return_results or capnp_return_exception;
 * it answers once. */
int32_t capnp_return_canceled(capnp_conn *conn, uint32_t answer_id);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* CAPNP_CORE_H */
