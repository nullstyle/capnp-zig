# DRAFT upstream note (nullstyle/quic-zig) — NOT FILED

**Finding:** a `Connection` whose handshake never completes and whose peer
goes quiet never dies: the idle-timeout machinery does not cover the
pre-handshake phase, and there is no handshake timer. capnp-zig measured
the consequence on the server side of its fanout wrapper: half-open slots
are immortal, accumulate under churn/loss (or an attacker sending bare
Initials), pin `max_concurrent_connections`, and the endpoint silently
refuses every new connection — the QUIC analog of a SYN flood. On the
client side, a dial whose Initials are all silently dropped waits forever
(observed: exactly the Initial retransmission budget sent, zero bytes
received, alive 600+ seconds).

capnp-zig has embedder-level guards since 2026-08-27 (`handshake_timeout_ms`
on both its Client- and ServerOptions, sweeping/aborting with a certified
cause). A `Connection`-level handshake deadline upstream would protect
every embedder: RFC 9000 §10.1 permits closing at any time, and most
implementations abort handshakes that exceed a bound (commonly tied to
the idle timeout, applied from the first flight).

Repro sketch: connect a client, deliver its Initial to a server
connection, then drop all further packets in both directions and step
both endpoints — neither ever leaves `.open`.
