//! Types shared by fd passing (Experimental): attaching a file descriptor to
//! a capability on an AF_UNIX connection (`CapDescriptor.attachedFd`).
//!
//! This file depends only on `builtin` and the `capnp_build_options` module
//! (the build's `-Dfd-passing`), so the peer layer, the transport binding and
//! the core (socket-free) root can all name the types. The Unix transport
//! (`rpc.transport.unix`) and `rpc.peer` re-export them.
//!
//! Fd passing is compiled in on Linux and macOS only, and only when the build
//! leaves `-Dfd-passing` on (`supported`). Everywhere else `FdHandle` is an
//! empty struct: the signatures that take or return one are the same on
//! every platform, but nothing can be attached.
//!
//! `supported` is the one gate. `fd_closer`, `fd_budget`, `fd_io` and the
//! Unix transport all read it, so they agree by construction.

const builtin = @import("builtin");
const build_options = @import("capnp_build_options");

/// The targets where fd passing can be compiled in: Linux and macOS. Not
/// iOS, tvOS, watchOS, visionOS, Mac Catalyst or DriverKit: no lane tests fd
/// passing there, and on the iOS family std's `Io.Threaded`, which the fd
/// closer uses, does not compile at Zig 0.17.0
/// (`docs/upstream/handoff-zig-fork-ios-nullfile.md`).
pub const target_supported: bool = builtin.target.os.tag == .linux or builtin.target.os.tag == .macos;

/// True where fd passing is compiled in: `target_supported`, and the build
/// option `-Dfd-passing` (default true). With the option off, fd passing,
/// the fd closer threads, the process fd budget and the AF_UNIX transport
/// are compiled out on every target (docs/build-integration.md).
pub const supported: bool = target_supported and build_options.fd_passing;

/// A POSIX file descriptor, wrapped so that the raw platform type never
/// appears in a signature (`posix.fd_t` is a pointer on Windows). Where fd
/// passing is compiled out, `fd` is `void` and the struct is empty.
///
/// An `FdHandle` says nothing about ownership. Each function that takes or
/// returns one says whether it borrows or owns the fd.
pub const FdHandle = struct {
    fd: if (supported) i32 else void,
};

/// The most fds one Cap'n Proto message carries: Linux's `SCM_MAX_FD`, the
/// most one `sendmsg` takes there. `CapDescriptor.attachedFd` is a `UInt8`
/// whose default, 0xff, means "no fd"; this cap keeps every index below it.
pub const max_fds_per_message_cap: u8 = 253;

/// The default for `FdPassing.max_live_imported_fds`, and the limit a `Peer`
/// uses until `Peer.setMaxLiveImportedFds` changes it.
pub const default_max_live_imported_fds: u32 = 64;

/// Fd passing on one AF_UNIX connection (`rpc.transport.unix.ListenOptions`
/// and `ConnectOptions`). Experimental, Linux and macOS.
///
/// One switch for both directions, as in C++ (`rpc-twoparty.c++`,
/// `setFds`): a connection with `max_fds_per_message = 0` neither keeps nor
/// sends fds. Sending needs the switch too because a receiver that did not
/// ask for fds does not reliably drop them: the spec expects it to
/// (`rpc.capnp:1118-1124`), but macOS installs them in its fd table anyway.
pub const FdPassing = struct {
    /// The most fds one inbound message keeps; the extras are closed. 0
    /// (the default) keeps none: every fd a peer attaches is closed (drain
    /// mode), and this side sends none either. Above 0, every export given
    /// an fd with `Peer.setExportFd` also carries it whenever it is sent
    /// (up to `max_fds_per_message_cap` per message, whatever this value).
    /// Values above `max_fds_per_message_cap` (253) count as 253.
    max_fds_per_message: u8 = 0,
    /// The most received fds the connection's `Peer` keeps attached to live
    /// imports at once. A capability that arrives with an fd past this limit
    /// keeps working, but its fd is closed and an event reports it
    /// (`.resource_rejection` on `.attached_fds`, `error.ImportedFdsOverLimit`).
    max_live_imported_fds: u32 = default_max_live_imported_fds,
};
