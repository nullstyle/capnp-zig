//! Types shared by fd passing (Experimental): attaching a file descriptor to
//! a capability on an AF_UNIX connection (`CapDescriptor.attachedFd`).
//!
//! This file has no dependencies beyond `builtin`, so the peer layer, the
//! transport binding and the core (socket-free) root can all name the types.
//! The Unix transport (`rpc.transport.unix`) and `rpc.peer` re-export them.
//!
//! Fd passing is compiled in on Linux and Darwin only (`supported`). On every
//! other target `FdHandle` is an empty struct: the signatures that take or
//! return one are the same on every platform, but nothing can be attached.

const builtin = @import("builtin");

/// True where fd passing is compiled in: Linux and Darwin.
pub const supported: bool = builtin.target.os.tag == .linux or builtin.target.os.tag.isDarwin();

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
/// Sending needs no option: on an AF_UNIX connection, every export given an
/// fd with `Peer.setExportFd` carries it whenever it is sent. These options
/// govern what this side accepts.
pub const FdPassing = struct {
    /// The most fds one inbound message keeps; the extras are closed. 0
    /// (the default) keeps none: every fd a peer attaches is closed (drain
    /// mode). Values above `max_fds_per_message_cap` (253) count as 253.
    max_fds_per_message: u8 = 0,
    /// The most received fds the connection's `Peer` keeps attached to live
    /// imports at once. A capability that arrives with an fd past this limit
    /// keeps working, but its fd is closed and an event reports it
    /// (`.resource_rejection` on `.attached_fds`, `error.ImportedFdsOverLimit`).
    max_live_imported_fds: u32 = default_max_live_imported_fds,
};
