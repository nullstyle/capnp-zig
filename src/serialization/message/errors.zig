//! Named error sets for the builder surface.
//!
//! These live in their own file because the builder modules are instantiated
//! through `define()` functions that cannot import `message.zig` itself. The
//! sets are re-exported from `message.zig` as `message.BuildError` and
//! `message.CopyError`, and generated `Builder` mutators spell them in their
//! signatures, so they are part of the frozen contract: adding a name is a
//! breaking change for consumers that switch exhaustively over the set.

/// Every error a builder primitive can return while writing into a
/// `MessageBuilder`: allocation, bounds checks, and wire-encoding limits.
///
/// Generated mutators that can allocate or write a pointer return exactly this
/// set: `initX` of a struct or list field, the Text/Data `setX` and the
/// capability setters. Scalar `setX`, `clearX`, `setXNull` and the `initX` of
/// an AnyPointer, AnyStruct, AnyList or interface field (which only returns a
/// handle to the slot) never allocate and keep their narrower inferred sets.
///
/// The pointer makers that `StructBuilder`, `PointerListBuilder` and
/// `AnyPointerBuilder` call through are typed with it, so a builder path that
/// starts returning a new error fails to compile instead of silently widening
/// to `anyerror`.
pub const BuildError = error{
    OutOfMemory,
    TooManySegments,
    InvalidSegmentId,
    OutOfBounds,
    IndexOutOfBounds,
    PointerIndexOutOfBounds,
    OffsetOutOfRange,
    FarPointerOffsetTooLarge,
    InvalidPointer,
    ElementCountTooLarge,
    ListTooLarge,
    TextTooLong,
};

/// `BuildError` plus the errors raised while reading the source of a deep
/// copy: resolving and bounds-checking the source message's pointers, the
/// recursion bound, and the scratch copy that the generated copy setters
/// build before publishing a pointer.
///
/// `cloneAnyPointer` and the generated `setX` methods that copy from a Reader
/// (`setX(value: T.Reader)`, `setX(value: SomeListReader)` and
/// `setX(value: message.AnyPointerReader)`) return exactly this set.
pub const CopyError = BuildError || error{
    InvalidFarPointer,
    InvalidInlineCompositePointer,
    PointerDepthLimit,
    RecursionLimitExceeded,
    InvalidRootPointer,
    TruncatedMessage,
    EmptyMessage,
    InvalidMessageSize,
    StructSizeMismatch,
};
