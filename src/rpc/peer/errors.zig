/// Named peer error groups.
///
/// The peer currently exposes inferred error unions from its entry points.
/// These sets centralize the vocabulary used by the implementation and tests
/// so future tranches can narrow APIs without hunting through `peer/mod.zig`.
///
/// ## F1 (v0.3.0 API freeze) outcome — leaked-error narrowing on the Stable
/// two-party-core surface
///
/// Of the seven F1 targets, only the two transport entry points admit an
/// honest narrowing; those sets live with the transport type they belong to
/// (`connection.Connection.InitError` = `error{OutOfMemory}`,
/// `connection.Connection.EnableWakeError` = `error{WakePipeCreateFailed}`)
/// rather than here, to avoid a peer→transport layering inversion.
///
/// The five peer-side targets — `Peer.releaseImport` and the four `sendCall*`
/// entry points — are intentionally LEFT OPEN (`anyerror`). Each synchronously
/// invokes arbitrary user code whose `anyerror` propagates out: `releaseImport`
/// funnels its Release through an installed `SendFrameOverride`, and every
/// `sendCall*` invokes the caller's `CallBuildFn` (`try build_fn(ctx, &call)`).
/// Narrowing either below `anyerror` would require changing a user-callback
/// contract, which the freeze must not do — so they stay open for the same
/// reason as the `CallBuildFn`/`QuestionCallback`/`CallHandler`/`SaveHandler`/
/// `RestoreHandler` typedefs. See the per-function doc comments in `mod.zig`.
pub const Limits = error{
    PeerLimitExceeded,
    QuestionIdExhausted,
    EmbargoIdExhausted,
    RefCountOverflow,
};

pub const Lifecycle = error{
    PeerShuttingDown,
    TransportNotAttached,
};

pub const Routing = error{
    CapabilityUnavailable,
    DuplicateQuestionId,
    EchoedDisembargoUnimplemented,
    MissingCallTarget,
    PromisedAnswerMissing,
    UnexpectedForwardedTailReturn,
    UnexpectedMessage,
    UnknownDisembargoTarget,
    UnknownExport,
    UnknownQuestion,
};

pub const Promise = error{
    DuplicateResolve,
    ExportIsNotPromise,
    PromiseAlreadyResolved,
    PromiseBroken,
    PromiseUnresolved,
};

pub const Release = error{
    ReleaseCountExceeded,
};

pub const ProvideAcceptJoin = error{
    DuplicateJoinQuestionId,
    DuplicateProvideQuestionId,
    DuplicateProvideRecipient,
    InvalidJoinKeyPart,
    MissingJoinKeyPart,
};

pub const ThirdParty = error{
    ConflictingThirdPartyAnswer,
    DuplicateThirdPartyAnswerId,
    DuplicateThirdPartyAwait,
    DuplicateThirdPartyReturn,
    InvalidThirdPartyAnswerId,
    /// `sendReturnResults` was called for an answer whose caller redirected the
    /// results to a third vat. The runtime cannot deliver them there, so it
    /// refuses rather than silently discarding them.
    ThirdPartyResultsNotRedirected,
    /// `sendReturnResultsSentElsewhere` was called for an answer whose caller
    /// did not redirect its results, which would leave that caller with no
    /// results at all.
    ResultsNotRedirected,
};

pub const Remote = error{
    RemoteAbort,
};

/// Typed failure surface of generated Response.unwrap().
pub const CallError = error{
    /// Remote answered with an exception (reason available on the union arm).
    RemoteException,
    /// Locally synthesized: transport closed / peer shut down / peer deinit.
    Disconnected,
    /// Locally synthesized by deadline expiry (checkDeadlines).
    CallTimedOut,
    /// Remote confirmed our cancellation (Return.canceled).
    Canceled,
    /// Rare Return arms the plain-call path never initiates
    /// (resultsSentElsewhere / takeFromOtherQuestion / acceptFromThirdParty).
    UnexpectedReturn,
};

pub const PeerError = Limits ||
    Lifecycle ||
    Routing ||
    Promise ||
    Release ||
    ProvideAcceptJoin ||
    ThirdParty ||
    Remote;
