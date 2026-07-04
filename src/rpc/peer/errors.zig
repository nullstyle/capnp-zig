/// Named peer error groups.
///
/// The peer currently exposes inferred error unions from its entry points.
/// These sets centralize the vocabulary used by the implementation and tests
/// so future tranches can narrow APIs without hunting through `peer/mod.zig`.
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
