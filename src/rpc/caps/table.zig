pub const descriptors = @import("descriptors.zig");
pub const lifecycle = @import("lifecycle.zig");
pub const inbound = @import("inbound.zig");
pub const outbound = @import("outbound.zig");
pub const payload_remap = @import("payload_remap.zig");

pub const max_table_size = lifecycle.max_table_size;

pub const ExportCap = descriptors.ExportCap;
pub const ImportCap = descriptors.ImportCap;
pub const ResolvedCap = descriptors.ResolvedCap;
pub const OwnedPromisedAnswer = descriptors.OwnedPromisedAnswer;

pub const CapTable = lifecycle.CapTable;
pub const InboundCapTable = inbound.InboundCapTable;

pub const OutboundEntry = outbound.OutboundEntry;
pub const OutboundCapTable = outbound.OutboundCapTable;
pub const CapEntryCallback = outbound.CapEntryCallback;
pub const CapEntryRollbackCallback = outbound.CapEntryRollbackCallback;
pub const OutboundCapEffects = outbound.OutboundCapEffects;

pub const resolveCapDescriptor = inbound.resolveCapDescriptor;
pub const resolvePromisedAnswer = descriptors.resolvePromisedAnswer;

pub const commitOutboundCapEffects = outbound.commitOutboundCapEffects;
pub const encodeCallPayloadCapsWithEffects = outbound.encodeCallPayloadCapsWithEffects;
pub const encodeCallPayloadCaps = outbound.encodeCallPayloadCaps;
pub const encodeReturnPayloadCapsWithEffects = outbound.encodeReturnPayloadCapsWithEffects;
pub const encodeReturnPayloadCaps = outbound.encodeReturnPayloadCaps;
