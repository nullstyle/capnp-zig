const std = @import("std");

/// Capability-pointer helpers used when an RPC payload needs to be viewed and
/// rewritten in-place. The implementation lives under serialization because it
/// depends on Cap'n Proto pointer encoding details, not RPC protocol state.
pub fn define(comptime message: type) type {
    return struct {
        /// Maximum recursion depth for pointer traversal to prevent stack overflow
        /// from deeply nested or cyclic structures.
        pub const max_traversal_depth: u32 = 64;

        pub const MessageView = struct {
            msg: message.Message,
            segments: []const []const u8,
        };

        /// Encode a capability index into a Cap'n Proto capability pointer word.
        ///
        /// Cap'n Proto capability pointers have tag bits 0b11 in the lowest two
        /// bits, with the capability index in the upper 32 bits.
        pub fn makeCapabilityPointer(cap_id: u32) u64 {
            return 3 | (@as(u64, cap_id) << 32);
        }

        /// Decode a Cap'n Proto capability pointer word into the capability index.
        ///
        /// Returns an error if the pointer word does not have the correct tag bits
        /// (0b11) or has non-zero reserved bits.
        pub fn decodeCapabilityPointer(pointer_word: u64) !u32 {
            if ((pointer_word & 0x3) != 3) return error.InvalidPointer;
            if (((pointer_word >> 2) & 0x3FFFFFFF) != 0) return error.InvalidPointer;
            return @as(u32, @truncate(pointer_word >> 32));
        }

        /// A capability pointer decoded together with an optional origin code.
        ///
        /// `origin_code` is null for a plain (app-authored or on-the-wire)
        /// capability pointer and non-null for an in-builder intermediate pointer
        /// that carries its id-space origin (see `makeOriginTaggedCapabilityPointer`).
        pub const CapabilityPointerInfo = struct {
            origin_code: ?u4,
            cap_id: u32,
        };

        /// Encode an ORIGIN-TAGGED capability pointer word.
        ///
        /// This is an intermediate, in-builder-only representation: the caller's
        /// opaque `origin_code` rides in the otherwise-reserved bits (2..6) so a
        /// later encode pass can recover the capability's true id-space instead of
        /// re-deriving it from the bare id (which is ambiguous when the local
        /// export and remote import id spaces collide). It is ALWAYS rewritten to
        /// a real cap-table index before serialization, so it never reaches the
        /// wire. Bit 2 is the "origin present" flag; bits 3..6 hold the code.
        pub fn makeOriginTaggedCapabilityPointer(origin_code: u4, cap_id: u32) u64 {
            return 3 | (@as(u64, 1) << 2) | (@as(u64, origin_code) << 3) | (@as(u64, cap_id) << 32);
        }

        /// Decode a capability pointer that may or may not carry an origin tag.
        ///
        /// A word with all reserved bits (2..31) clear is a plain capability
        /// pointer (`origin_code == null`). A word with bit 2 set carries a
        /// 4-bit origin code in bits 3..6; any other reserved bits must be clear.
        pub fn decodeCapabilityWithOrigin(pointer_word: u64) !CapabilityPointerInfo {
            if ((pointer_word & 0x3) != 3) return error.InvalidPointer;
            const reserved: u30 = @truncate(pointer_word >> 2);
            const cap_id: u32 = @truncate(pointer_word >> 32);
            if (reserved == 0) return .{ .origin_code = null, .cap_id = cap_id };
            if ((reserved & 0x1) == 0) return error.InvalidPointer;
            if ((reserved >> 5) != 0) return error.InvalidPointer;
            const origin_code: u4 = @truncate(reserved >> 1);
            return .{ .origin_code = origin_code, .cap_id = cap_id };
        }

        /// Build a read-only `Message` view over a `MessageBuilder`'s segments.
        ///
        /// The returned `segments` slice must be freed by the caller. The `msg`
        /// does not own the segment data (segments_owned = false).
        pub fn buildMessageView(
            allocator: std.mem.Allocator,
            builder: *message.MessageBuilder,
        ) !MessageView {
            const segment_count = builder.segments.items.len;
            const segments = try allocator.alloc([]const u8, segment_count);
            errdefer allocator.free(segments);

            for (builder.segments.items, 0..) |segment, idx| {
                segments[idx] = segment.items;
            }

            const msg = message.Message{
                .allocator = allocator,
                .segments = segments,
                .segments_owned = false,
                .backing_data = null,
            };
            return .{ .msg = msg, .segments = segments };
        }

        /// Write a raw 64-bit pointer word into a builder segment at the given position.
        pub fn writePointerWord(
            builder: *message.MessageBuilder,
            segment_id: u32,
            pointer_pos: usize,
            word: u64,
        ) !void {
            if (segment_id >= builder.segments.items.len) return error.InvalidSegmentId;
            var segment = &builder.segments.items[segment_id];
            if (pointer_pos + 8 > segment.items.len) return error.OutOfBounds;
            std.mem.writeInt(u64, segment.items[pointer_pos..][0..8], word, .little);
        }
    };
}
