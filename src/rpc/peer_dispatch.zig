const protocol = @import("protocol.zig");

pub const InboundRoute = enum {
    unimplemented,
    abort,
    bootstrap,
    call,
    return_,
    finish,
    release,
    resolve,
    disembargo,
    provide,
    accept,
    join,
    third_party_answer,
    unknown,
};

pub fn route(tag: protocol.MessageTag) InboundRoute {
    return switch (tag) {
        .unimplemented => .unimplemented,
        .abort => .abort,
        .bootstrap => .bootstrap,
        .call => .call,
        .return_ => .return_,
        .finish => .finish,
        .release => .release,
        .resolve => .resolve,
        .disembargo => .disembargo,
        .provide => .provide,
        .accept => .accept,
        .join => .join,
        .third_party_answer => .third_party_answer,
        else => .unknown,
    };
}
