//! Interface / RPC client emission for the code generator.
//!
//! Extracted from generator.zig as the C1 tranche: ~840 contiguous lines, the
//! largest single cluster in that file. It emits the generated `Client`,
//! `Server`, `VTable`, per-method structs, streaming clients and pipelined
//! call machinery for a Cap'n Proto interface node.
//!
//! Shape follows the `rpc.peer` decomposition: a comptime-generic namespace
//! over the Generator type, holding free functions that take `self: *G`, with
//! generator.zig keeping thin thunks so every call site and the emitted output
//! are unchanged. The Generator type itself does NOT move — the API snapshot
//! renders literal declaration paths, and `codegen.Generator.*` entry points
//! are still frozen.
//!
//! This extraction was blocked until 2026-08-13. Zig's privacy is file-scoped,
//! so lifting these bodies forces the helpers they call back into to become
//! `pub` — and a blanket `p("capnpc-zig.codegen")` rule would have frozen all
//! eleven permanently. Narrowing that rule to the reviewed entry points is
//! what made this possible; the widened helpers are Experimental.

const std = @import("std");
const schema = @import("../serialization/schema.zig");
const types = @import("types.zig");

pub fn Interface(comptime G: type) type {
    return struct {
        const Self = @This();

        pub fn generateInterface(self: *G, node: *const schema.Node, writer: anytype, children: ?[]const u8, self_qualify: bool) !void {
            const interface_info = node.interface_node orelse return error.InvalidInterfaceNode;
            const decl_name = try self.allocTypeDeclName(node);
            defer self.allocator.free(decl_name);

            const ancestors = try self.collectAncestors(node);
            defer self.freeAncestors(ancestors);
            const has_ancestors = ancestors.len > 0;

            // When this interface is nested inside another interface, its own
            // Client/Server/VTable/Method/BootstrapResponse/… decls (and per-method
            // structs) are shadowed by the enclosing interface's same-named decls, so
            // every self-reference to one of them is ambiguous. Qualify them with the
            // interface's own decl name (`Handle.Client`), which is unambiguous from
            // anywhere inside the body — mirroring the nested-struct `<Name>.WhichTag`
            // scheme. A file-scoped interface uses an empty prefix, so its output is
            // byte-identical to the pre-nesting generator.
            const qual = if (self_qualify)
                try std.fmt.allocPrint(self.allocator, "{s}.", .{decl_name})
            else
                try self.allocator.dupe(u8, "");
            defer self.allocator.free(qual);

            try writer.print("pub const {s} = struct {{\n", .{decl_name});
            try writer.print("    pub const interface_id: u64 = 0x{x};\n", .{node.id});
            // Zero-method interfaces produce an empty enum; this is valid Zig but uninhabitable.
            try writer.writeAll("    pub const Method = enum(u16) {\n");
            if (interface_info.methods.len == 0) {
                try writer.writeAll("        _,\n");
            }
            // The methods list is ordered by ordinal, not declaration order (code_order).
            // The wire ordinal IS the list index, so use it directly.
            for (interface_info.methods, 0..) |method, ordinal| {
                const zig_name = try self.toZigIdentifier(method.name);
                defer self.allocator.free(zig_name);
                const escaped_name = try types.escapeZigKeyword(self.allocator, zig_name);
                defer self.allocator.free(escaped_name);
                try writer.print("        {s} = {},\n", .{ escaped_name, ordinal });
            }
            try writer.writeAll("    };\n\n");

            for (interface_info.methods, 0..) |method, ordinal| {
                try Self.generateMethodStruct(self, node, method, ordinal, qual, writer);
            }

            // --- Client ---
            try writer.writeAll("    pub const Client = struct {\n");
            try writer.writeAll("        peer: *rpc.peer.Peer,\n");
            try writer.writeAll("        cap_id: u32,\n\n");
            try writer.print("        pub fn init(peer: *rpc.peer.Peer, cap_id: u32) {s}Client {{\n", .{qual});
            try writer.writeAll("            return .{ .peer = peer, .cap_id = cap_id };\n");
            try writer.writeAll("        }\n\n");
            try writer.writeAll("        /// Release the import ref this Client owns (balances the bootstrap-return\n");
            try writer.writeAll("        /// retainCapability). Call at most once per owned Client; best-effort —\n");
            try writer.writeAll("        /// peer teardown's import release is the backstop.\n");
            try writer.print("        pub fn release(self: {s}Client) void {{\n", .{qual});
            try writer.writeAll("            self.peer.releaseImport(self.cap_id, 1) catch {};\n");
            try writer.writeAll("        }\n\n");

            // Own call methods
            for (interface_info.methods) |method| {
                try Self.generateClientCallMethod(self, method, "interface_id", null, qual, writer);
            }
            // Inherited call methods
            for (ancestors) |ancestor| {
                for (ancestor.methods) |method| {
                    try Self.generateClientCallMethod(self, method, null, ancestor.name, qual, writer);
                }
            }

            // Generate callXxxPipelined methods for own methods with interface-typed results
            for (interface_info.methods) |method| {
                try Self.generateClientPipelinedMethod(self, method, null, qual, writer);
            }
            // Inherited pipelined call methods
            for (ancestors) |ancestor| {
                for (ancestor.methods) |method| {
                    try Self.generateClientPipelinedMethod(self, method, ancestor.name, qual, writer);
                }
            }

            try writer.print("        pub fn fromBootstrap(peer: *rpc.peer.Peer, user_ctx: *anyopaque, callback: {s}BootstrapCallback) !u32 {{\n", .{qual});
            try writer.print("            return {s}bootstrap(peer, user_ctx, callback);\n", .{qual});
            try writer.writeAll("        }\n\n");

            try writer.writeAll("    };\n\n");

            // --- StreamClient (only when interface or ancestors have streaming methods) ---
            if (self.hasStreamingMethods(node, ancestors)) {
                try Self.generateStreamClient(self, node, interface_info, ancestors, qual, writer);
            }

            // Generate Pipeline types for methods with interface-typed results (own methods)
            for (interface_info.methods) |method| {
                try Self.generatePipelineType(self, method, null, writer);
            }
            // Inherited pipeline types
            for (ancestors) |ancestor| {
                for (ancestor.methods) |method| {
                    try Self.generatePipelineType(self, method, ancestor.name, writer);
                }
            }

            // --- PipelinedClient ---
            try writer.writeAll("    pub const PipelinedClient = struct {\n");
            try writer.writeAll("        peer: *rpc.peer.Peer,\n");
            try writer.writeAll("        question_id: u32,\n");
            try writer.writeAll("        pointer_index: u16,\n\n");

            // Own pipelined call methods
            for (interface_info.methods) |method| {
                try Self.generatePipelinedClientCallMethod(self, method, "interface_id", null, qual, writer);
            }
            // Inherited pipelined call methods
            for (ancestors) |ancestor| {
                for (ancestor.methods) |method| {
                    try Self.generatePipelinedClientCallMethod(self, method, null, ancestor.name, qual, writer);
                }
            }

            try writer.writeAll("    };\n\n");

            // --- Bootstrap ---
            try writer.writeAll("    pub const BootstrapResponse = union(enum) {\n");
            try writer.print("        client: {s}Client,\n", .{qual});
            try writer.writeAll("        exception: rpc.wire.protocol.Exception,\n");
            try writer.writeAll("        canceled,\n");
            try writer.writeAll("        results_sent_elsewhere,\n");
            try writer.writeAll("        take_from_other_question: u32,\n");
            try writer.writeAll("        accept_from_third_party,\n\n");
            try writer.writeAll("        /// Collapse this BootstrapResponse into its Client or a typed\n");
            try writer.writeAll("        /// rpc.peer.CallError. Classification comes from the exception's\n");
            try writer.writeAll("        /// spec `Exception.Type`, so a disconnect reported by ANY\n");
            try writer.writeAll("        /// implementation is recognized; every other exception is\n");
            try writer.writeAll("        /// RemoteException (reason available on the union arm).\n");
            try writer.print("        pub fn unwrap(self: {s}BootstrapResponse) rpc.peer.CallError!{s}Client {{\n", .{ qual, qual });
            try writer.writeAll("            return switch (self) {\n");
            try writer.writeAll("                .client => |c| c,\n");
            try writer.writeAll("                .exception => |ex| switch (ex.kind()) {\n");
            try writer.writeAll("                    .disconnected => error.Disconnected,\n");
            try writer.writeAll("                    // The spec has no distinct timeout type and classes\n");
            try writer.writeAll("                    // timeouts as overloaded, so our own deadline sentinel\n");
            try writer.writeAll("                    // still separates a local timeout from a remote's\n");
            try writer.writeAll("                    // genuine backpressure.\n");
            try writer.writeAll("                    .overloaded => if (std.mem.eql(u8, ex.reason, rpc.peer.deadline_reason))\n");
            try writer.writeAll("                        error.CallTimedOut\n");
            try writer.writeAll("                    else\n");
            try writer.writeAll("                        error.RemoteException,\n");
            try writer.writeAll("                    // .failed, .unimplemented (spec: treat like failed), and\n");
            try writer.writeAll("                    // any future code. `else` is required: the enum is\n");
            try writer.writeAll("                    // non-exhaustive because the wire field is remote-controlled.\n");
            try writer.writeAll("                    else => error.RemoteException,\n");
            try writer.writeAll("                },\n");
            try writer.writeAll("                .canceled => error.Canceled,\n");
            try writer.writeAll("                .results_sent_elsewhere, .take_from_other_question, .accept_from_third_party => error.UnexpectedReturn,\n");
            try writer.writeAll("            };\n");
            try writer.writeAll("        }\n");
            try writer.writeAll("    };\n");
            try writer.print("    pub const BootstrapCallback = *const fn (ctx: *anyopaque, peer: *rpc.peer.Peer, response: {s}BootstrapResponse) anyerror!void;\n\n", .{qual});

            try writer.writeAll("    const BootstrapContext = struct {\n");
            try writer.writeAll("        user_ctx: *anyopaque,\n");
            try writer.print("        callback: {s}BootstrapCallback,\n\n", .{qual});
            try writer.writeAll("        fn deinitCtx(ctx_allocator: std.mem.Allocator, ctx_ptr: *anyopaque) void {\n");
            try writer.print("            const dead: *{s}BootstrapContext = @ptrCast(@alignCast(ctx_ptr));\n", .{qual});
            try writer.writeAll("            ctx_allocator.destroy(dead);\n");
            try writer.writeAll("        }\n");
            try writer.writeAll("    };\n\n");

            try writer.writeAll("    fn bootstrapReturn(ctx_ptr: *anyopaque, peer: *rpc.peer.Peer, ret: rpc.wire.protocol.Return, caps: *const rpc.caps.table.InboundCapTable) anyerror!void {\n");
            try writer.print("        const ctx: *{s}BootstrapContext = @ptrCast(@alignCast(ctx_ptr));\n", .{qual});
            try writer.writeAll("        defer peer.allocator.destroy(ctx);\n");
            try writer.print("        var response: {s}BootstrapResponse = undefined;\n", .{qual});
            try writer.writeAll("        switch (ret.tag) {\n");
            try writer.writeAll("            .results => {\n");
            try writer.writeAll("                const payload = ret.results orelse return error.MissingReturnPayload;\n");
            try writer.writeAll("                const cap = try payload.content.getCapability();\n");
            try writer.writeAll("                var mutable_caps = caps.*;\n");
            try writer.writeAll("                try mutable_caps.retainCapability(cap);\n");
            try writer.writeAll("                const resolved = try caps.resolveCapability(cap);\n");
            try writer.writeAll("                switch (resolved) {\n");
            try writer.print("                    .imported => |imported| response = .{{ .client = {s}Client.init(peer, imported.id) }},\n", .{qual});
            try writer.writeAll("                    else => return error.UnexpectedBootstrapCapability,\n");
            try writer.writeAll("                }\n");
            try writer.writeAll("            },\n");
            try writer.writeAll("            .exception => {\n");
            try writer.writeAll("                const ex = ret.exception orelse return error.MissingException;\n");
            try writer.writeAll("                response = .{ .exception = ex };\n");
            try writer.writeAll("            },\n");
            try writer.writeAll("            .canceled => response = .canceled,\n");
            try writer.writeAll("            .resultsSentElsewhere => response = .results_sent_elsewhere,\n");
            try writer.writeAll("            .takeFromOtherQuestion => {\n");
            try writer.writeAll("                const qid = ret.take_from_other_question orelse return error.MissingQuestionId;\n");
            try writer.writeAll("                response = .{ .take_from_other_question = qid };\n");
            try writer.writeAll("            },\n");
            try writer.writeAll("            .awaitFromThirdParty => response = .accept_from_third_party,\n");
            try writer.writeAll("        }\n");
            try writer.writeAll("        try ctx.callback(ctx.user_ctx, peer, response);\n");
            try writer.writeAll("    }\n\n");

            try writer.print("    pub fn bootstrap(peer: *rpc.peer.Peer, user_ctx: *anyopaque, callback: {s}BootstrapCallback) !u32 {{\n", .{qual});
            try writer.print("        const ctx = try peer.allocator.create({s}BootstrapContext);\n", .{qual});
            try writer.writeAll("        errdefer peer.allocator.destroy(ctx);\n");
            try writer.writeAll("        ctx.* = .{ .user_ctx = user_ctx, .callback = callback };\n");
            try writer.print("        const question_id = try peer.sendBootstrap(ctx, {s}bootstrapReturn);\n", .{qual});
            try writer.print("        peer.setQuestionDeinitCtx(question_id, {s}BootstrapContext.deinitCtx);\n", .{qual});
            try writer.writeAll("        return question_id;\n");
            try writer.writeAll("    }\n\n");

            // --- Server + VTable ---
            try writer.writeAll("    pub const Server = struct {\n");
            try writer.writeAll("        ctx: *anyopaque,\n");
            try writer.print("        vtable: {s}VTable,\n", .{qual});
            try writer.writeAll("    };\n\n");

            try writer.writeAll("    pub const VTable = struct {\n");
            // Own method fields
            for (interface_info.methods) |method| {
                try Self.generateVTableField(self, method, null, qual, writer);
            }
            // Inherited method fields
            for (ancestors) |ancestor| {
                for (ancestor.methods) |method| {
                    try Self.generateVTableField(self, method, ancestor.name, qual, writer);
                }
            }
            try writer.writeAll("    };\n\n");

            try writer.print("    pub fn exportServer(peer: *rpc.peer.Peer, server: *{s}Server) !u32 {{\n", .{qual});
            try writer.print("        return peer.addExport(.{{ .ctx = server, .on_call = {s}onCall }});\n", .{qual});
            try writer.writeAll("    }\n\n");

            try writer.print("    pub fn setBootstrap(peer: *rpc.peer.Peer, server: *{s}Server) !u32 {{\n", .{qual});
            try writer.print("        return peer.setBootstrap(.{{ .ctx = server, .on_call = {s}onCall }});\n", .{qual});
            try writer.writeAll("    }\n\n");

            // --- onCall dispatch ---
            try writer.writeAll("    fn onCall(ctx: *anyopaque, peer: *rpc.peer.Peer, call: rpc.wire.protocol.Call, caps: *const rpc.caps.table.InboundCapTable) anyerror!void {\n");
            try writer.print("        const server: *{s}Server = @ptrCast(@alignCast(ctx));\n", .{qual});

            var dispatch_method_count: usize = interface_info.methods.len;
            for (ancestors) |ancestor| {
                dispatch_method_count += ancestor.methods.len;
            }
            if (dispatch_method_count == 0) {
                try writer.writeAll("        _ = server;\n");
                try writer.writeAll("        _ = caps;\n");
            }

            if (has_ancestors) {
                // Dispatch by interface_id first, then method_id
                try writer.print("        if (call.interface_id == {s}interface_id) {{\n", .{qual});
                try writer.writeAll("            switch (call.method_id) {\n");
                for (interface_info.methods) |method| {
                    const zig_name = try self.toZigIdentifier(method.name);
                    defer self.allocator.free(zig_name);
                    try writer.print("                {s}{s}.ordinal => try {s}{s}.handleCall(server, peer, call, caps),\n", .{ qual, zig_name, qual, zig_name });
                }
                try writer.writeAll("                else => try peer.sendReturnException(call.question_id, \"unknown method\"),\n");
                try writer.writeAll("            }\n");

                for (ancestors) |ancestor| {
                    try writer.print("        }} else if (call.interface_id == {s}.interface_id) {{\n", .{ancestor.name});
                    try writer.writeAll("            switch (call.method_id) {\n");
                    for (ancestor.methods) |method| {
                        const zig_name = try self.toZigIdentifier(method.name);
                        defer self.allocator.free(zig_name);
                        const method_field = try self.lowerFirst(zig_name);
                        defer self.allocator.free(method_field);
                        const escaped_field = try types.escapeZigKeyword(self.allocator, method_field);
                        defer self.allocator.free(escaped_field);
                        const deferred_field = try std.fmt.allocPrint(self.allocator, "{s}_deferred", .{method_field});
                        defer self.allocator.free(deferred_field);
                        const escaped_deferred_field = try types.escapeZigKeyword(self.allocator, deferred_field);
                        defer self.allocator.free(escaped_deferred_field);
                        if (method.isStreaming()) {
                            try writer.print("                {s}.{s}.ordinal => try {s}.{s}.handleCallDirect(server.vtable.{s}, server.ctx, peer, call, caps),\n", .{
                                ancestor.name, zig_name, ancestor.name, zig_name, escaped_field,
                            });
                        } else {
                            try writer.print("                {s}.{s}.ordinal => try {s}.{s}.handleCallDirect(server.vtable.{s}, server.vtable.{s}, server.ctx, peer, call, caps),\n", .{
                                ancestor.name, zig_name, ancestor.name, zig_name, escaped_field, escaped_deferred_field,
                            });
                        }
                    }
                    try writer.writeAll("                else => try peer.sendReturnException(call.question_id, \"unknown method\"),\n");
                    try writer.writeAll("            }\n");
                }
                try writer.writeAll("        } else {\n");
                try writer.writeAll("            try peer.sendReturnException(call.question_id, \"unknown interface\");\n");
                try writer.writeAll("        }\n");
            } else {
                // No ancestors — simple dispatch by method_id only (backward compatible)
                try writer.writeAll("        switch (call.method_id) {\n");
                for (interface_info.methods) |method| {
                    const zig_name = try self.toZigIdentifier(method.name);
                    defer self.allocator.free(zig_name);
                    try writer.print("            {s}{s}.ordinal => try {s}{s}.handleCall(server, peer, call, caps),\n", .{ qual, zig_name, qual, zig_name });
                }
                try writer.writeAll("            else => try peer.sendReturnException(call.question_id, \"unknown method\"),\n");
                try writer.writeAll("        }\n");
            }

            try writer.writeAll("    }\n");

            // Splice the interface's nested types + method param/result struct
            // definitions inside the interface body, before the closing brace.
            if (children) |c| {
                try G.writeReindented(writer, c, 4);
            }

            try writer.writeAll("};\n\n");
        }

        /// Generate a single method struct inside an interface.
        /// `ordinal` is the method's index in the interface's methods list, which capnp
        /// guarantees equals its wire ordinal (methods are ordered by ordinal).
        pub fn generateMethodStruct(self: *G, iface_node: *const schema.Node, method: schema.Method, ordinal: usize, qual: []const u8, writer: anytype) !void {
            const zig_name = try self.toZigIdentifier(method.name);
            defer self.allocator.free(zig_name);
            const escaped_zig_name = try types.escapeZigKeyword(self.allocator, zig_name);
            defer self.allocator.free(escaped_zig_name);
            const method_field = try self.lowerFirst(zig_name);
            defer self.allocator.free(method_field);
            const escaped_method_field = try types.escapeZigKeyword(self.allocator, method_field);
            defer self.allocator.free(escaped_method_field);
            const param_name = try self.resolveMethodStructName(iface_node, method.param_struct_type);
            defer self.allocator.free(param_name);
            const result_name = try self.resolveMethodStructName(iface_node, method.result_struct_type);
            defer self.allocator.free(result_name);

            const param_layout = self.structLayout(method.param_struct_type) orelse return error.InvalidStructNode;
            const result_layout = self.structLayout(method.result_struct_type) orelse return error.InvalidStructNode;
            const is_streaming = method.isStreaming();

            try writer.print("    pub const {s} = struct {{\n", .{escaped_zig_name});
            try writer.print("        pub const ordinal: u16 = {};\n", .{ordinal});
            try writer.print("        pub const is_streaming: bool = {};\n", .{is_streaming});
            try writer.print("        pub const Params = {s};\n", .{param_name});
            try writer.print("        pub const Results = {s};\n", .{result_name});
            try writer.writeAll("        pub const BuildFn = *const fn (ctx: *anyopaque, params: *Params.Builder) anyerror!void;\n");

            if (is_streaming) {
                try writer.writeAll("        pub const StreamHandler = *const fn (ctx: *anyopaque, peer: *rpc.peer.Peer, params: Params.Reader, caps: *const rpc.caps.table.InboundCapTable) anyerror!void;\n");
            } else {
                try writer.writeAll("        pub const Handler = *const fn (ctx: *anyopaque, peer: *rpc.peer.Peer, params: Params.Reader, results: *Results.Builder, caps: *const rpc.caps.table.InboundCapTable) anyerror!void;\n");
                try writer.writeAll("        pub const DeferredHandler = *const fn (ctx: *anyopaque, peer: *rpc.peer.Peer, params: Params.Reader, caps: *const rpc.caps.table.InboundCapTable, sender: ReturnSender) anyerror!void;\n");
            }

            try writer.writeAll("        pub const Response = union(enum) {\n");
            try writer.writeAll("            results: Results.Reader,\n");
            try writer.writeAll("            exception: rpc.wire.protocol.Exception,\n");
            try writer.writeAll("            canceled,\n");
            try writer.writeAll("            results_sent_elsewhere,\n");
            try writer.writeAll("            take_from_other_question: u32,\n");
            try writer.writeAll("            accept_from_third_party,\n\n");
            try writer.writeAll("            /// Collapse this Response into its success payload or a typed\n");
            try writer.writeAll("            /// rpc.peer.CallError. Classification comes from the exception's\n");
            try writer.writeAll("            /// spec `Exception.Type`, so a disconnect reported by ANY\n");
            try writer.writeAll("            /// implementation is recognized; every other exception is\n");
            try writer.writeAll("            /// RemoteException (reason available on the union arm).\n");
            try writer.writeAll("            pub fn unwrap(self: Response) rpc.peer.CallError!Results.Reader {\n");
            try writer.writeAll("                return switch (self) {\n");
            try writer.writeAll("                    .results => |r| r,\n");
            try writer.writeAll("                    .exception => |ex| switch (ex.kind()) {\n");
            try writer.writeAll("                        .disconnected => error.Disconnected,\n");
            try writer.writeAll("                        // The spec has no distinct timeout type and classes\n");
            try writer.writeAll("                        // timeouts as overloaded, so our own deadline sentinel\n");
            try writer.writeAll("                        // still separates a local timeout from a remote's\n");
            try writer.writeAll("                        // genuine backpressure.\n");
            try writer.writeAll("                        .overloaded => if (std.mem.eql(u8, ex.reason, rpc.peer.deadline_reason))\n");
            try writer.writeAll("                            error.CallTimedOut\n");
            try writer.writeAll("                        else\n");
            try writer.writeAll("                            error.RemoteException,\n");
            try writer.writeAll("                        // .failed, .unimplemented (spec: treat like failed), and\n");
            try writer.writeAll("                        // any future code. `else` is required: the enum is\n");
            try writer.writeAll("                        // non-exhaustive because the wire field is remote-controlled.\n");
            try writer.writeAll("                        else => error.RemoteException,\n");
            try writer.writeAll("                    },\n");
            try writer.writeAll("                    .canceled => error.Canceled,\n");
            try writer.writeAll("                    .results_sent_elsewhere, .take_from_other_question, .accept_from_third_party => error.UnexpectedReturn,\n");
            try writer.writeAll("                };\n");
            try writer.writeAll("            }\n");
            try writer.writeAll("        };\n");
            try writer.writeAll("        pub const Callback = *const fn (ctx: *anyopaque, peer: *rpc.peer.Peer, response: Response, caps: *const rpc.caps.table.InboundCapTable) anyerror!void;\n\n");

            try writer.writeAll("        const CallContext = struct {\n");
            try writer.writeAll("            user_ctx: *anyopaque,\n");
            try writer.writeAll("            build: ?BuildFn,\n");
            try writer.writeAll("            callback: Callback,\n\n");
            try writer.writeAll("            // While a generated send is still on the caller's stack, this\n");
            try writer.writeAll("            // points at its ownership flag. A synchronous Return marks the\n");
            try writer.writeAll("            // context settled before freeing it; an asynchronous send clears\n");
            try writer.writeAll("            // the pointer before returning to the user.\n");
            try writer.writeAll("            settled_flag: ?*bool = null,\n\n");
            try writer.writeAll("            // Frees the heap ctx if the question is still outstanding at\n");
            try writer.writeAll("            // Peer.deinit (the normal return path frees it in callReturn).\n");
            try writer.writeAll("            fn deinitCtx(ctx_allocator: std.mem.Allocator, ctx_ptr: *anyopaque) void {\n");
            try writer.writeAll("                const dead: *CallContext = @ptrCast(@alignCast(ctx_ptr));\n");
            try writer.writeAll("                ctx_allocator.destroy(dead);\n");
            try writer.writeAll("            }\n");
            try writer.writeAll("        };\n\n");

            if (!is_streaming) {
                try writer.writeAll("        const DirectReturnContext = struct {\n");
                try writer.writeAll("            handler: Handler,\n");
                try writer.writeAll("            ctx: *anyopaque,\n");
                try writer.writeAll("            peer: *rpc.peer.Peer,\n");
                try writer.writeAll("            params: Params.Reader,\n");
                try writer.writeAll("            caps: *const rpc.caps.table.InboundCapTable,\n");
                try writer.writeAll("        };\n\n");

                try writer.writeAll("        pub const ReturnSender = struct {\n");
                try writer.writeAll("            peer: *rpc.peer.Peer,\n");
                try writer.writeAll("            question_id: u32,\n\n");
                try writer.writeAll("            pub fn sendResults(self: ReturnSender, ctx: *anyopaque, build: *const fn (ctx: *anyopaque, ret: *rpc.wire.protocol.ReturnBuilder) anyerror!void) !void {\n");
                try writer.writeAll("                try self.peer.sendReturnResults(self.question_id, ctx, build);\n");
                try writer.writeAll("            }\n\n");
                try writer.writeAll("            pub fn sendException(self: ReturnSender, reason: []const u8) !void {\n");
                try writer.writeAll("                try self.peer.sendReturnException(self.question_id, reason);\n");
                try writer.writeAll("            }\n");
                try writer.writeAll("        };\n\n");
            }

            try writer.writeAll("        fn callBuild(ctx_ptr: *anyopaque, call: *rpc.wire.protocol.CallBuilder) anyerror!void {\n");
            try writer.writeAll("            const ctx: *CallContext = @ptrCast(@alignCast(ctx_ptr));\n");
            try writer.writeAll("            var payload = try call.payloadTyped();\n");
            try writer.writeAll("            var params_any = try payload.initContent();\n");
            try writer.print("            const params_builder = try params_any.initStruct({}, {});\n", .{
                param_layout.data_words,
                param_layout.pointer_words,
            });
            try writer.writeAll("            var params = Params.Builder.wrap(params_builder);\n");
            try writer.writeAll("            if (ctx.build) |build_fn| {\n");
            try writer.writeAll("                try build_fn(ctx.user_ctx, &params);\n");
            try writer.writeAll("            }\n");
            try writer.writeAll("            _ = try call.initCapTableTyped(0);\n");
            try writer.writeAll("        }\n\n");

            try writer.writeAll("        fn callReturn(ctx_ptr: *anyopaque, peer: *rpc.peer.Peer, ret: rpc.wire.protocol.Return, caps: *const rpc.caps.table.InboundCapTable) anyerror!void {\n");
            try writer.writeAll("            const ctx: *CallContext = @ptrCast(@alignCast(ctx_ptr));\n");
            try writer.writeAll("            if (ctx.settled_flag) |flag| flag.* = true;\n");
            try writer.writeAll("            defer peer.allocator.destroy(ctx);\n");
            try writer.writeAll("            var response: Response = undefined;\n");
            try writer.writeAll("            switch (ret.tag) {\n");
            try writer.writeAll("                .results => {\n");
            try writer.writeAll("                    const payload = ret.results orelse return error.MissingReturnPayload;\n");
            try writer.writeAll("                    const struct_reader = try payload.content.getStruct();\n");
            try writer.writeAll("                    const results = Results.Reader.wrap(struct_reader);\n");
            try writer.writeAll("                    response = .{ .results = results };\n");
            try writer.writeAll("                },\n");
            try writer.writeAll("                .exception => {\n");
            try writer.writeAll("                    const ex = ret.exception orelse return error.MissingException;\n");
            try writer.writeAll("                    response = .{ .exception = ex };\n");
            try writer.writeAll("                },\n");
            try writer.writeAll("                .canceled => response = .canceled,\n");
            try writer.writeAll("                .resultsSentElsewhere => response = .results_sent_elsewhere,\n");
            try writer.writeAll("                .takeFromOtherQuestion => {\n");
            try writer.writeAll("                    const qid = ret.take_from_other_question orelse return error.MissingQuestionId;\n");
            try writer.writeAll("                    response = .{ .take_from_other_question = qid };\n");
            try writer.writeAll("                },\n");
            try writer.writeAll("                .awaitFromThirdParty => response = .accept_from_third_party,\n");
            try writer.writeAll("            }\n");
            try writer.writeAll("            try ctx.callback(ctx.user_ctx, peer, response, caps);\n");
            try writer.writeAll("        }\n\n");

            if (is_streaming) {
                // handleCallDirect: takes StreamHandler + ctx directly
                try writer.writeAll("        pub fn handleCallDirect(handler: StreamHandler, ctx: *anyopaque, peer: *rpc.peer.Peer, call: rpc.wire.protocol.Call, caps: *const rpc.caps.table.InboundCapTable) anyerror!void {\n");
                try writer.writeAll("            const params_struct = try call.params.content.getStruct();\n");
                try writer.writeAll("            const params = Params.Reader.wrap(params_struct);\n");
                try writer.writeAll("            try handler(ctx, peer, params, caps);\n");
                try writer.writeAll("            try peer.sendReturnEmptyStruct(call.question_id);\n");
                try writer.writeAll("        }\n\n");

                // handleCall delegates to handleCallDirect
                try writer.print("        fn handleCall(server: *{s}Server, peer: *rpc.peer.Peer, call: rpc.wire.protocol.Call, caps: *const rpc.caps.table.InboundCapTable) anyerror!void {{\n", .{qual});
                try writer.print("            try handleCallDirect(server.vtable.{s}, server.ctx, peer, call, caps);\n", .{escaped_method_field});
                try writer.writeAll("        }\n\n");

                // StreamCallContext + streamCallBuild + streamCallReturn for fire-and-forget streaming
                try writer.writeAll("        pub const StreamCallContext = struct {\n");
                try writer.writeAll("            stream: *rpc.transport.stream_state.StreamState,\n");
                try writer.writeAll("            build_ctx: *anyopaque,\n");
                try writer.writeAll("            build: ?BuildFn,\n");
                try writer.writeAll("        };\n\n");

                try writer.writeAll("        fn streamCallBuild(ctx_ptr: *anyopaque, call: *rpc.wire.protocol.CallBuilder) anyerror!void {\n");
                try writer.writeAll("            const ctx: *StreamCallContext = @ptrCast(@alignCast(ctx_ptr));\n");
                try writer.writeAll("            var payload = try call.payloadTyped();\n");
                try writer.writeAll("            var params_any = try payload.initContent();\n");
                try writer.print("            const params_builder = try params_any.initStruct({}, {});\n", .{
                    param_layout.data_words,
                    param_layout.pointer_words,
                });
                try writer.writeAll("            var params = Params.Builder.wrap(params_builder);\n");
                try writer.writeAll("            if (ctx.build) |build_fn| try build_fn(ctx.build_ctx, &params);\n");
                try writer.writeAll("            _ = try call.initCapTableTyped(0);\n");
                try writer.writeAll("        }\n\n");

                try writer.writeAll("        fn streamCallReturn(ctx_ptr: *anyopaque, peer: *rpc.peer.Peer, ret: rpc.wire.protocol.Return, caps: *const rpc.caps.table.InboundCapTable) anyerror!void {\n");
                try writer.writeAll("            const ctx: *StreamCallContext = @ptrCast(@alignCast(ctx_ptr));\n");
                try writer.writeAll("            defer peer.allocator.destroy(ctx);\n");
                try writer.writeAll("            _ = caps;\n");
                try writer.writeAll("            ctx.stream.handleReturn(ret.tag == .exception);\n");
                try writer.writeAll("        }\n");
            } else {
                // handleCallDirect: takes Handler + ?DeferredHandler + ctx directly
                try writer.writeAll("        pub fn handleCallDirect(handler: Handler, deferred_handler: ?DeferredHandler, ctx: *anyopaque, peer: *rpc.peer.Peer, call: rpc.wire.protocol.Call, caps: *const rpc.caps.table.InboundCapTable) anyerror!void {\n");
                try writer.writeAll("            const params_struct = try call.params.content.getStruct();\n");
                try writer.writeAll("            const params = Params.Reader.wrap(params_struct);\n");
                try writer.writeAll("            if (deferred_handler) |deferred_fn| {\n");
                try writer.writeAll("                const sender = ReturnSender{ .peer = peer, .question_id = call.question_id };\n");
                try writer.writeAll("                try deferred_fn(ctx, peer, params, caps, sender);\n");
                try writer.writeAll("            } else {\n");
                try writer.writeAll("                var dctx = DirectReturnContext{\n");
                try writer.writeAll("                    .handler = handler,\n");
                try writer.writeAll("                    .ctx = ctx,\n");
                try writer.writeAll("                    .peer = peer,\n");
                try writer.writeAll("                    .params = params,\n");
                try writer.writeAll("                    .caps = caps,\n");
                try writer.writeAll("                };\n");
                try writer.writeAll("                try peer.sendReturnResults(call.question_id, &dctx, buildReturnDirect);\n");
                try writer.writeAll("            }\n");
                try writer.writeAll("        }\n\n");

                // handleCall delegates to handleCallDirect
                try writer.print("        fn handleCall(server: *{s}Server, peer: *rpc.peer.Peer, call: rpc.wire.protocol.Call, caps: *const rpc.caps.table.InboundCapTable) anyerror!void {{\n", .{qual});
                const deferred_field = try std.fmt.allocPrint(self.allocator, "{s}_deferred", .{method_field});
                defer self.allocator.free(deferred_field);
                const escaped_deferred_field = try types.escapeZigKeyword(self.allocator, deferred_field);
                defer self.allocator.free(escaped_deferred_field);
                try writer.print("            try handleCallDirect(server.vtable.{s}, server.vtable.{s}, server.ctx, peer, call, caps);\n", .{ escaped_method_field, escaped_deferred_field });
                try writer.writeAll("        }\n\n");

                try writer.writeAll("        fn buildReturnDirect(ctx_ptr: *anyopaque, ret: *rpc.wire.protocol.ReturnBuilder) anyerror!void {\n");
                try writer.writeAll("            const dctx: *DirectReturnContext = @ptrCast(@alignCast(ctx_ptr));\n");
                try writer.writeAll("            var payload = try ret.payloadTyped();\n");
                try writer.writeAll("            var results_any = try payload.initContent();\n");
                try writer.print("            const results_builder = try results_any.initStruct({}, {});\n", .{
                    result_layout.data_words,
                    result_layout.pointer_words,
                });
                try writer.writeAll("            var results = Results.Builder.wrap(results_builder);\n");
                try writer.writeAll("            try dctx.handler(dctx.ctx, dctx.peer, dctx.params, &results, dctx.caps);\n");
                // No cap-table init here: buildReturnDirect only ever runs inside
                // peer.sendReturnResults, whose encodeReturnPayloadCapsWithEffects
                // pass re-derives and (re)writes the payload cap table
                // unconditionally, so a zero-length placeholder is dead weight.
                try writer.writeAll("        }\n");
            }

            try writer.writeAll("    };\n\n");
        }

        /// Generate a VTable field for a method. If `ancestor_name` is set, uses the
        /// ancestor's (fully qualified) types; otherwise `qual` disambiguates the own
        /// method struct when this interface is nested inside another.
        pub fn generateVTableField(self: *G, method: schema.Method, ancestor_name: ?[]const u8, qual: []const u8, writer: anytype) !void {
            const zig_name = try self.toZigIdentifier(method.name);
            defer self.allocator.free(zig_name);
            const method_field = try self.lowerFirst(zig_name);
            defer self.allocator.free(method_field);
            const escaped_field = try types.escapeZigKeyword(self.allocator, method_field);
            defer self.allocator.free(escaped_field);
            const deferred_field = try std.fmt.allocPrint(self.allocator, "{s}_deferred", .{method_field});
            defer self.allocator.free(deferred_field);
            const escaped_deferred_field = try types.escapeZigKeyword(self.allocator, deferred_field);
            defer self.allocator.free(escaped_deferred_field);
            if (ancestor_name) |aname| {
                if (method.isStreaming()) {
                    try writer.print("        {s}: {s}.{s}.StreamHandler,\n", .{ escaped_field, aname, zig_name });
                } else {
                    try writer.print("        {s}: {s}.{s}.Handler,\n", .{ escaped_field, aname, zig_name });
                    try writer.print("        {s}: ?{s}.{s}.DeferredHandler = null,\n", .{ escaped_deferred_field, aname, zig_name });
                }
            } else {
                if (method.isStreaming()) {
                    try writer.print("        {s}: {s}{s}.StreamHandler,\n", .{ escaped_field, qual, zig_name });
                } else {
                    try writer.print("        {s}: {s}{s}.Handler,\n", .{ escaped_field, qual, zig_name });
                    try writer.print("        {s}: ?{s}{s}.DeferredHandler = null,\n", .{ escaped_deferred_field, qual, zig_name });
                }
            }
        }

        /// Resolved names for a method call, shared across Client, StreamClient, and PipelinedClient generation.
        const MethodCallParams = struct {
            zig_name: []const u8,
            call_name: []const u8,
            method_prefix: []const u8,
            dot: []const u8,
            iface_id: []const u8,
            iface_id_owned: bool,
        };

        /// `qual` is the own-interface disambiguation prefix (e.g. "Handle." for a
        /// nested interface, "" for a file-scoped one). For an OWN method the method
        /// struct and `interface_id` are referenced as `{qual}{Method}` /
        /// `{qual}interface_id`; for an INHERITED method the ancestor's fully
        /// qualified name is used instead and `qual` is irrelevant.
        pub fn resolveMethodCallParams(self: *G, method: schema.Method, interface_id_expr: ?[]const u8, ancestor_name: ?[]const u8, qual: []const u8) !MethodCallParams {
            const zig_name = try self.toZigIdentifier(method.name);
            errdefer self.allocator.free(zig_name);
            const call_name = try std.fmt.allocPrint(self.allocator, "call{s}", .{zig_name});
            errdefer self.allocator.free(call_name);

            // Own methods: qualify `interface_id` with the interface's own prefix so a
            // nested interface's dispatch doesn't bind the enclosing interface's
            // `interface_id`. Inherited methods reference the ancestor's constant.
            const iface_id = if (interface_id_expr) |expr| blk: {
                const temp = try std.fmt.allocPrint(self.allocator, "{s}{s}", .{ qual, expr });
                break :blk temp;
            } else blk: {
                const temp = try std.fmt.allocPrint(self.allocator, "{s}.interface_id", .{ancestor_name.?});
                break :blk temp;
            };

            return .{
                .zig_name = zig_name,
                .call_name = call_name,
                // Own: `{qual}{Method}` (qual carries its own trailing dot, so dot="").
                // Inherited: `{ancestor}.{Method}`.
                .method_prefix = ancestor_name orelse qual,
                .dot = if (ancestor_name != null) "." else "",
                .iface_id = iface_id,
                // iface_id is now always freshly allocated (both branches).
                .iface_id_owned = true,
            };
        }

        pub fn freeMethodCallParams(self: *G, params: MethodCallParams) void {
            self.allocator.free(params.zig_name);
            self.allocator.free(params.call_name);
            if (params.iface_id_owned) self.allocator.free(params.iface_id);
        }

        /// Generate a Client call method. Uses `interface_id_expr` for own methods or
        /// ancestor_name for inherited; `qual` disambiguates the Client receiver and
        /// own method structs when this interface is nested inside another.
        pub fn generateClientCallMethod(self: *G, method: schema.Method, interface_id_expr: ?[]const u8, ancestor_name: ?[]const u8, qual: []const u8, writer: anytype) !void {
            const p = try Self.resolveMethodCallParams(self, method, interface_id_expr, ancestor_name, qual);
            defer Self.freeMethodCallParams(self, p);

            try writer.print("        pub fn {s}(self: {s}Client, user_ctx: *anyopaque, build: ?{s}{s}{s}.BuildFn, on_return: {s}{s}{s}.Callback) !u32 {{\n", .{
                p.call_name, qual, p.method_prefix, p.dot, p.zig_name, p.method_prefix, p.dot, p.zig_name,
            });
            try writer.print("            return self.{s}WithOptions(user_ctx, build, on_return, .{{}});\n", .{p.call_name});
            try writer.writeAll("        }\n\n");
            try writer.print("        pub fn {s}WithOptions(self: {s}Client, user_ctx: *anyopaque, build: ?{s}{s}{s}.BuildFn, on_return: {s}{s}{s}.Callback, options: rpc.peer.CallOptions) !u32 {{\n", .{
                p.call_name, qual, p.method_prefix, p.dot, p.zig_name, p.method_prefix, p.dot, p.zig_name,
            });
            try writer.print("            const ctx = try self.peer.allocator.create({s}{s}{s}.CallContext);\n", .{ p.method_prefix, p.dot, p.zig_name });
            try writer.writeAll("            var settled = false;\n");
            try writer.writeAll("            ctx.* = .{ .user_ctx = user_ctx, .build = build, .callback = on_return, .settled_flag = &settled };\n");
            try writer.print("            const question_id = self.peer.sendCallGeneratedWithOptions(self.cap_id, {s}, {s}{s}{s}.ordinal, ctx, {s}{s}{s}.callBuild, {s}{s}{s}.callReturn, options) catch |err| {{\n", .{
                p.iface_id, p.method_prefix, p.dot, p.zig_name, p.method_prefix, p.dot, p.zig_name, p.method_prefix, p.dot, p.zig_name,
            });
            try writer.writeAll("                if (!settled) self.peer.allocator.destroy(ctx);\n");
            try writer.writeAll("                return err;\n");
            try writer.writeAll("            };\n");
            try writer.writeAll("            if (settled) return question_id;\n");
            try writer.writeAll("            ctx.settled_flag = null;\n");
            try writer.print("            self.peer.setQuestionDeinitCtx(question_id, {s}{s}{s}.CallContext.deinitCtx);\n", .{ p.method_prefix, p.dot, p.zig_name });
            try writer.writeAll("            return question_id;\n");
            try writer.writeAll("        }\n\n");
        }

        /// Generate a StreamClient type for an interface with streaming methods.
        pub fn generateStreamClient(
            self: *G,
            node: *const schema.Node,
            interface_info: schema.InterfaceNode,
            ancestors: []const G.AncestorInfo,
            qual: []const u8,
            writer: anytype,
        ) !void {
            _ = node;
            try writer.writeAll("    pub const StreamClient = struct {\n");
            try writer.print("        client: {s}Client,\n", .{qual});
            try writer.writeAll("        stream: rpc.transport.stream_state.StreamState = .{},\n\n");

            try writer.print("        pub fn init(client: {s}Client) {s}StreamClient {{\n", .{ qual, qual });
            try writer.writeAll("            return .{ .client = client };\n");
            try writer.writeAll("        }\n\n");

            // Own methods
            for (interface_info.methods) |method| {
                try Self.generateStreamClientCallMethod(self, method, "interface_id", null, qual, writer);
            }
            // Inherited methods
            for (ancestors) |ancestor| {
                for (ancestor.methods) |method| {
                    try Self.generateStreamClientCallMethod(self, method, null, ancestor.name, qual, writer);
                }
            }

            try writer.print("        pub fn waitStreaming(self: *{s}StreamClient, ctx: *anyopaque, callback: rpc.transport.stream_state.StreamState.DrainCallback) void {{\n", .{qual});
            try writer.writeAll("            self.stream.waitStreaming(ctx, callback);\n");
            try writer.writeAll("        }\n");

            try writer.writeAll("    };\n\n");
        }

        /// Generate a single StreamClient call method. Streaming methods become
        /// fire-and-forget; non-streaming methods pass through to the inner Client.
        pub fn generateStreamClientCallMethod(
            self: *G,
            method: schema.Method,
            interface_id_expr: ?[]const u8,
            ancestor_name: ?[]const u8,
            qual: []const u8,
            writer: anytype,
        ) !void {
            const p = try Self.resolveMethodCallParams(self, method, interface_id_expr, ancestor_name, qual);
            defer Self.freeMethodCallParams(self, p);

            if (method.isStreaming()) {
                // Fire-and-forget streaming call
                try writer.print("        pub fn {s}(self: *{s}StreamClient, build_ctx: *anyopaque, build: ?{s}{s}{s}.BuildFn) !void {{\n", .{
                    p.call_name, qual, p.method_prefix, p.dot, p.zig_name,
                });
                try writer.writeAll("            if (self.stream.hasFailed()) return self.stream.stream_error.?;\n");
                try writer.writeAll("            try self.stream.noteCallSent();\n");
                try writer.print("            const ctx = self.client.peer.allocator.create({s}{s}{s}.StreamCallContext) catch |err| {{\n", .{ p.method_prefix, p.dot, p.zig_name });
                try writer.writeAll("                self.stream.in_flight -= 1;\n");
                try writer.writeAll("                return err;\n");
                try writer.writeAll("            };\n");
                try writer.writeAll("            ctx.* = .{ .stream = &self.stream, .build_ctx = build_ctx, .build = build };\n");
                try writer.print("            _ = self.client.peer.sendCall(self.client.cap_id, {s}, {s}{s}{s}.ordinal, ctx, {s}{s}{s}.streamCallBuild, {s}{s}{s}.streamCallReturn) catch |err| {{\n", .{
                    p.iface_id, p.method_prefix, p.dot, p.zig_name, p.method_prefix, p.dot, p.zig_name, p.method_prefix, p.dot, p.zig_name,
                });
                try writer.writeAll("                self.stream.in_flight -= 1;\n");
                try writer.writeAll("                self.client.peer.allocator.destroy(ctx);\n");
                try writer.writeAll("                return err;\n");
                try writer.writeAll("            };\n");
                try writer.writeAll("        }\n\n");
            } else {
                // Pass-through to inner Client
                try writer.print("        pub fn {s}(self: *{s}StreamClient, user_ctx: *anyopaque, build: ?{s}{s}{s}.BuildFn, on_return: {s}{s}{s}.Callback) !u32 {{\n", .{
                    p.call_name, qual, p.method_prefix, p.dot, p.zig_name, p.method_prefix, p.dot, p.zig_name,
                });
                try writer.print("            return self.client.{s}(user_ctx, build, on_return);\n", .{p.call_name});
                try writer.writeAll("        }\n\n");
            }
        }

        /// Generate a callXxxPipelined method on Client if the method has interface-typed results.
        /// `qual` disambiguates the Client receiver, the own `{Method}Pipeline` return
        /// type, and the own method struct when this interface is nested inside another.
        pub fn generateClientPipelinedMethod(self: *G, method: schema.Method, ancestor_name: ?[]const u8, qual: []const u8, writer: anytype) !void {
            const iface_fields = try self.getInterfaceFields(method.result_struct_type);
            defer self.freeInterfaceFields(iface_fields);
            if (iface_fields.len == 0) return;

            const zig_name = try self.toZigIdentifier(method.name);
            defer self.allocator.free(zig_name);

            // The Pipeline type is declared as a sibling of Client named `{Method}Pipeline`
            // (see generatePipelineType / allocMethodPipelineName), NOT `{Method}.Pipeline`.
            const pipeline_name = try self.allocMethodPipelineName(method.name);
            defer self.allocator.free(pipeline_name);

            // Own method: `{qual}{Method}` (qual carries its trailing dot). Inherited:
            // `{ancestor}.{Method}`, and the Pipeline type lives on the ancestor.
            const method_prefix = ancestor_name orelse qual;
            const dot = if (ancestor_name != null) "." else "";

            try writer.print("        pub fn call{s}Pipelined(self: {s}Client, user_ctx: *anyopaque, build: ?{s}{s}{s}.BuildFn, on_return: {s}{s}{s}.Callback) !{s}{s}{s} {{\n", .{
                zig_name, qual, method_prefix, dot, zig_name, method_prefix, dot, zig_name, method_prefix, dot, pipeline_name,
            });
            try writer.print("            return self.call{s}PipelinedWithOptions(user_ctx, build, on_return, .{{}});\n", .{zig_name});
            try writer.writeAll("        }\n\n");
            try writer.print("        pub fn call{s}PipelinedWithOptions(self: {s}Client, user_ctx: *anyopaque, build: ?{s}{s}{s}.BuildFn, on_return: {s}{s}{s}.Callback, options: rpc.peer.CallOptions) !{s}{s}{s} {{\n", .{
                zig_name, qual, method_prefix, dot, zig_name, method_prefix, dot, zig_name, method_prefix, dot, pipeline_name,
            });
            try writer.print("            const qid = try self.call{s}WithOptions(user_ctx, build, on_return, options);\n", .{zig_name});
            try writer.writeAll("            return .{ .peer = self.peer, .question_id = qid };\n");
            try writer.writeAll("        }\n\n");
        }

        /// Generate a Pipeline type for a method with interface-typed results.
        pub fn generatePipelineType(self: *G, method: schema.Method, ancestor_name: ?[]const u8, writer: anytype) !void {
            const iface_fields = try self.getInterfaceFields(method.result_struct_type);
            defer self.freeInterfaceFields(iface_fields);
            if (iface_fields.len == 0) return;

            const zig_name = try self.toZigIdentifier(method.name);
            defer self.allocator.free(zig_name);
            const escaped_zig_name = try types.escapeZigKeyword(self.allocator, zig_name);
            defer self.allocator.free(escaped_zig_name);

            // For inherited methods, the Pipeline type is defined on the parent interface,
            // so we don't re-generate it here. The client method references the parent's Pipeline type.
            if (ancestor_name != null) return;

            try writer.print("    pub const {s}Pipeline = struct {{\n", .{escaped_zig_name});
            try writer.writeAll("        peer: *rpc.peer.Peer,\n");
            try writer.writeAll("        question_id: u32,\n\n");

            for (iface_fields) |ifield| {
                try writer.print("        pub fn get{s}(self: @This()) {s}.PipelinedClient {{\n", .{ ifield.name, ifield.type_name });
                try writer.print("            return .{{ .peer = self.peer, .question_id = self.question_id, .pointer_index = {} }};\n", .{ifield.pointer_offset});
                try writer.writeAll("        }\n\n");
            }

            try writer.writeAll("    };\n\n");
        }

        /// Generate a PipelinedClient call method. `qual` disambiguates the
        /// PipelinedClient receiver and own method structs under nesting.
        pub fn generatePipelinedClientCallMethod(self: *G, method: schema.Method, interface_id_expr: ?[]const u8, ancestor_name: ?[]const u8, qual: []const u8, writer: anytype) !void {
            const p = try Self.resolveMethodCallParams(self, method, interface_id_expr, ancestor_name, qual);
            defer Self.freeMethodCallParams(self, p);

            try writer.print("        pub fn {s}(self: {s}PipelinedClient, user_ctx: *anyopaque, build: ?{s}{s}{s}.BuildFn, on_return: {s}{s}{s}.Callback) !u32 {{\n", .{
                p.call_name, qual, p.method_prefix, p.dot, p.zig_name, p.method_prefix, p.dot, p.zig_name,
            });
            try writer.print("            return self.{s}WithOptions(user_ctx, build, on_return, .{{}});\n", .{p.call_name});
            try writer.writeAll("        }\n\n");
            try writer.print("        pub fn {s}WithOptions(self: {s}PipelinedClient, user_ctx: *anyopaque, build: ?{s}{s}{s}.BuildFn, on_return: {s}{s}{s}.Callback, options: rpc.peer.CallOptions) !u32 {{\n", .{
                p.call_name, qual, p.method_prefix, p.dot, p.zig_name, p.method_prefix, p.dot, p.zig_name,
            });
            try writer.print("            const ctx = try self.peer.allocator.create({s}{s}{s}.CallContext);\n", .{ p.method_prefix, p.dot, p.zig_name });
            try writer.writeAll("            var settled = false;\n");
            try writer.writeAll("            ctx.* = .{ .user_ctx = user_ctx, .build = build, .callback = on_return, .settled_flag = &settled };\n");
            try writer.print("            const question_id = self.peer.sendCallPromisedWithOpsGeneratedWithOptions(self.question_id, &[_]rpc.wire.protocol.PromisedAnswerOp{{.{{ .tag = .getPointerField, .pointer_index = self.pointer_index }}}}, {s}, {s}{s}{s}.ordinal, ctx, {s}{s}{s}.callBuild, {s}{s}{s}.callReturn, options) catch |err| {{\n", .{
                p.iface_id, p.method_prefix, p.dot, p.zig_name, p.method_prefix, p.dot, p.zig_name, p.method_prefix, p.dot, p.zig_name,
            });
            try writer.writeAll("                if (!settled) self.peer.allocator.destroy(ctx);\n");
            try writer.writeAll("                return err;\n");
            try writer.writeAll("            };\n");
            try writer.writeAll("            if (settled) return question_id;\n");
            try writer.writeAll("            ctx.settled_flag = null;\n");
            try writer.print("            self.peer.setQuestionDeinitCtx(question_id, {s}{s}{s}.CallContext.deinitCtx);\n", .{ p.method_prefix, p.dot, p.zig_name });
            try writer.writeAll("            return question_id;\n");
            try writer.writeAll("        }\n\n");
        }
    };
}
