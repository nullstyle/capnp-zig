//! Additive typed applications. Wire calls remain delegated to the existing
//! erased generated methods, so typed thunks share their question ownership.
const std = @import("std");
const schema = @import("../serialization/schema.zig");
const types = @import("types.zig");
const applications = @import("generic_application.zig");
const resolution = @import("../serialization/type_resolver.zig");

pub fn Emitter(comptime G: type) type {
    return struct {
        const Self = @This();
        fn owner(self: *G, node: *const schema.Node) ?*const schema.Node {
            if (node.scope_id != 0) return self.getNode(node.scope_id);
            for (self.nodes) |*candidate| {
                const iface = candidate.interface_node orelse continue;
                for (iface.methods) |method| if (method.param_struct_type == node.id or method.result_struct_type == node.id) return candidate;
            }
            return null;
        }
        fn methodFor(self: *G, node: *const schema.Node) ?schema.Method {
            const parent = owner(self, node) orelse return null;
            const iface = parent.interface_node orelse return null;
            for (iface.methods) |method| if (method.param_struct_type == node.id or method.result_struct_type == node.id) return method;
            return null;
        }
        pub fn needsData(self: *G, node: *const schema.Node) bool {
            if (node.is_generic) return true;
            if (methodFor(self, node)) |method| if (method.implicit_parameters.len > 0) return true;
            if (node.struct_node) |info| for (info.fields) |field| {
                if (field.slot) |slot| if (slot.type == .@"struct" or slot.type == .interface) {
                    // Ordinary structs can occur between a generic parameter
                    // and its eventual capability. Give each such hop an
                    // application so its typed pipeline composes too.
                    if (resolvableType(self, slot.type)) return true;
                };
            };
            return false;
        }
        fn resolvableType(self: *G, typ: schema.Type) bool {
            var current = typ;
            var depth: usize = 0;
            while (current == .list) {
                if (depth >= resolution.max_resolution_depth) return false;
                depth += 1;
                current = current.list.element_type.*;
            }
            return switch (current) {
                .@"struct" => |named| self.getNode(named.type_id) != null,
                .interface => |named| self.getNode(named.type_id) != null,
                .@"enum" => |named| self.getNode(named.type_id) != null,
                else => true,
            };
        }
        fn parameterName(self: *G, node: *const schema.Node, parameter: schema.TypeMetadata.AnyPointer) ![]const u8 {
            return switch (parameter) {
                .parameter => |p| blk: {
                    const scope = self.getNode(p.scope_id) orelse return error.InvalidStructNode;
                    if (p.parameter_index >= scope.parameters.len) return error.InvalidStructNode;
                    break :blk scope.parameters[p.parameter_index].name;
                },
                .implicit_method_parameter => |p| blk: {
                    const method = self.generic_method_context orelse methodFor(self, node) orelse return error.InvalidStructNode;
                    if (p.parameter_index >= method.implicit_parameters.len) return error.InvalidStructNode;
                    break :blk method.implicit_parameters[p.parameter_index].name;
                },
                else => error.InvalidStructNode,
            };
        }
        fn rootName(self: *G, id: schema.Id) ![]const u8 {
            const name = try self.qualifiedTypeName(id);
            defer self.allocator.free(name);
            return if (std.mem.startsWith(u8, name, "_capnp_file.")) self.allocator.dupe(u8, name) else std.fmt.allocPrint(self.allocator, "_capnp_file.{s}", .{name});
        }
        fn appliedName(self: *G, context: *const schema.Node, target: *const schema.Node, brand: schema.Brand) ![]const u8 {
            const raw = try rootName(self, target.id);
            defer self.allocator.free(raw);
            if (!needsData(self, target) and target.kind != .interface) return self.allocator.dupe(u8, raw);
            var args = std.ArrayList(u8).empty;
            defer args.deinit(self.allocator);
            const w = @import("generator.zig").ArrayListWriter{ .list = &args, .allocator = self.allocator, .max_bytes = self.codegen_budget.max_output_bytes };
            try w.print("{s}.Apply(.{{", .{raw});
            var scope: ?*const schema.Node = target;
            var depth: usize = 0;
            while (scope) |s| {
                if (depth >= 64) return error.InvalidStructNode;
                depth += 1;
                for (s.parameters, 0..) |parameter, index| {
                    if (s.id == target.id) if (methodFor(self, target)) |method| {
                        if (index < method.implicit_parameters.len) {
                            try w.print(" .{s} = @field(_method_bindings, \"{s}\"),", .{ parameter.name, parameter.name });
                            continue;
                        }
                    };
                    var expression: ?schema.TypeExpression = null;
                    for (brand.scopes) |binding_scope| if (binding_scope.scope_id == s.id and binding_scope.binding == .bind) {
                        const bindings = binding_scope.binding.bind;
                        if (index >= bindings.len) return error.InvalidStructNode;
                        if (bindings[index] == .type) expression = bindings[index].type.*;
                    };
                    const codec = if (expression) |e| try expressionCodec(self, context, e) else try std.fmt.allocPrint(self.allocator, "@field(_bindings, \"{s}\")", .{parameter.name});
                    defer self.allocator.free(codec);
                    try w.print(" .{s} = {s},", .{ parameter.name, codec });
                }
                scope = owner(self, s);
            }
            try w.writeAll(" })");
            return args.toOwnedSlice(self.allocator);
        }
        fn expressionCodec(self: *G, node: *const schema.Node, expression: schema.TypeExpression) (std.mem.Allocator.Error || error{ InvalidStructNode, CodegenBudgetExceeded })![]const u8 {
            return switch (expression.type) {
                .text => self.allocator.dupe(u8, "capnpc.generic.Text"),
                .data => self.allocator.dupe(u8, "capnpc.generic.Data"),
                .any_pointer => blk: {
                    if (expression.metadata != .any_pointer) return self.allocator.dupe(u8, "capnpc.generic.AnyPointer");
                    if (expression.metadata.any_pointer == .unconstrained) return self.allocator.dupe(u8, "capnpc.generic.AnyPointer");
                    const name = try parameterName(self, node, expression.metadata.any_pointer);
                    break :blk std.fmt.allocPrint(self.allocator, "@field({s}, \"{s}\")", .{ if (expression.metadata.any_pointer == .implicit_method_parameter and self.generic_method_context != null) "_method_bindings" else "_bindings", name });
                },
                .list => |list| blk: {
                    const metadata = if (expression.metadata == .list) expression.metadata.list.* else schema.TypeMetadata.none;
                    const element = try expressionCodec(self, node, .{ .type = list.element_type.*, .metadata = metadata });
                    defer self.allocator.free(element);
                    break :blk std.fmt.allocPrint(self.allocator, "capnpc.generic.List({s})", .{element});
                },
                .@"enum" => |named| blk: {
                    const name = try rootName(self, named.type_id);
                    defer self.allocator.free(name);
                    break :blk std.fmt.allocPrint(self.allocator, "capnpc.generic.Enum({s})", .{name});
                },
                .void, .bool, .int8, .uint8, .int16, .uint16, .int32, .uint32, .float32, .int64, .uint64, .float64 => std.fmt.allocPrint(self.allocator, "capnpc.generic.Scalar(.{s})", .{@tagName(expression.type)}),
                .@"struct", .interface => blk: {
                    const id = if (expression.type == .@"struct") expression.type.@"struct".type_id else expression.type.interface.type_id;
                    const target = self.getNode(id) orelse return error.InvalidStructNode;
                    const name = try appliedName(self, node, target, if (expression.metadata == .named) expression.metadata.named else .{});
                    defer self.allocator.free(name);
                    if (target.struct_node) |info| break :blk std.fmt.allocPrint(self.allocator, "capnpc.generic.Struct({s}, {}, {})", .{ name, info.data_word_count, info.pointer_count });
                    break :blk std.fmt.allocPrint(self.allocator, "capnpc.generic.Capability({s})", .{name});
                },
            };
        }
        fn applyBegin(self: *G, node: *const schema.Node, writer: anytype) !void {
            var parameters = std.ArrayList(schema.Parameter).empty;
            defer parameters.deinit(self.allocator);
            var scope: ?*const schema.Node = node;
            var depth: usize = 0;
            while (scope) |value| {
                if (depth >= 64) return error.InvalidStructNode;
                depth += 1;
                try parameters.appendSlice(self.allocator, value.parameters);
                scope = owner(self, value);
            }
            try writer.writeAll("pub fn Apply(comptime bindings: anytype) type {\n    _ = &bindings;\n    return @This()._Apply(");
            for (parameters.items, 0..) |parameter, index| {
                if (index != 0) try writer.writeAll(", ");
                try writer.print("@field(bindings, \"{s}\")", .{parameter.name});
            }
            try writer.writeAll(");\n}\nfn _Apply(");
            for (parameters.items, 0..) |_, index| {
                if (index != 0) try writer.writeAll(", ");
                try writer.print("comptime _Parameter{}: type", .{index});
            }
            try writer.writeAll(") type {\n    const _bindings = .{");
            for (parameters.items, 0..) |parameter, index| try writer.print(" .{s} = _Parameter{},", .{ parameter.name, index });
            try writer.writeAll(" };\n    _ = &_bindings;\n");
            for (parameters.items, 0..) |_, index| try writer.print("    capnpc.generic.requirePointer(_Parameter{});\n", .{index});
            try writer.writeAll("    return struct {\n");
        }
        fn pointer(typ: schema.Type) bool {
            return switch (typ) {
                .text, .data, .any_pointer, .@"struct", .interface, .list => true,
                else => false,
            };
        }
        pub fn emitData(self: *G, node: *const schema.Node, writer: anytype) !void {
            if (!needsData(self, node)) return;
            const info = node.struct_node orelse return;
            if (info.is_group) return;
            const raw = try rootName(self, node.id);
            defer self.allocator.free(raw);
            try applyBegin(self, node, writer);
            try writer.print("    const _Data = @This();\n    pub const Raw = {s};\n", .{raw});
            for (info.fields, 0..) |field, index| if (field.slot) |slot| {
                if (!pointer(slot.type) or !resolvableType(self, slot.type)) continue;
                const codec = try expressionCodec(self, node, .{ .type = slot.type, .metadata = slot.type_metadata });
                defer self.allocator.free(codec);
                try writer.print("    const _Field{} = {s};\n    comptime {{ capnpc.generic.requirePointer(_Field{}); }}\n", .{ index, codec, index });
            };
            inline for (.{ false, true }) |builder| {
                try writer.print("    pub const {s} = struct {{\n        inner: message.{s},\n        pub fn wrap(inner: message.{s}) @This() {{ return .{{ .inner = inner }}; }}\n        pub fn raw(self: @This()) Raw.{s} {{ return Raw.{s}.wrap(self.inner); }}\n", .{ if (builder) "Builder" else "Reader", if (builder) "StructBuilder" else "StructReader", if (builder) "StructBuilder" else "StructReader", if (builder) "Builder" else "Reader", if (builder) "Builder" else "Reader" });
                if (builder) try writer.writeAll("        pub fn asReader(self: @This(), storage: *capnpc.generated_helpers.ReaderStorage) !_Data.Reader { try storage.bind(self.inner.builder); return _Data.Reader.wrap(try storage.reader(self.inner)); }\n");
                for (info.fields, 0..) |field, index| {
                    const slot = field.slot orelse continue;
                    // Incomplete schema graphs retain the original erased
                    // accessor via raw(); no typed codec can name that target.
                    if (!resolvableType(self, slot.type)) continue;
                    const name = try types.identToZigTypeName(self.allocator, field.name);
                    defer self.allocator.free(name);
                    if (pointer(slot.type)) {
                        try writer.print("        pub fn get{s}(self: @This()) !_Field{}.{s} {{\n", .{ name, index, if (builder) "Builder" else "Reader" });
                        if (builder) try writer.writeAll("            var raw_value = self.raw();\n            _ = &raw_value;\n") else try writer.writeAll("            const raw_value = self.raw();\n");
                        if (slot.type == .text or slot.type == .data or slot.type == .interface) {
                            try writer.print("            return raw_value.get{s}();\n        }}\n", .{name});
                        } else if (slot.type == .any_pointer) {
                            try writer.print("            return _Field{}.{s}(try raw_value.get{s}());\n        }}\n", .{ index, if (builder) "get" else "read", name });
                        } else {
                            try writer.print("            return _Field{}.wrapRaw{s}(try raw_value.get{s}());\n        }}\n", .{ index, if (builder) "Builder" else "Reader", name });
                        }
                        if (builder) {
                            try writer.print("        pub fn set{s}(self: @This(), value: _Field{}.Reader) !void {{\n", .{ name, index });
                            const raw_pointer = slot.type == .any_pointer and (slot.type_metadata != .any_pointer or slot.type_metadata.any_pointer == .unconstrained);
                            if (raw_pointer) {
                                try writer.print("            var raw_value = self.raw();\n            try raw_value.set{s}(value);\n", .{name});
                            } else {
                                try writer.print("            try _Field{}.set(try self.inner.getAnyPointer({}), value);\n", .{ index, slot.offset });
                                if (field.discriminant_value != 0xffff) try writer.print("            try self.inner.writeU16Strict({}, {});\n", .{ info.discriminant_offset * 2, field.discriminant_value });
                            }
                            try writer.writeAll("        }\n");
                            try writer.print("        pub const init{s} = capnpc.generic.Initializer(_Field{}, _Data.Builder, {}, {}, {}).call;\n", .{ name, index, slot.offset, info.discriminant_offset * 2, field.discriminant_value });
                        }
                    } else {
                        try writer.print("        pub fn get{s}(self: @This()) @typeInfo(@TypeOf(Raw.{s}.get{s})).@\"fn\".return_type.? {{ return self.raw().get{s}(); }}\n", .{ name, if (builder) "Builder" else "Reader", name, name });
                        if (builder) try writer.print("        pub fn set{s}(self: @This(), value: @typeInfo(@TypeOf(Raw.Builder.set{s})).@\"fn\".param_types[1].?) !void {{ var raw_value = self.raw(); try raw_value.set{s}(value); }}\n", .{ name, name, name });
                    }
                }
                try writer.writeAll("    };\n");
            }
            try writer.writeAll("    pub const Pipeline = struct {\n        peer: *@import(\"capnpc-zig\").rpc.peer.Peer,\n        question_id: u32,\n        pointer_indexes: [64]u16 = undefined,\n        pointer_count: u8 = 0,\n");
            for (info.fields, 0..) |field, index| {
                const slot = field.slot orelse continue;
                if (!resolvableType(self, slot.type)) continue;
                if (field.discriminant_value != 0xffff or !pointer(slot.type)) continue;
                if (slot.type != .any_pointer and slot.type != .@"struct" and slot.type != .interface) continue;
                const name = try types.identToZigTypeName(self.allocator, field.name);
                defer self.allocator.free(name);
                try writer.print("        pub fn get{s}(self: @This()) !_Field{}.Pipeline {{\n            if (self.pointer_count >= 64) return error.PipelineDepthLimit;\n            var path = self;\n            path.pointer_indexes[path.pointer_count] = {};\n            path.pointer_count += 1;\n            return _Field{}.pipeline(path);\n        }}\n", .{ name, index, slot.offset, index });
            }
            try writer.writeAll("    };\n");
            try writer.writeAll("    };\n}\n");
        }
        fn resolvedCodec(self: *G, context: *const schema.Node, resolver: *const resolution.Resolver, cursor: resolution.Cursor) (std.mem.Allocator.Error || error{ InvalidStructNode, CodegenBudgetExceeded })![]const u8 {
            const resolved = resolver.resolve(cursor) catch return error.InvalidStructNode;
            const expression = resolved.cursor.expression;
            if (expression.type != .@"struct" and expression.type != .interface) return expressionCodec(self, context, expression);
            const id = if (expression.type == .@"struct") expression.type.@"struct".type_id else expression.type.interface.type_id;
            const target = self.getNode(id) orelse return error.InvalidStructNode;
            const child = resolver.enterNamed(id, resolution.Resolver.namedBrand(expression) catch return error.InvalidStructNode, resolved.cursor.context_depth) catch return error.InvalidStructNode;
            const name = try resolvedName(self, context, target, &child);
            defer self.allocator.free(name);
            if (target.struct_node) |info| return std.fmt.allocPrint(self.allocator, "capnpc.generic.Struct({s}, {}, {})", .{ name, info.data_word_count, info.pointer_count });
            return std.fmt.allocPrint(self.allocator, "capnpc.generic.Capability({s})", .{name});
        }
        fn resolvedName(self: *G, context: *const schema.Node, target: *const schema.Node, resolver: *const resolution.Resolver) (std.mem.Allocator.Error || error{ InvalidStructNode, CodegenBudgetExceeded })![]const u8 {
            const raw = try rootName(self, target.id);
            defer self.allocator.free(raw);
            if (target.kind != .interface and !needsData(self, target)) return self.allocator.dupe(u8, raw);
            var text = std.ArrayList(u8).empty;
            defer text.deinit(self.allocator);
            const writer = @import("generator.zig").ArrayListWriter{ .list = &text, .allocator = self.allocator, .max_bytes = self.codegen_budget.max_output_bytes };
            try writer.print("{s}.Apply(.{{", .{raw});
            var scope: ?*const schema.Node = target;
            var depth: usize = 0;
            while (scope) |value| {
                if (depth >= 64) return error.InvalidStructNode;
                depth += 1;
                for (value.parameters, 0..) |parameter, index| {
                    const expression = schema.TypeExpression{ .type = .any_pointer, .metadata = .{ .any_pointer = .{ .parameter = .{ .scope_id = value.id, .parameter_index = @intCast(index) } } } };
                    const codec = try resolvedCodec(self, context, resolver, resolver.cursor(expression));
                    defer self.allocator.free(codec);
                    try writer.print(" .{s} = {s},", .{ parameter.name, codec });
                }
                scope = owner(self, value);
            }
            try writer.writeAll(" })");
            return text.toOwnedSlice(self.allocator);
        }
        const MethodEntry = struct { method: schema.Method, name: []const u8, raw_name: []const u8, typed_name: ?[]const u8 = null, ambiguous: bool = false };
        fn duplicateMethod(entries: []const MethodEntry, name: []const u8) bool {
            for (entries) |entry| if (std.mem.eql(u8, entry.name, name)) return true;
            return false;
        }
        pub fn emitInterface(self: *G, node: *const schema.Node, writer: anytype) !void {
            const info = node.interface_node orelse return;
            const raw = try rootName(self, node.id);
            defer self.allocator.free(raw);
            const ancestors = try self.collectAncestors(node);
            defer self.freeAncestors(ancestors);
            const old_interface = self.interface_context;
            const old_ancestors = self.interface_ancestors;
            self.interface_context = node;
            self.interface_ancestors = ancestors;
            defer {
                self.interface_context = old_interface;
                self.interface_ancestors = old_ancestors;
            }
            const branded_ancestors = applications.interfaceAncestors(self.allocator, self.nodes, node, self.codegen_budget.max_brand_specializations) catch |err| switch (err) {
                error.InvalidSchema => return error.InvalidStructNode,
                error.OutOfMemory => return error.OutOfMemory,
                error.CodegenBudgetExceeded => return error.CodegenBudgetExceeded,
            };
            defer self.allocator.free(branded_ancestors);
            var methods = std.ArrayList(MethodEntry).empty;
            defer {
                for (methods.items) |entry| {
                    self.allocator.free(entry.name);
                    self.allocator.free(entry.raw_name);
                    if (entry.typed_name) |name| self.allocator.free(name);
                }
                methods.deinit(self.allocator);
            }
            for (info.methods) |method| {
                const name = try self.toZigIdentifier(method.name);
                errdefer self.allocator.free(name);
                const raw_name = try std.fmt.allocPrint(self.allocator, "Raw.{s}", .{name});
                errdefer self.allocator.free(raw_name);
                try methods.append(self.allocator, .{ .method = method, .name = name, .raw_name = raw_name });
            }
            try applyBegin(self, node, writer);
            try writer.writeAll("    const _Applied = @This();\n");
            try writer.print("    pub const Raw = {s};\n    pub const interface_id = Raw.interface_id;\n", .{raw});
            for (branded_ancestors, 0..) |*ancestor, index| {
                const name = try resolvedName(self, node, ancestor.target, &ancestor.resolver);
                defer self.allocator.free(name);
                const qualified = try self.qualifiedTypeName(ancestor.target.id);
                defer self.allocator.free(qualified);
                try writer.print("    const _Ancestor{} = {s};\n", .{ index, name });
                for (ancestor.target.interface_node.?.methods) |method| {
                    const member = try self.allocInterfaceMemberName(method.name, qualified);
                    errdefer self.allocator.free(member);
                    const method_name = try self.toZigIdentifier(method.name);
                    defer self.allocator.free(method_name);
                    const raw_name = try std.fmt.allocPrint(self.allocator, "_Ancestor{}.Raw.{s}", .{ index, method_name });
                    errdefer self.allocator.free(raw_name);
                    const typed_name = try std.fmt.allocPrint(self.allocator, "_Ancestor{}.{s}", .{ index, method_name });
                    errdefer self.allocator.free(typed_name);
                    try methods.append(self.allocator, .{ .method = method, .name = member, .raw_name = raw_name, .typed_name = typed_name });
                }
            }
            for (methods.items, 0..) |*entry, index| {
                for (methods.items[0..index]) |*previous| {
                    if (std.mem.eql(u8, entry.name, previous.name)) {
                        entry.ambiguous = true;
                        previous.ambiguous = true;
                    }
                }
            }
            for (methods.items) |entry| {
                if (entry.ambiguous) continue;
                const method = entry.method;
                if (entry.typed_name) |name| {
                    try writer.print("    pub const {s} = {s};\n", .{ entry.name, name });
                    continue;
                }
                const old_method_context = self.generic_method_context;
                self.generic_method_context = method;
                defer self.generic_method_context = old_method_context;
                const pn = self.getNode(method.param_struct_type) orelse return error.InvalidStructNode;
                const rn = self.getNode(method.result_struct_type) orelse return error.InvalidStructNode;
                const params = try appliedName(self, pn, pn, method.param_brand);
                defer self.allocator.free(params);
                const results = try appliedName(self, rn, rn, method.result_brand);
                defer self.allocator.free(results);
                if (method.implicit_parameters.len > 0) {
                    try writer.print("    pub const {s} = struct {{ pub fn Apply(comptime _method_bindings: anytype) type {{ return capnpc.generic.Method({s}, {s}, {s}); }} }};\n", .{ entry.name, entry.raw_name, params, results });
                } else try writer.print("    pub const {s} = capnpc.generic.Method({s}, {s}, {s});\n", .{ entry.name, entry.raw_name, params, results });
            }
            inline for (.{ false, true }) |pipelined| {
                const cname = if (pipelined) "PipelinedClient" else "Client";
                try writer.print("    pub const {s} = struct {{\n        raw: Raw.{s},\n", .{ cname, cname });
                if (!pipelined) try writer.writeAll("        pub fn init(peer: *rpc.peer.Peer, cap_id: u32) @This() { return .{ .raw = Raw.Client.init(peer, cap_id) }; }\n        pub fn release(self: @This()) void { self.raw.release(); }\n");
                if (!pipelined and branded_ancestors.len > 0) {
                    try writer.writeAll("        pub fn asAncestor(self: @This(), comptime Ancestor: type) Ancestor.Client {\n            if (comptime !(false");
                    for (branded_ancestors, 0..) |_, index| try writer.print(" or Ancestor == _Ancestor{}", .{index});
                    try writer.writeAll(")) @compileError(\"requested type is not an ancestor application\");\n            return Ancestor.Client.init(self.raw.peer, self.raw.cap_id);\n        }\n");
                }
                for (methods.items) |entry| {
                    if (entry.ambiguous) continue;
                    const method_type = if (entry.method.implicit_parameters.len > 0)
                        try std.fmt.allocPrint(self.allocator, "_Applied.{s}.Apply(_method_bindings)", .{entry.name})
                    else
                        try std.fmt.allocPrint(self.allocator, "_Applied.{s}", .{entry.name});
                    defer self.allocator.free(method_type);
                    try writer.print("        pub fn call{s}(self: @This(), {s}ctx: *anyopaque, comptime build: ?{s}.BuildFn, comptime callback: {s}.Callback) !u32 {{\n            const Adapter = {s}.ClientAdapter(build, callback);\n            return self.raw.call{s}(ctx, if (build != null) Adapter.build else null, Adapter.callback);\n        }}\n", .{ entry.name, if (entry.method.implicit_parameters.len > 0) "comptime _method_bindings: anytype, " else "", method_type, method_type, method_type, entry.name });
                    const result_node = self.getNode(entry.method.result_struct_type) orelse return error.InvalidStructNode;
                    if (!pipelined and needsData(self, result_node)) try writer.print("        pub fn call{s}Pipelined(self: @This(), {s}ctx: *anyopaque, comptime build: ?{s}.BuildFn, comptime callback: {s}.Callback) !{s}.Results.Pipeline {{\n            const qid = try self.call{s}({s}ctx, build, callback);\n            return .{{ .peer = self.raw.peer, .question_id = qid }};\n        }}\n", .{ entry.name, if (entry.method.implicit_parameters.len > 0) "comptime _method_bindings: anytype, " else "", method_type, method_type, method_type, entry.name, if (entry.method.implicit_parameters.len > 0) "_method_bindings, " else "" });
                }
                try writer.writeAll("    };\n");
            }
            try writer.writeAll("    pub fn ServerAdapter(comptime handlers: anytype) type {\n        _ = &handlers;\n        return struct {\n            raw: Raw.Server,\n            pub fn init(ctx: *anyopaque) @This() { return .{ .raw = .{ .ctx = ctx, .vtable = .{\n");
            for (methods.items, 0..) |entry, index| {
                if (duplicateMethod(methods.items[0..index], entry.name)) continue;
                const field = try self.lowerFirst(entry.name);
                defer self.allocator.free(field);
                const escaped_field = try types.escapeZigKeyword(self.allocator, field);
                defer self.allocator.free(escaped_field);
                if (entry.ambiguous or entry.method.implicit_parameters.len > 0) {
                    try writer.print("                .{s} = if (@hasField(@TypeOf(handlers), \"{s}\")) handlers.{s} else unsupported{s},\n", .{ escaped_field, field, escaped_field, entry.name });
                } else try writer.print("                .{s} = if (@hasField(@TypeOf(handlers), \"{s}\")) _Applied.{s}.ServerAdapter(handlers.{s}).handle else unsupported{s},\n", .{ escaped_field, field, entry.name, escaped_field, entry.name });
            }
            try writer.writeAll("            } } }; }\n            pub fn exportServer(self: *@This(), peer: *rpc.peer.Peer) !u32 { return Raw.exportServer(peer, &self.raw); }\n");
            for (methods.items, 0..) |entry, index| {
                if (duplicateMethod(methods.items[0..index], entry.name)) continue;
                if (entry.method.isStreaming()) {
                    try writer.print("            fn unsupported{s}(_: *anyopaque, _: *rpc.peer.Peer, _: {s}.Params.Reader, _: *const rpc.caps.table.InboundCapTable) anyerror!void {{ return error.Unimplemented; }}\n", .{ entry.name, entry.raw_name });
                } else try writer.print("            fn unsupported{s}(_: *anyopaque, _: *rpc.peer.Peer, _: {s}.Params.Reader, _: *{s}.Results.Builder, _: *const rpc.caps.table.InboundCapTable) anyerror!void {{ return error.Unimplemented; }}\n", .{ entry.name, entry.raw_name, entry.raw_name });
            }
            try writer.writeAll("        };\n    }\n    };\n}\n");
        }
    };
}
