//! Binary schema descriptors, brand-aware reflection, and dynamic wire access.
//! Registries own descriptor memory. Schema and dynamic views borrow their
//! registry and message; keep both owners alive until the last view is used.
const registry = @import("registry.zig");
pub const SchemaRef = registry.SchemaRef;
pub const Registry = registry.Registry;
pub const Schema = registry.Schema;
pub const StructSchema = registry.StructSchema;
pub const EnumSchema = registry.EnumSchema;
pub const InterfaceSchema = registry.InterfaceSchema;
pub const Field = registry.Field;
pub const Method = registry.Method;
pub const Type = registry.Type;
const dynamic = @import("dynamic.zig");
pub const DynamicStruct = dynamic.DynamicStruct;
pub const DynamicList = dynamic.DynamicList;
pub const DynamicEnum = dynamic.Enum;
pub const Value = dynamic.Value;
