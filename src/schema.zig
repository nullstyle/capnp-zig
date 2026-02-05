const std = @import("std");

/// Represents a 64-bit ID used throughout Cap'n Proto schemas
pub const Id = u64;

/// Element size enumeration for lists
pub const ElementSize = enum(u16) {
    empty = 0,
    bit = 1,
    byte = 2,
    two_bytes = 3,
    four_bytes = 4,
    eight_bytes = 5,
    pointer = 6,
    inline_composite = 7,
};

/// Type information
pub const Type = union(enum) {
    void: void,
    bool: void,
    int8: void,
    int16: void,
    int32: void,
    int64: void,
    uint8: void,
    uint16: void,
    uint32: void,
    uint64: void,
    float32: void,
    float64: void,
    text: void,
    data: void,
    list: struct {
        element_type: *Type,
    },
    @"enum": struct {
        type_id: Id,
    },
    @"struct": struct {
        type_id: Id,
    },
    interface: struct {
        type_id: Id,
    },
    any_pointer: void,
};

/// Value information (for defaults and constants)
pub const Value = union(enum) {
    void: void,
    bool: bool,
    int8: i8,
    int16: i16,
    int32: i32,
    int64: i64,
    uint8: u8,
    uint16: u16,
    uint32: u32,
    uint64: u64,
    float32: f32,
    float64: f64,
    text: []const u8,
    data: []const u8,
    list: PointerValue,
    @"enum": u16,
    @"struct": PointerValue,
    interface: void,
    any_pointer: PointerValue,
};

pub const PointerValue = struct {
    message_bytes: []const u8,
};

/// Field slot information
pub const FieldSlot = struct {
    offset: u32,
    type: Type,
    default_value: ?Value,
};

/// Field group information
pub const FieldGroup = struct {
    type_id: Id,
};

/// Field definition
pub const Field = struct {
    name: []const u8,
    code_order: u16,
    annotations: []AnnotationUse,
    discriminant_value: u16,
    slot: ?FieldSlot,
    group: ?FieldGroup,
};

/// Enumerant definition
pub const Enumerant = struct {
    name: []const u8,
    code_order: u16,
    annotations: []AnnotationUse,
};

/// Method definition
pub const Method = struct {
    name: []const u8,
    code_order: u16,
    param_struct_type: Id,
    result_struct_type: Id,
    annotations: []AnnotationUse,
};

/// Node types
pub const NodeKind = enum {
    file,
    @"struct",
    @"enum",
    interface,
    @"const",
    annotation,
};

/// Struct node information
pub const StructNode = struct {
    data_word_count: u16,
    pointer_count: u16,
    preferred_list_encoding: ElementSize,
    is_group: bool,
    discriminant_count: u16,
    discriminant_offset: u32,
    fields: []Field,
};

/// Enum node information
pub const EnumNode = struct {
    enumerants: []Enumerant,
};

/// Interface node information
pub const InterfaceNode = struct {
    methods: []Method,
};

/// Const node information
pub const ConstNode = struct {
    type: Type,
    value: Value,
};

/// Annotation node information
pub const AnnotationNode = struct {
    type: Type,
    targets_file: bool,
    targets_const: bool,
    targets_enum: bool,
    targets_enumerant: bool,
    targets_struct: bool,
    targets_field: bool,
    targets_union: bool,
    targets_group: bool,
    targets_interface: bool,
    targets_method: bool,
    targets_param: bool,
    targets_annotation: bool,
};

/// Schema node
pub const Node = struct {
    id: Id,
    display_name: []const u8,
    display_name_prefix_length: u32,
    scope_id: Id,
    nested_nodes: []NestedNode,
    annotations: []AnnotationUse,
    kind: NodeKind,
    struct_node: ?StructNode,
    enum_node: ?EnumNode,
    interface_node: ?InterfaceNode,
    const_node: ?ConstNode,
    annotation_node: ?AnnotationNode,

    pub const NestedNode = struct {
        name: []const u8,
        id: Id,
    };
};

/// Annotation use on a declaration or member.
pub const AnnotationUse = struct {
    id: Id,
    value: Value,
};

/// Import information
pub const Import = struct {
    id: Id,
    name: []const u8,
};

/// Requested file information
pub const RequestedFile = struct {
    id: Id,
    filename: []const u8,
    imports: []Import,
};

/// Cap'n Proto version information
pub const CapnpVersion = struct {
    major: u16,
    minor: u8,
    micro: u8,
};

/// Code generator request (main input structure)
pub const CodeGeneratorRequest = struct {
    nodes: []Node,
    requested_files: []RequestedFile,
    capnp_version: ?CapnpVersion,
};
