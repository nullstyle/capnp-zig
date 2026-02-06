fn main() {
    let schema_dir = [
        std::path::Path::new("../schemas"),
        std::path::Path::new("schemas"),
        std::path::Path::new("tests/e2e/schemas"),
    ]
    .iter()
    .copied()
    .find(|path| path.join("game_world.capnp").exists())
    .expect("failed to locate e2e schema directory");

    capnpc::CompilerCommand::new()
        .src_prefix(schema_dir)
        .file(schema_dir.join("game_types.capnp"))
        .file(schema_dir.join("game_world.capnp"))
        .file(schema_dir.join("chat.capnp"))
        .file(schema_dir.join("inventory.capnp"))
        .file(schema_dir.join("matchmaking.capnp"))
        .run()
        .expect("failed to compile Cap'n Proto schemas");
}
