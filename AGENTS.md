# Agent Guidance

Start with `docs/repo-navigation.md` for the current workspace map, architecture/data-flow mental model, task routing, and command/CI anchors. `docs/development.md` has the build and test commands.

## Operating Notes

- Prefer updating tool-agnostic docs in `docs/` when capturing durable repository knowledge.
- Keep documentation aligned with actual workspace paths and manifests.
- The repository is Rust-only: add no Python, and nothing may link DataFusion (CI checks both).
- The workspace has five crates (`docs/repo-navigation.md`, "Workspace Layout"); `reference/` is `fireparq hypercore-reference`, whose `hl_*` tables live in their own root, never a fireparq dataset root (`docs/hypercore-reference.md`).
- Regenerate, never hand-edit, `docs/schemas/*.md` (`cargo run -p blocks --example dump_schemas`) and `firehose-parquet/src/networks_generated.rs` (`docs/network-registry-integration.md`).
- Treat `target/`, Cargo `OUT_DIR` protobuf outputs, and `output/` as generated/runtime artifacts, not source.
