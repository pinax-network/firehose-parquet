//! Offline CLI compatibility snapshot. Prints option definitions, never env values.
use clap::{Command, CommandFactory, Parser};
use firehose_parquet::cli::Commands;
use serde_json::{json, Value};

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

fn visit(command: &Command, prefix: &str, out: &mut Vec<Value>) {
    let path = format!("{prefix} {}", command.get_name());
    let options: Vec<_> = command
        .get_arguments()
        .filter(|arg| arg.get_id().as_str().starts_with("aws_"))
        .map(|arg| {
            json!({
                "id": arg.get_id().as_str(), "long": arg.get_long(),
                "short": arg.get_short(), "env_name": arg.get_env().map(|x| x.to_string_lossy()),
                "action": format!("{:?}", arg.get_action()),
                "defaults": arg.get_default_values().iter().map(|x| x.to_string_lossy()).collect::<Vec<_>>(),
                "required": arg.is_required_set(), "global": arg.is_global_set(),
                "hide_env_values": arg.is_hide_env_values_set(),
                "num_args": arg.get_num_args().map(|x| format!("{x:?}")),
                "value_parser": format!("{:?}", arg.get_value_parser()),
                "requires_equals": arg.is_require_equals_set(),
                "aliases": arg.get_all_aliases(),
            })
        })
        .collect();
    if !options.is_empty() {
        out.push(json!({"command": path.trim(), "options": options}));
    }
    for child in command.get_subcommands() {
        visit(child, &path, out);
    }
}
fn main() {
    let mut command = Cli::command();
    command.build();
    let mut result = Vec::new();
    visit(&command, "", &mut result);
    println!("{}", serde_json::to_string_pretty(&result).unwrap());
}
