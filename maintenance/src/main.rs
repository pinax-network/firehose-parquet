//! `fireparq-maintenance`: the Delta maintenance job of a fireparq lake. The
//! settings are environment variables, the output is JSON lines on stdout;
//! see the library docs and docs/delta-maintenance.md.

use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    let env = |name: &str| std::env::var(name).ok();
    let status = fireparq_maintenance::run(&env, &mut std::io::stdout()).await;
    ExitCode::from(u8::try_from(status).unwrap_or(1))
}
