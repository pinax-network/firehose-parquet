//! #637: help, the version and completions of the real binary work whatever
//! the env file holds, while every other command still fails, naming the file,
//! when its env file cannot be loaded (#617).
use std::path::Path;
use std::process::{Command, Output};

/// A line without `=` that also looks like a secret, which no output may echo.
const MALFORMED: &str = "LOG_LEVEL=info\nthis-line-is-a-sekrit-token\n";

/// The binary in `dir`, with a cleared environment plus `env`.
fn fireparq(dir: &Path, args: &[&str], env: &[(&str, &str)]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_fireparq"))
        .args(args)
        .current_dir(dir)
        .env_clear()
        .envs(env.iter().copied())
        .output()
        .unwrap()
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// Commands that only display something, with what their stdout must hold.
const DISPLAY_ONLY: &[(&[&str], &str)] = &[
    (&["--help"], "Usage: fireparq"),
    (&["-h"], "Usage: fireparq"),
    (&["--version"], env!("CARGO_PKG_VERSION")),
    (&["-V"], env!("CARGO_PKG_VERSION")),
    (&["help", "build"], "Usage: fireparq build"),
    (&["build", "--help"], "Usage: fireparq build"),
    (
        &["recovery", "status", "--help"],
        "Usage: fireparq recovery status",
    ),
    (&["completions", "zsh"], "#compdef fireparq"),
];

/// A real command: it fails on the env file before it reads anything else.
const BUILD: &[&str] = &[
    "build",
    "--endpoint",
    "http://127.0.0.1:9",
    "--block-type",
    "evm",
    "--output",
    "./out",
];

#[test]
fn help_version_and_completions_ignore_an_unloadable_env_file() {
    let clean = tempfile::tempdir().unwrap();
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join(".env"), MALFORMED).unwrap();
    let env_file = dir.path().canonicalize().unwrap().join(".env");

    for (args, expected) in DISPLAY_ONLY {
        let output = fireparq(dir.path(), args, &[]);
        let (stdout, stderr) = (text(&output.stdout), text(&output.stderr));
        assert!(output.status.success(), "{args:?}: {stdout}{stderr}");
        assert!(stdout.contains(expected), "{args:?}: {stdout}");
        assert!(!stdout.contains("sekrit") && !stderr.contains("sekrit"));
        // The same output as without any env file.
        assert_eq!(
            output.stdout,
            fireparq(clean.path(), args, &[]).stdout,
            "{args:?}"
        );
        if args[0] == "completions" {
            assert_eq!(stderr, "", "a completion script stays silent");
        } else {
            assert!(stderr.contains("warning: invalid env file"), "{stderr}");
            assert!(stderr.contains(&env_file.display().to_string()), "{stderr}");
        }
    }

    // Every other command still fails on the file, naming it, before it
    // writes anything.
    for args in [BUILD, &["maintenance"], &["recovery", "status", "./out"]] {
        let output = fireparq(dir.path(), args, &[]);
        let stderr = text(&output.stderr);
        assert!(!output.status.success(), "{args:?}: {stderr}");
        assert!(stderr.contains("invalid env file"), "{args:?}: {stderr}");
        assert!(stderr.contains(&env_file.display().to_string()), "{stderr}");
        assert!(!stderr.contains("sekrit"), "{stderr}");
        assert!(output.stdout.is_empty(), "{args:?}");
        assert!(!dir.path().join("out").exists(), "{args:?}");
    }
}

/// A missing `--env-file` or `FIREPARQ_ENV_FILE` path: help, the version and
/// completions still run, every other command fails naming the path.
#[test]
fn a_missing_explicit_env_file_only_stops_real_commands() {
    let dir = tempfile::tempdir().unwrap();
    // Neither selector may fall back to ./.env.
    std::fs::write(dir.path().join(".env"), "LOG_LEVEL=info\n").unwrap();
    let missing = dir.path().join("missing.env");
    let missing = missing.to_str().unwrap();

    let by_flag = |args: &[&str]| {
        let mut argv = vec!["--env-file", missing];
        argv.extend_from_slice(args);
        fireparq(dir.path(), &argv, &[])
    };
    let by_variable = |args: &[&str]| fireparq(dir.path(), args, &[("FIREPARQ_ENV_FILE", missing)]);

    for run in [&by_flag as &dyn Fn(&[&str]) -> Output, &by_variable] {
        for (args, expected) in DISPLAY_ONLY {
            let output = run(args);
            let stdout = text(&output.stdout);
            assert!(
                output.status.success(),
                "{args:?}: {}",
                text(&output.stderr)
            );
            assert!(stdout.contains(expected), "{args:?}: {stdout}");
        }
        let output = run(BUILD);
        let stderr = text(&output.stderr);
        assert!(!output.status.success(), "{stderr}");
        assert!(stderr.contains("cannot read env file"), "{stderr}");
        assert!(stderr.contains("missing.env"), "{stderr}");
        assert!(!dir.path().join("out").exists());
    }
}
