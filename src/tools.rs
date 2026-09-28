//! The desktop-provided override extension (`tools.ts`): re-registers the
//! SEVEN built-ins (`bash` / `read` / `write` / `edit` / `find` / `grep` /
//! `ls`, Phase 1) + `ask` / `sudo_exec` / `manage_todo_list` (Phase 2) with
//! the SAME name + schema as the originals, but `execute()` = a bridge
//! round-trip (the desktop executes the delegated tool itself). The
//! extension is SELF-GATED on the platform (Linux only — the desktop's
//! bridge listener only exists on Linux) + the four
//! `PI_ARCHIMEDES_BRIDGE_*` env vars the bridge setup already sets, so it
//! is inert on a bridge-less spawn. On a Linux bridge spawn the desktop
//! ALSO spawns pi with `--no-builtin-tools` ([`spawn_args`]) so the
//! override's built-ins win (first-wins merge); off-Linux the override is
//! inert AND the flag is absent → pi's built-ins remain (no regression).
//!
//! The extension source (a single self-contained TS file, NO imports from
//! `@pi-archimedes/*`) is embedded in the binary (`include_str!`), written
//! to `config_dir/pi-tools/tools.ts` at startup (idempotent), and injected
//! into every bridge-enabled agent spawn via a SECOND `-e` arg (the
//! desktop already injects `-e <gate>`; this adds `-e <tools>`). There is
//! no `tools_env`: the extension self-gates on the EXISTING bridge env —
//! a no-op env function would be dead weight.
//!
//! Registration is DEFERRED (inside the extension's `session_start`
//! handler): two extensions registering the same tool name at LOAD time
//! is a `process.exit(1)` conflict (`DefaultResourceLoader.
//! addExtensionConflictDiagnostics`); a deferred registration is absent at
//! load-finalization → no conflict. The override still wins (CLI `-e`
//! extensions load FIRST; the tool merge is first-wins).

/// The embedded extension source (`src-tauri/assets/tools.ts`).
pub const TOOLS_SOURCE: &str = include_str!("../../assets/tools.ts");

/// The Rust tool executors (native-agent-harness Task 1) — a submodule of
/// this file (there is deliberately NO `tools/mod.rs`).
pub mod exec;

pub use exec::{execute_tool, ContentBlock, ImageRef, ToolCtx, ToolResult};

/// Write the embedded tools override to `config_dir/pi-tools/tools.ts`
/// (0644; idempotent — skip the write when the file already matches).
/// Returns the path (used for the `-e` arg).
pub fn install_tools_extension(
    config_dir: &std::path::Path,
) -> std::io::Result<std::path::PathBuf> {
    let dir = config_dir.join("pi-tools");
    std::fs::create_dir_all(&dir)?;
    let path = dir.join("tools.ts");
    // Idempotent: skip the write when the file already matches (no mtime
    // churn on restart).
    if path.exists() && std::fs::read_to_string(&path).is_ok_and(|c| c == TOOLS_SOURCE) {
        return Ok(path);
    }
    std::fs::write(&path, TOOLS_SOURCE)?;
    // 0644 (world-readable, owner-writable — the agent child reads it).
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))?;
    }
    Ok(path)
}

/// The spawn-args additions for a tools-override agent: `["-e", <tools
/// path>]` appended (a `None` path is a no-op — the args are returned
/// as-is). Deliberately does NOT know about `--no-builtin-tools` (it has
/// no `bridge_active_linux` param for it) — that flag lives in the
/// [`spawn_args`] composition below.
pub fn tools_spawn_args(tools_path: Option<&std::path::Path>, args: &[String]) -> Vec<String> {
    match tools_path {
        Some(path) => {
            let mut out = args.to_vec();
            out.push("-e".to_string());
            out.push(path.to_string_lossy().into_owned());
            out
        }
        None => args.to_vec(),
    }
}

/// The COMPOSED spawn-args additions (gate + tools + `--no-builtin-tools`)
/// — one testable pure function (the args were assembled inline at the call
/// sites, unobservable; this factors the whole composition). `base_args` is
/// the spawn's base (the entry's args for a session spawn — `entry.args`
/// for `start_session`, `entry.args` + `--session <file>` for `resume_session`
/// — the `subagent_pi_args` launch args for a subagent dispatch), so the
/// same composition serves all three call sites without changing their base.
///
/// `bridge_active_linux` = the condition under which the `tools.ts` override
/// will ACTUALLY register the built-ins = `bridge_setup.is_some() &&
/// cfg!(target_os = "linux")` (the override is self-gated on the platform;
/// on Windows the bridge env is set but the override is inert at
/// `tools.ts` — keying the flag on `tools_path` instead would strip pi's
/// built-ins with nothing to replace them → zero tools, the exact
/// regression the `tools.ts` header warns about). When the override will
/// register the built-ins, `--no-builtin-tools` is appended (pi's built-in
/// execution is disabled and the override wins — first-wins merge); the
/// `-e <tools>` flag is appended whenever `tools_path` is `Some` (the suite
/// delegates + the gate still load; the override self-gates at runtime).
pub fn spawn_args(
    base_args: &[String],
    gate_path: Option<&std::path::Path>,
    tools_path: Option<&std::path::Path>,
    bridge_active_linux: bool,
) -> Vec<String> {
    let mut args = crate::agent::gate::gate_spawn_args(gate_path, base_args);
    args = tools_spawn_args(tools_path, &args);
    if tools_path.is_some() && bridge_active_linux {
        args.push("--no-builtin-tools".to_string());
    }
    args
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("tools-{}-{}", tag, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn install_tools_extension_writes_file() {
        let dir = temp_dir("write");
        let path = install_tools_extension(&dir).expect("install");
        assert_eq!(path, dir.join("pi-tools").join("tools.ts"));
        assert!(path.exists(), "the tools file is written");
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            TOOLS_SOURCE,
            "the content is the embedded source"
        );
        // 0644
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o644, "the mode is 0644");
        }
        // Idempotent: a second install does not rewrite (mtime unchanged).
        let mtime1 = std::fs::metadata(&path).unwrap().modified().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
        let path2 = install_tools_extension(&dir).expect("reinstall");
        let mtime2 = std::fs::metadata(&path2).unwrap().modified().unwrap();
        assert_eq!(mtime1, mtime2, "an idempotent install does not rewrite");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn install_tools_extension_rewrites_a_stale_file() {
        let dir = temp_dir("stale");
        let path = install_tools_extension(&dir).unwrap();
        std::fs::write(&path, "stale content").unwrap();
        let path2 = install_tools_extension(&dir).unwrap();
        assert_eq!(
            std::fs::read_to_string(&path2).unwrap(),
            TOOLS_SOURCE,
            "a stale file is rewritten"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn tools_spawn_args_appends_e_flag() {
        let p = PathBuf::from("/tmp/pi-tools/tools.ts");
        let args = tools_spawn_args(Some(&p), &["--mode".to_string(), "rpc".to_string()]);
        assert_eq!(
            args,
            vec!["--mode", "rpc", "-e", "/tmp/pi-tools/tools.ts"],
            "the -e flag + path are appended"
        );
        let args = tools_spawn_args(None, &["--mode".to_string()]);
        assert_eq!(args, vec!["--mode"], "a None tools path is a no-op");
    }

    // ── `spawn_args` (the composed gate + tools + `--no-builtin-tools`
    // composition — the Phase 1 seam; the args are assembled inline at the
    // call sites, so the composition is unobservable without this helper) ──

    #[test]
    fn spawn_args_appends_no_builtin_tools_on_a_linux_bridge_spawn() {
        // Linux + bridge active + tools override present: the built-ins are
        // re-registered by the override, so pi's built-ins are stripped
        // (`--no-builtin-tools`) — the override wins (first-wins merge).
        let gate = PathBuf::from("/tmp/pi-gate/gate.ts");
        let tools = PathBuf::from("/tmp/pi-tools/tools.ts");
        let args = spawn_args(
            &["--mode".to_string(), "rpc".to_string()],
            Some(gate.as_path()),
            Some(tools.as_path()),
            true,
        );
        assert_eq!(
            args,
            vec![
                "--mode",
                "rpc",
                "-e",
                "/tmp/pi-gate/gate.ts",
                "-e",
                "/tmp/pi-tools/tools.ts",
                "--no-builtin-tools"
            ],
            "the gate + tools -e flags + --no-builtin-tools"
        );
    }

    #[test]
    fn spawn_args_omits_no_builtin_tools_when_the_override_is_not_active() {
        // `tools_path` is Some for EVERY spawn (every platform), but the
        // override only registers the built-ins on a Linux bridge spawn —
        // off-Linux (or bridge-less) `--no-builtin-tools` would strip pi's
        // built-ins with nothing to replace them (zero tools — the exact
        // regression the tools.ts header warns about).
        let gate = PathBuf::from("/tmp/pi-gate/gate.ts");
        let tools = PathBuf::from("/tmp/pi-tools/tools.ts");
        let args = spawn_args(
            &["--mode".to_string(), "rpc".to_string()],
            Some(gate.as_path()),
            Some(tools.as_path()),
            false,
        );
        assert_eq!(
            args,
            vec![
                "--mode",
                "rpc",
                "-e",
                "/tmp/pi-gate/gate.ts",
                "-e",
                "/tmp/pi-tools/tools.ts"
            ],
            "the -e flags WITHOUT --no-builtin-tools (the Windows/macOS guard)"
        );
    }

    #[test]
    fn spawn_args_with_no_tools_path_has_no_tools_flags() {
        // `tools_path` None → no `-e <tools>` AND no `--no-builtin-tools`
        // (the built-ins are not overridden, so they must not be stripped).
        let gate = PathBuf::from("/tmp/pi-gate/gate.ts");
        let args = spawn_args(
            &["--mode".to_string(), "rpc".to_string()],
            Some(gate.as_path()),
            None,
            true,
        );
        assert_eq!(
            args,
            vec!["--mode", "rpc", "-e", "/tmp/pi-gate/gate.ts"],
            "no tools flags at all"
        );
    }
}
