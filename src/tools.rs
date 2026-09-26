//! The desktop-provided override extension (`tools.ts`): re-registers
//! `ask` / `sudo_exec` / `manage_todo_list` with the SAME name + schema as
//! the suite's versions, but `execute()` = "send the params to the desktop
//! over the bridge, await the result, return it". The extension is
//! SELF-GATED on the platform (Linux only — the desktop's bridge listener
//! only exists on Linux) + the four `PI_ARCHIMEDES_BRIDGE_*` env vars the
//! bridge setup already sets, so it is inert on a bridge-less spawn.
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
/// as-is).
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
}
