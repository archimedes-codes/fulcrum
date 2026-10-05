//! The pure argv / secret helpers for the `sudo_exec` tool: the POSIX-ish
//! shell-word splitter, the `sudo -S` argv builder, the secret scrubber,
//! and the auth-failure signature. No process, no channel — pure functions.

/// A small POSIX-ish shell-word splitter for the `sudo -S` argv
/// construction (the suite's `splitCommandIntoArgv`,
/// `packages/sudo/src/argv-split.ts`): single quotes (fully literal),
/// double quotes (backslash escapes `\"` and `\\`), and backslash escapes
/// outside quotes. Deliberately NO other shell semantics: pipes,
/// redirects, `&&`, env assignments stay in the words and pass through to
/// sudo's argv — never a shell.
fn split_command_into_argv(command: &str) -> Vec<String> {
    let mut args: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut has_word = false;
    let chars: Vec<char> = command.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let ch = chars[i];
        match ch {
            '\'' => {
                // Single quotes: literal until the closing quote.
                i += 1;
                let mut closed = false;
                while i < chars.len() {
                    let inner = chars[i];
                    if inner == '\'' {
                        closed = true;
                        i += 1;
                        break;
                    }
                    current.push(inner);
                    i += 1;
                }
                has_word = true;
                if !closed {
                    break; // unterminated — the rest was literal
                }
            }
            '"' => {
                // Double quotes: backslash only escapes `"` and `\`.
                i += 1;
                let mut closed = false;
                while i < chars.len() {
                    let inner = chars[i];
                    if inner == '\\'
                        && (chars.get(i + 1) == Some(&'"') || chars.get(i + 1) == Some(&'\\'))
                    {
                        current.push(chars[i + 1]);
                        i += 2;
                        continue;
                    }
                    if inner == '"' {
                        closed = true;
                        i += 1;
                        break;
                    }
                    current.push(inner);
                    i += 1;
                }
                has_word = true;
                if !closed {
                    break; // unterminated — the rest was literal
                }
            }
            '\\' => {
                // Outside quotes: escape the next character.
                let next = chars.get(i + 1);
                current.push(next.copied().unwrap_or('\\'));
                i += 2;
                has_word = true;
            }
            ' ' | '\t' | '\n' | '\r' => {
                if has_word {
                    args.push(std::mem::take(&mut current));
                    has_word = false;
                }
                i += 1;
            }
            _ => {
                current.push(ch);
                i += 1;
                has_word = true;
            }
        }
    }
    if has_word {
        args.push(current);
    }
    args
}

/// Build the full argv for `sudo -S <command>` (the suite's
/// `buildSudoArgv`, with one deliberate security-hardening divergence):
/// a proper POSIX-ish word split (quoted args stay single argv entries —
/// never a shell). A stray leading `sudo` is stripped so `sudo -S` is
/// applied exactly once. NO `-p` (the password prompt text is never
/// customized — the auth-failure signature depends on sudo's own stderr).
///
/// The `--` between the sudo options and the command words is the
/// DIVERGENCE (the suite's `buildSudoArgv` shares the hole — it passes
/// the words straight through): without it, a leading-dash word lands in
/// SUDO's own option space — `command: "-u nobody id"` → `sudo -S -u
/// nobody id` (runs as `nobody`, not root), and `command: "-p x id"` → a
/// custom `-p` prompt that DEFEATS `is_sudo_auth_failure` (the `[sudo]
/// password for` precondition never appears in stderr → a wrong password
/// is not detected as an auth failure → the bad credential stays cached
/// for the TTL). With `--`, sudo's options end there and the dash words
/// are the TARGET command's argv (a command named `-u` does not exist →
/// sudo fails cleanly). `--` is a no-op for the normal (no-leading-dash)
/// case (`sudo -S -- ls` ≡ `sudo -S ls`).
pub fn build_sudo_argv(command: &str) -> Vec<String> {
    let mut words = split_command_into_argv(command.trim());
    if words.first().is_some_and(|w| w == "sudo") {
        words.remove(0);
    }
    let mut argv = vec!["sudo".to_string(), "-S".to_string(), "--".to_string()];
    argv.extend(words);
    argv
}

/// Belt-and-braces: drop any line that contains the raw password so it
/// can never appear in tool details/result content (normally stderr is
/// clean — the password never appears in argv — but scrub defensively;
/// applied PER STREAM).
///
/// STRONGER than the suite's `scrubSecret` (a deliberate choice): the
/// suite replaces the secret SUBSTRING within the line; this replaces the
/// ENTIRE line containing the secret with `[redacted]`. The over-redaction
/// is safe (a line that mentions the password carries no useful content
/// worth preserving) and is simpler to reason about.
pub fn scrub_secret(text: &str, secret: &str) -> String {
    if secret.is_empty() {
        return text.to_string();
    }
    text.split('\n')
        .map(|line| {
            if line.contains(secret) {
                "[redacted]"
            } else {
                line
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The suite's EXACT auth-failure signature (`tool.ts:225-228`): the
/// sudo-prompt precondition (`/[sudo] password for/i`) AND the
/// "incorrect password" marker (`/incorrect password/i`) — case-insensitive,
/// BOTH conditions (not an OR on bare substrings, not a "parse error" —
/// there is no "parse error" in the suite).
pub fn is_sudo_auth_failure(stderr: &str) -> bool {
    let lower = stderr.to_lowercase();
    lower.contains("[sudo] password for") && lower.contains("incorrect password")
}

#[cfg(test)]
mod tests {
    use super::{build_sudo_argv, scrub_secret};

    #[test]
    fn build_sudo_argv_splits_quoted_args_and_strips_a_leading_sudo() {
        // `--` separates sudo's options from the command words (the
        // leading-dash hardening — the next test).
        assert_eq!(
            build_sudo_argv("apt install ripgrep"),
            vec!["sudo", "-S", "--", "apt", "install", "ripgrep"]
        );
        // A stray leading `sudo` is stripped so `sudo -S` is applied once.
        assert_eq!(
            build_sudo_argv("sudo apt update"),
            vec!["sudo", "-S", "--", "apt", "update"]
        );
        // Quoted args stay single argv entries (NO `-p`, never a shell).
        assert_eq!(
            build_sudo_argv(r"echo 'hello world'"),
            vec!["sudo", "-S", "--", "echo", "hello world"]
        );
        assert_eq!(
            build_sudo_argv(r#"apt install "ripgrep""#),
            vec!["sudo", "-S", "--", "apt", "install", "ripgrep"]
        );
        // Leading whitespace is trimmed; a lone `sudo` yields `sudo -S --`.
        assert_eq!(
            build_sudo_argv("  ls -la"),
            vec!["sudo", "-S", "--", "ls", "-la"]
        );
        assert_eq!(build_sudo_argv("sudo"), vec!["sudo", "-S", "--"]);
    }

    #[test]
    fn build_sudo_argv_a_leading_dash_word_is_not_a_sudo_flag() {
        // The security hole: without the `--`, a leading-dash word lands in
        // SUDO's own option space — `"-u nobody id"` → `sudo -S -u nobody
        // id` (runs as `nobody`, not root), and `"-p x id"` → a custom `-p`
        // prompt that DEFEATS `is_sudo_auth_failure` (the `[sudo] password
        // for` precondition never appears in stderr → a wrong password is
        // not detected as an auth failure → the bad credential stays cached
        // for the TTL). `--` ends sudo's options: the dash words are the
        // TARGET command's argv (a command named `-u` does not exist → sudo
        // fails cleanly). The suite's `buildSudoArgv` shares the hole —
        // this is a deliberate security-hardening divergence.
        assert_eq!(
            build_sudo_argv("-u nobody id"),
            vec!["sudo", "-S", "--", "-u", "nobody", "id"]
        );
        assert_eq!(
            build_sudo_argv("-p x id"),
            vec!["sudo", "-S", "--", "-p", "x", "id"]
        );
        // The normal (no-leading-dash) case is unchanged in behavior:
        // `sudo -S -- ls -la` ≡ `sudo -S ls -la` (`-la` is `ls`'s arg in
        // both — only a word BEFORE any non-option word was the hole).
        assert_eq!(
            build_sudo_argv("ls -la"),
            vec!["sudo", "-S", "--", "ls", "-la"]
        );
    }

    #[test]
    fn scrub_secret_replaces_any_line_containing_the_secret() {
        assert_eq!(scrub_secret("a\nhunter2\nc", "hunter2"), "a\n[redacted]\nc");
        assert_eq!(scrub_secret("a\nhunter2", ""), "a\nhunter2");
        assert_eq!(scrub_secret("clean", "hunter2"), "clean");
    }
}
