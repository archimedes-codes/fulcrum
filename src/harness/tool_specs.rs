use serde_json::{json, Map, Value};

use crate::agent::harness::provider::{ToolCall, ToolSpec};

/// What the `bash` tool tells the model about the shell it is about to get.
///
/// Two literals, keyed on the SAME predicate `exec_bash` branches on
/// (`unix` / `not(unix)`, not `windows` — keying the sentence and the executor
/// to one predicate is what makes them unable to disagree), because
/// `exec_bash` picks `sh -c` on Unix and `cmd.exe /d /s /c` on Windows. One
/// unconditional sentence is therefore a lie on one of them, and the lie is
/// what made issue #79 expensive: six detours, because the model — and the
/// reader — trusted `NOT re-parsed` on the one platform where it is false twice
/// over (the string IS re-quoted, on the way in, by std's command-line builder).
///
/// ADR 0017's invariant ("the `<tools>` section is byte-identical to the
/// `tools[]` API param") SURVIVES the `cfg`: `model_request`
/// (`harness/loop.rs:911`) and `advertised_specs` (`harness/loop.rs:926`) call
/// the same `advertised_tool_specs`, and `worker/core.rs` renders that same
/// value into the prompt — one process, one function, one target OS, so the two
/// consumers cannot disagree. A resume replays the STORED seq-0 prompt, so an
/// in-flight session keeps the sentence it started with.
///
/// The Unix literal is byte-identical to the single literal this replaced
/// (pinned by `the_bash_description_names_the_shell_this_platform_actually_runs`).
#[cfg(unix)]
const BASH_DESCRIPTION: &str =
    "Run a shell command (sh -c; NOT re-parsed), capturing interleaved stdout+stderr.";

/// The Windows sibling — see [`BASH_DESCRIPTION`]'s doc for why there are two.
///
/// HARD CONSTRAINT on the wording: the expanded string must contain NEITHER the
/// substring `read` NOR the substring `subagent`, because
/// `src-tauri/tests/worker_e2e.rs` (`full_turn_real_worker_canned_provider`)
/// asserts `!tools_section.contains("read") && !tools_section.contains("subagent")`
/// over the rendered `<tools>` section with only `bash` enabled, and that test
/// runs on the Windows CI leg. `the_bash_description_never_names_another_advertised_tool`
/// keeps that constraint visible from Linux. It rules out `already`, `reads`,
/// `reading`, `thread`, `spread` — hence "takes", not "reads".
#[cfg(not(unix))]
const BASH_DESCRIPTION: &str = "Run a shell command (cmd.exe /d /s /c; the string reaches \
    cmd.exe EXACTLY as written, so write quoting the way cmd.exe expects it), capturing \
    interleaved stdout+stderr. Two traps: `start` takes its FIRST quoted token as a window \
    title, so never pass `\"\"` there, and a background launch is verified by the log file \
    appearing — never by the launcher's exit code; `timeout /t` cannot run here (this child \
    has no console input) — wait with `ping -n 20 127.0.0.1 > NUL`.";

/// The tool specs sent to the model (the built-ins + the suite tools —
/// the schemas mirror the `tools.ts` override). `pub(crate)` so the
/// `dispatch_native` driver derives the child's tool set (the names
/// minus `subagent`) from it.
pub(crate) fn tool_specs() -> Vec<ToolSpec> {
    vec![
        ToolSpec {
            name: "bash".into(),
            description: BASH_DESCRIPTION.into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "command": { "type": "string" },
                    "timeout_ms": { "type": "number" }
                },
                "required": ["command"]
            }),
        },
        ToolSpec {
            name: "read".into(),
            description: "Read a file (or an image, as a base64 content block).".into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "offset": { "type": "number" },
                    "limit": { "type": "number" }
                },
                "required": ["path"]
            }),
        },
        ToolSpec {
            name: "write".into(),
            description: "Write a file (creating parent directories).".into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "content": { "type": "string" }
                },
                "required": ["path", "content"]
            }),
        },
        ToolSpec {
            name: "edit".into(),
            description: "Edit a file (exact text replacements).".into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "old_text": { "type": "string" },
                    "new_text": { "type": "string" },
                    "replace_all": { "type": "boolean" }
                },
                "required": ["path", "old_text", "new_text"]
            }),
        },
        ToolSpec {
            name: "find".into(),
            description: "Find files by name (a glob).".into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "pattern": { "type": "string" },
                    "path": { "type": "string" },
                    "max_results": { "type": "number" }
                },
                "required": ["pattern"]
            }),
        },
        ToolSpec {
            name: "grep".into(),
            description: "Search file contents (a regex).".into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "pattern": { "type": "string" },
                    "path": { "type": "string" },
                    "glob": { "type": "string" },
                    "max_results": { "type": "number" },
                    "-i": { "type": "boolean" }
                },
                "required": ["pattern"]
            }),
        },
        ToolSpec {
            name: "ls".into(),
            description: "List a directory.".into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "long": { "type": "boolean" }
                },
                "required": ["path"]
            }),
        },
        ToolSpec {
            name: "list_skills".into(),
            description: "List the model-visible Skills (name, scope, description) — the catalog \
                          the `<skills>` system-prompt section advertises, re-read from disk."
                .into(),
            parameters: json!({ "type": "object" }),
        },
        ToolSpec {
            name: "read_skill".into(),
            description: [
                "Load a Skill's instructions by NAME — the sanctioned way to read a skill file.",
                "",
                "`name` matches a discovered Skill (see list_skills, or the `<available_skills>` \
                section); no `path` returns that skill's `SKILL.md` body. `path` reads a file \
                BUNDLED with that skill, resolved against the skill's directory — it must stay \
                inside it.",
                "",
                "A Skill whose frontmatter sets `disable-model-invocation: true` is model-hidden: \
                it is not advertised and this tool refuses it — ask the user to send it as a \
                `$name` Mention, or to load it explicitly if its name is outside the mention \
                token grammar.",
                "",
                "Skills live OUTSIDE the session `cwd` (`~/.agents/skills`, `.agents/skills`). \
                The skill tools are the sanctioned route: `read_skill` resolves a NAME against \
                the discovered catalog and confines every read to that skill's own directory.",
            ]
            .join("\n"),
            parameters: json!({
                "type": "object",
                "properties": {
                    "name": { "type": "string", "description": "The Skill's name" },
                    "path": {
                        "type": "string",
                        "description": "A file inside the skill's directory (e.g. a referenced ./format.md)"
                    }
                },
                "required": ["name"]
            }),
        },
        ToolSpec {
            name: "ask".into(),
            description: "Ask the user a question (one or more, each with options).".into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "questions": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": { "type": "string" },
                                "question": { "type": "string" },
                                // An option is a bare label string OR an object
                                // with a `label` — because that is what the
                                // parser ACCEPTS, and the app's own model has
                                // been sending the string form all along (80 of
                                // the 395 options in the session store). The
                                // schema used to say `{"type": "object"}`, which
                                // forbade the form the app was silently handling:
                                // a payload the model was told not to send, and
                                // that rendered fine anyway. `description` is
                                // spelled out because 312 recorded options carry
                                // one and the renderer shows it.
                                "options": {
                                    "type": "array",
                                    "items": {
                                        "anyOf": [
                                            { "type": "string" },
                                            {
                                                "type": "object",
                                                "properties": {
                                                    "label": { "type": "string" },
                                                    "description": { "type": "string" }
                                                },
                                                "required": ["label"]
                                            }
                                        ]
                                    }
                                },
                                "multi": { "type": "boolean" }
                            },
                            // `id` stays REQUIRED on purpose: the parser now
                            // synthesizes a positional id when one is missing
                            // (so a missing id can no longer break the card),
                            // but a real id is still what the model should send.
                            "required": ["id", "question", "options"]
                        }
                    }
                },
                "required": ["questions"]
            }),
        },
        ToolSpec {
            name: "sudo_exec".into(),
            description: "Run a command with elevated privileges (confirm + masked password)."
                .into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "command": { "type": "string" },
                    "reason": { "type": "string" },
                    "timeoutMs": { "type": "number" }
                },
                "required": ["command", "reason"]
            }),
        },
        ToolSpec {
            name: "manage_todo_list".into(),
            description: "Manage a structured todo list (write / read).".into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "operation": { "type": "string", "enum": ["write", "read"] },
                    "todoList": { "type": "array" }
                },
                "required": ["operation"]
            }),
        },
        ToolSpec {
            name: "subagent".into(),
            description: "Delegate tasks to subagents. `task` is required. `agentName` names a discovered Agent definition (`list_agents`): its system-prompt body, model, thinking and tools apply, layered under any explicit params. Prefer `agentName` and put extra constraints in `task` — an explicit `systemPrompt` REPLACES the definition's body, it does not extend it. Omit `agentName` for a config-less dispatch (parent model, all tools, no system prompt).".into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "task": { "type": "string" },
                    "agentName": { "type": "string" },
                    "model": { "type": "string" },
                    "thinking": { "type": "string" },
                    "systemPrompt": { "type": "string" },
                    "tools": { "type": "array", "items": { "type": "string" } }
                },
                "required": ["task"]
            }),
        },
        ToolSpec {
            name: "list_agents".into(),
            description: "List available subagent configurations (name, description, source, model/tools overrides). Call before dispatching if unsure which agents exist or which fits the task.".into(),
            parameters: json!({ "type": "object" }),
        },
        ToolSpec {
            name: "mcp".into(),
            description: [
                "Gateway to MCP (Model Context Protocol) servers. Use this tool to discover and call tools from connected MCP servers.",
                "",
                "Workflow:",
                "1. Search: mcp({ search: 'keyword' }) — find available tools",
                "2. Describe: mcp({ describe: 'tool_name' }) — see full parameters",
                "3. Call: mcp({ tool: 'tool_name', args: { ... } }) — execute the tool",
                "",
                "Other actions:",
                "  Status:  mcp({}) or mcp({ action: 'status' }) — list all servers and their connection status",
                "  List:    mcp({ server: 'name' }) — list all tools on a specific server",
                "  Connect: mcp({ connect: 'name' }) — eagerly connect to a server",
                "  Auth:    mcp({ action: 'auth', server: 'name' }) — run the interactive OAuth flow",
                "",
                "Use 'server' to disambiguate when two servers export a tool with the same name.",
            ]
            .join("\n"),
            parameters: json!({
                "type": "object",
                "properties": {
                    "tool": { "type": "string", "description": "Tool name to call" },
                    "args": { "description": "Tool arguments (a JSON string or an object)" },
                    "search": { "type": "string", "description": "Search tools by name/description keyword" },
                    "describe": { "type": "string", "description": "Tool name to show full parameter schema for" },
                    "connect": { "type": "string", "description": "Server name to eagerly connect" },
                    "server": { "type": "string", "description": "Scopes the call to one server (search, describe, that server's tool list, or a tool call), and it must name a server this session holds — an entry the config marks disabled (or a legacy sse / malformed one) is not held, so a name outside that set fails rather than answering 'not found'" },
                    "action": { "type": "string", "description": "Action string ('status' or 'auth')" }
                }
            }),
        },
        ToolSpec {
            name: "web_search".into(),
            description: "Search the web using DuckDuckGo or configured search providers.".into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "Single search query string" },
                    "queries": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Multiple search queries executed concurrently"
                    },
                    "num_results": { "type": "number", "description": "Maximum number of results to return (1-20, default 5)" },
                    // (issue #82, interim) The camelCase aliases the handler
                    // actually accepts (`params.get("num_results").or_else(||
                    // params.get("numResults"))`), plus `proxy`, which the
                    // handler reads and the spec never mentioned. Advertised ==
                    // accepted is what keeps the #78 unknown-param checker from
                    // warning on a legal call; the aliases stay undocumented
                    // behaviour otherwise.
                    "numResults": { "type": "number", "description": "Alias for num_results" },
                    "provider": { "type": "string", "description": "Search provider to use (default: auto)" },
                    "recency_filter": { "type": "string", "description": "Time filter for results" },
                    "recencyFilter": { "type": "string", "description": "Alias for recency_filter" },
                    "domain_filter": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "List of domains to restrict results to"
                    },
                    "domainFilter": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Alias for domain_filter"
                    },
                    "proxy": { "type": "string", "description": "Proxy URL override for this search (defaults to the session proxy)" }
                }
            }),
        },
        ToolSpec {
            name: "fetch_content".into(),
            description: "Fetch web content from a URL and extract readable markdown or raw content.".into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "url": { "type": "string", "description": "URL to fetch and extract content from" },
                    "mode": {
                        "type": "string",
                        "enum": ["readable", "raw"],
                        "description": "Extraction mode: readable (default, converted to Markdown) or raw (original body)"
                    },
                    "max_chars": {
                        "type": "number",
                        "description": "Maximum characters to return in head (default 20,000, clamp 200..=200,000)"
                    },
                    // (issue #82, interim) The camelCase alias the handler
                    // accepts — see the `web_search` note.
                    "maxChars": { "type": "number", "description": "Alias for max_chars" }
                },
                "required": ["url"]
            }),
        },
        ToolSpec {
            name: "get_search_content".into(),
            // (issue #82, round 2) The handle is named in its SNAKE_CASE form —
            // the spelling `required` names, and the spelling every
            // model-visible web sentence now uses
            // (`web::tools::RESPONSE_ID_PARAM`). The camelCase alias stays
            // ADVERTISED below (the handler accepts it, so the #78 checker must
            // not flag it) but is no longer the form the tool tells the model to
            // type: `required: ["response_id"]` is not satisfied by `responseId`,
            // so a description that named the alias documented a call its own
            // schema rejects.
            description: "Get cached search content or full document from a previous fetch/search by response_id.".into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "response_id": { "type": "string", "description": "Response ID returned by web_search or fetch_content" },
                    // (issue #82, interim) `fetch_content`'s own truncation
                    // note tells the model to call this tool with `responseId`,
                    // and the handler accepts the camelCase form — so the alias
                    // must be advertised or the #78 checker flags the call the
                    // tool itself provoked.
                    "responseId": { "type": "string", "description": "Alias for response_id" },
                    "offset": { "type": "number", "description": "Character offset to start reading from (default 0)" },
                    "limit": { "type": "number", "description": "Maximum characters to read (capped at 20,000)" },
                    "find_text": { "type": "string", "description": "Substring to search for in cached content" },
                    "findText": { "type": "string", "description": "Alias for find_text" }
                },
                "required": ["response_id"]
            }),
        },
    ]
}

/// (issue #78) The warning for a tool call that passes a key the model was
/// never told about, or `None` when there is nothing to say.
///
/// WHY IT LIVES HERE: this file owns the advertised shapes, so the known-key
/// set and the shape the model was shown stay in one place. Callers MUST pass
/// `AgentLoop::advertised_specs()` (`harness/loop.rs`) — the SAME value
/// `model_request` sends as `tools[]` and `build_main_prompt` renders into
/// `<tools>` (ADR 0017's byte-identity) — so a check can never disagree with
/// the prompt and there is no second table to drift.
///
/// The rule, in order:
/// 1. No spec for the (alias-normalized) name → NO check. An unknown tool is
///    already a hard error in `dispatch_tool`'s `unknown tool:` arm
///    (`loop/tools.rs`), and a tool the session disabled is not in
///    `advertised_specs()` either — the model was never told it existed.
/// 2. `parameters.properties` absent or not an object → NO check.
///    Load-bearing: `list_agents` / `list_skills` are a bare
///    `{"type":"object"}`, so inferring "nothing is allowed" there would flag
///    EVERY key on EVERY call.
/// 3. Every TOP-LEVEL argument key absent from `properties` is reported.
///    `required` is irrelevant — only `properties` defines legality — and
///    nested keys are never inspected (an `ask` question's `recommended` is
///    legal, unadvertised and nested, so it stays untouched).
/// 4. At most one suggestion, drawn from the `properties` keys ONLY, by
///    case-insensitive containment in either direction — so `agent` →
///    `agentName`. Never suggested from anything but the advertised shape.
///
/// TOP-LEVEL ONLY, and it never mutates: a pure function of `(specs, tc)`
/// returning display text, so it is safe to call before a dispatch has
/// decided anything. Malformed JSON needs no branch — `parse_arguments`
/// (`provider/openai.rs`) already collapses unparseable arguments to `{}`, and
/// a non-object payload simply has no keys.
pub(crate) fn unknown_param_warning(specs: &[ToolSpec], tc: &ToolCall) -> Option<String> {
    // The `dispatch_subagent` alias resolves to the `subagent` spec — the same
    // `effective_tool_name` `is_tool_enabled` gates on, so the alias is checked
    // against the shape the model was actually shown and the message names the
    // canonical tool, not the alias it guessed at.
    let spec = specs
        .iter()
        .find(|s| s.name == crate::agent::harness::dispatch::effective_tool_name(&tc.name))?;
    let properties = spec.parameters.get("properties")?.as_object()?;
    let args = tc.arguments.as_object()?;
    let mut items: Vec<String> = args
        .keys()
        .filter(|key| !properties.contains_key(key.as_str()))
        .map(|key| match suggest(key, properties) {
            Some(hit) => format!("`{key}` (did you mean `{hit}`?)"),
            None => format!("`{key}`"),
        })
        .collect();
    if items.is_empty() {
        return None;
    }
    // `serde_json`'s default `Map` is a `BTreeMap` (`Cargo.toml` never enables
    // `preserve_order`, so `args.keys()` — and `properties.keys()` — iterate in
    // sorted order already). The sort is therefore redundant-but-intentional:
    // it makes the determinism local rather than a dependency on serde_json's
    // map type, which is what lets a test pin the message byte-for-byte.
    items.sort();
    Some(format!(
        "⚠ unknown param(s) for `{}`: {} — ignored.",
        spec.name,
        items.join(", ")
    ))
}

/// The ONE suggested name for `key`: the first advertised property that shares
/// a case-insensitive substring with it in either direction — so `agent` →
/// `agentName` (the advertised name contains the typed one) and a hypothetical
/// `name` → `agentName` too. Searching the advertised keys ONLY is the point: a
/// suggestion the model cannot legally send is worse than none.
///
/// Deliberately separator-blind: `agent_name` gets NO suggestion (the `_`
/// survives `to_lowercase`, so neither string contains the other) — it is still
/// REPORTED, which is the half that matters; normalizing separators would
/// suggest across unrelated names and buys nothing the report does not already
/// say.
fn suggest(key: &str, properties: &Map<String, Value>) -> Option<String> {
    let lower = key.to_lowercase();
    properties
        .keys()
        .find(|name| {
            let l = name.to_lowercase();
            l.contains(&lower) || lower.contains(&l)
        })
        .cloned()
}

#[cfg(test)]
mod tests {
    use serde_json::Value;
    use std::collections::BTreeSet;

    use super::*;

    /// A tool call with the given name + arguments (the checker input).
    fn call(name: &str, args: Value) -> ToolCall {
        ToolCall {
            id: "t1".into(),
            name: name.into(),
            arguments: args,
        }
    }

    fn exec_param_keys() -> [(&'static str, &'static [&'static str]); 12] {
        [
            ("bash", &["command", "timeout_ms"]),
            ("read", &["path", "offset", "limit"]),
            ("write", &["path", "content"]),
            ("edit", &["path", "old_text", "new_text", "replace_all"]),
            ("find", &["pattern", "path", "max_results"]),
            ("grep", &["pattern", "path", "glob", "max_results", "-i"]),
            ("ls", &["path", "long"]),
            ("list_skills", &[]),
            ("read_skill", &["name", "path"]),
            (
                "web_search",
                &[
                    "query",
                    "queries",
                    "num_results",
                    "numResults",
                    "provider",
                    "recency_filter",
                    "recencyFilter",
                    "domain_filter",
                    "domainFilter",
                    "proxy",
                ],
            ),
            ("fetch_content", &["url", "mode", "max_chars", "maxChars"]),
            (
                "get_search_content",
                &[
                    "response_id",
                    "responseId",
                    "offset",
                    "limit",
                    "find_text",
                    "findText",
                ],
            ),
        ]
    }

    #[test]
    fn every_tool_spec_is_in_the_param_key_table() {
        const LOOP_INTERNAL_TOOLS: &[&str] = &[
            "ask",
            "sudo_exec",
            "manage_todo_list",
            "subagent",
            "list_agents",
            "mcp",
        ];
        let param_keys = exec_param_keys();
        for spec in tool_specs() {
            if LOOP_INTERNAL_TOOLS.contains(&spec.name.as_str()) {
                continue;
            }
            assert!(
                param_keys.iter().any(|(name, _)| *name == spec.name),
                "tool `{}` from `tool_specs()` is missing from `exec_param_keys()`",
                spec.name
            );
        }
    }

    #[test]
    fn tool_specs_advertise_exactly_the_executor_param_keys() {
        let specs = tool_specs();
        for (name, keys) in exec_param_keys() {
            let spec = specs
                .iter()
                .find(|s| s.name == name)
                .unwrap_or_else(|| panic!("no `tool_specs` entry for `{name}`"));
            let advertised: BTreeSet<String> = spec
                .parameters
                .get("properties")
                .and_then(Value::as_object)
                .map(|o| o.keys().cloned().collect())
                .unwrap_or_default();
            let expected: BTreeSet<String> = keys.iter().map(|s| s.to_string()).collect();
            assert_eq!(
                advertised, expected,
                "tool `{name}`: advertised {advertised:?} != executor keys {expected:?}"
            );
        }
    }

    // ---------------------------------------------------------------------
    // (issue #78) `unknown_param_warning` — the silently-dropped parameter
    // announces itself in the tool result.
    // ---------------------------------------------------------------------

    /// The exact wire text the model reads on its next turn, pinned
    /// byte-for-byte: the `⚠` (U+26A0, NO variation selector), the `(s)`, the
    /// backticks, the em dash and the trailing `ignored` are all part of the
    /// contract — a drift in any of them is a regression, because the model
    /// learns the habit from this sentence.
    #[test]
    fn an_unknown_param_is_reported_with_a_suggestion() {
        let warning = unknown_param_warning(
            &tool_specs(),
            &call("subagent", json!({ "task": "t", "agent": "reviewer" })),
        );
        assert_eq!(
            warning.as_deref(),
            Some(
                "⚠ unknown param(s) for `subagent`: `agent` (did you mean `agentName`?) — ignored."
            ),
            "the whole sentence is the wire text the model reads"
        );
    }

    /// A key with no substring relation to any advertised property is still
    /// REPORTED, just without a suggestion — the report is the half that
    /// matters. `agent_name` is the realistic second misspelling and pins the
    /// separator-blind decision in `suggest`: the `_` survives `to_lowercase`,
    /// so neither `agent_name` nor `agentName` contains the other. `zzzz` is
    /// the control that proves the `agent_name` half is not passing by accident.
    #[test]
    fn an_unknown_param_without_a_close_match_is_still_reported() {
        let warning = unknown_param_warning(
            &tool_specs(),
            &call("subagent", json!({ "task": "t", "agent_name": "reviewer" })),
        )
        .expect("`agent_name` is not an advertised property, so it is reported");
        assert!(warning.contains("`subagent`"), "{warning}");
        assert!(warning.contains("`agent_name`"), "{warning}");
        assert!(
            !warning.contains("did you mean"),
            "`agent_name` must get NO suggestion (suggest is deliberately \
             separator-blind): {warning}"
        );

        let warning = unknown_param_warning(
            &tool_specs(),
            &call("subagent", json!({ "task": "t", "zzzz": 1 })),
        )
        .expect("`zzzz` is not an advertised property, so it is reported");
        assert!(warning.contains("`zzzz`"), "{warning}");
        assert!(
            !warning.contains("did you mean"),
            "a key with no substring relation at all gets no suggestion: {warning}"
        );
    }

    /// A spec whose `parameters.properties` is absent or not an object is
    /// NEVER checked. Load-bearing: `list_agents` / `list_skills` are a bare
    /// `{"type":"object"}`, so treating "no properties" as "nothing allowed"
    /// would flag EVERY key on EVERY call and turn the warning into noise.
    #[test]
    fn a_spec_with_no_properties_is_never_checked() {
        for bare in ["list_agents", "list_skills"] {
            assert_eq!(
                unknown_param_warning(&tool_specs(), &call(bare, json!({ "anything": 1 }))),
                None,
                "`{bare}` is a bare `{{\"type\":\"object\"}}` — no properties, no check"
            );
        }
        // The "properties exists but is not an object" branch, which no real
        // spec exercises today.
        let odd = vec![ToolSpec {
            name: "odd".into(),
            description: String::new(),
            parameters: json!({ "type": "object", "properties": "not-an-object" }),
        }];
        assert_eq!(
            unknown_param_warning(&odd, &call("odd", json!({ "anything": 1 }))),
            None,
            "a non-object `properties` must disable the check, not enable it"
        );
    }

    /// An unknown TOOL is not checked — `dispatch_tool`'s `unknown tool:` arm
    /// (`loop/tools.rs`) already returns a hard `is_error: true` result, and a
    /// second message from the checker would double-report it.
    #[test]
    fn an_unknown_tool_name_is_never_checked() {
        assert_eq!(
            unknown_param_warning(
                &tool_specs(),
                &call("no_such_tool", json!({ "whatever": 1 }))
            ),
            None
        );
    }

    /// A call that passes only advertised keys is silent (the false-positive
    /// guard), and the `dispatch_subagent` ALIAS is checked against the
    /// `subagent` spec — the `effective_tool_name` lookup is what makes this
    /// non-vacuous, and the message names the canonical tool.
    #[test]
    fn a_well_formed_call_produces_no_warning() {
        assert_eq!(
            unknown_param_warning(
                &tool_specs(),
                &call(
                    "subagent",
                    json!({
                        "task": "t",
                        "agentName": "scout",
                        "thinking": "high",
                        "model": "m",
                        "systemPrompt": "s",
                        "tools": ["read"]
                    })
                )
            ),
            None,
            "every key here is advertised, so nothing may be reported"
        );
        let warning = unknown_param_warning(
            &tool_specs(),
            &call("dispatch_subagent", json!({ "task": "t", "agent": "x" })),
        )
        .expect("`dispatch_subagent` is an ALIAS of `subagent`, not an unknown tool");
        assert!(warning.contains("`subagent`"), "{warning}");
        assert!(warning.contains("`agent`"), "{warning}");
        assert!(warning.contains("`agentName`"), "{warning}");
    }

    /// The `subagent` spec advertises exactly the set of keys the executor
    /// reads — pinned by EXACT set equality, so both a dropped `thinking` and
    /// an unadvertised sixth key redden this test.
    ///
    /// Where each key is consumed: `task` / `systemPrompt` / `model` /
    /// `thinking` / `tools` in `interactive::dispatch_params`; `agentName` in
    /// `dispatch::dispatch_subagent`. A future key added to either reader must
    /// be advertised here — this is the test that goes RED when it is not.
    #[test]
    fn the_subagent_spec_advertises_every_param_the_executor_reads() {
        let spec = tool_specs()
            .into_iter()
            .find(|s| s.name == "subagent")
            .expect("subagent");
        let advertised: BTreeSet<String> = spec.parameters["properties"]
            .as_object()
            .expect("properties")
            .keys()
            .cloned()
            .collect();
        assert_eq!(
            advertised,
            [
                "agentName",
                "model",
                "systemPrompt",
                "task",
                "thinking",
                "tools"
            ]
            .into_iter()
            .map(String::from)
            .collect::<BTreeSet<String>>()
        );
    }

    /// (issue #82, interim) The three web tools' handlers accept camelCase
    /// aliases and `proxy` that the specs must advertise — the checker added
    /// for #78 is generic, so an unadvertised-but-legal key would be warned on
    /// every web call. `exec_param_keys()` is the census this test shares with
    /// `tool_specs_advertise_exactly_the_executor_param_keys`; it must list
    /// EVERY key the handler reads (`.get("…")`, including the
    /// `.or_else(|| params.get("…"))` alias arms) in `agent/web/tools.rs`.
    #[test]
    fn the_web_tool_specs_advertise_every_key_the_handlers_read() {
        let read = |name: &str| -> BTreeSet<String> {
            tool_specs()
                .into_iter()
                .find(|s| s.name == name)
                .unwrap_or_else(|| panic!("no spec for `{name}`"))
                .parameters["properties"]
                .as_object()
                .unwrap_or_else(|| panic!("`{name}` must advertise properties"))
                .keys()
                .cloned()
                .collect()
        };
        assert_eq!(
            read("web_search"),
            [
                "query",
                "queries",
                "num_results",
                "numResults",
                "provider",
                "recency_filter",
                "recencyFilter",
                "domain_filter",
                "domainFilter",
                "proxy"
            ]
            .into_iter()
            .map(String::from)
            .collect::<BTreeSet<String>>(),
            "exec_web_search reads all of these (web/tools.rs)"
        );
        assert_eq!(
            read("fetch_content"),
            ["url", "mode", "max_chars", "maxChars"]
                .into_iter()
                .map(String::from)
                .collect::<BTreeSet<String>>(),
            "exec_fetch_content reads all of these (web/tools.rs)"
        );
        assert_eq!(
            read("get_search_content"),
            [
                "response_id",
                "responseId",
                "offset",
                "limit",
                "find_text",
                "findText"
            ]
            .into_iter()
            .map(String::from)
            .collect::<BTreeSet<String>>(),
            "exec_get_search_content reads all of these (web/tools.rs) — the \
             camelCase alias stays ADVERTISED because the handler honours it, even \
             though nothing model-facing tells the model to SEND it any more \
             (see `the_model_facing_web_prose_names_the_key_the_advertised_schema_requires`)"
        );
    }

    /// The advertised `bash` description names the shell THIS platform's
    /// executor actually picks, and never names the other one.
    ///
    /// This is issue #79's lie-as-regression: the sentence the model is given
    /// must match the shell `exec_bash` branches to (`sh -c` on Unix,
    /// `cmd.exe /d /s /c` on Windows). It was `sh -c; NOT re-parsed` on Windows
    /// too, and that single false clause is what turned one quoting bug into six
    /// detours — the model trusted it, and so did the reader of the output.
    ///
    /// Reads the description out of `tool_specs()` rather than comparing the
    /// const to itself: the claim worth pinning is that the ADVERTISED spec is
    /// honest. The Unix arm's exact-equality assertion is also the guard that
    /// moving the literal into a const did not disturb one byte of the sentence
    /// every Linux and macOS session prompt carries.
    #[test]
    fn the_bash_description_names_the_shell_this_platform_actually_runs() {
        let d = bash_description();
        #[cfg(unix)]
        {
            assert_eq!(
                d,
                "Run a shell command (sh -c; NOT re-parsed), capturing interleaved stdout+stderr.",
                "the Unix description is byte-frozen (it is printed into every \
                 Unix session prompt and persisted as seq 0 of every session)"
            );
            assert!(
                !d.contains("cmd.exe"),
                "a Unix run never touches `cmd.exe`, so the description must not name it"
            );
        }
        #[cfg(not(unix))]
        {
            assert!(
                d.contains("cmd.exe") && d.contains("/d /s /c"),
                "the Windows description must name the shell the executor picks — \
                 `exec_bash` runs `cmd.exe /d /s /c` here, got {d:?}"
            );
            assert!(
                !d.contains("sh -c"),
                "the Windows description must not promise `sh -c` (that was #79's \
                 lie: the model trusted a sentence about a shell it never got), got {d:?}"
            );
        }
    }

    /// The `bash` description may not contain the substring `read` or the
    /// substring `subagent` — on ANY platform.
    ///
    /// Why a whole substring ban and not a word-boundary one: the `<tools>`
    /// section of the system prompt is rendered from the advertised specs
    /// (`harness/prompt.rs`'s `- {name}: {description}`), and
    /// `src-tauri/tests/worker_e2e.rs::full_turn_real_worker_canned_provider`
    /// asserts `!tools_section.contains("read") &&
    /// !tools_section.contains("subagent")` with only `bash` enabled — the
    /// assertion is a plain `contains`, so a `bash` description that merely
    /// contains the letters `read` (in `already`, `reads`, `thread`) reddens a
    /// Worker e2e. That test is not `cfg`-gated, so on a `windows`-only wording
    /// change the failure would appear ONLY on the Windows CI leg — the worst
    /// possible place to discover it. This test moves the constraint to a
    /// Linux-visible one.
    #[test]
    fn the_bash_description_never_names_another_advertised_tool() {
        let d = bash_description();
        assert!(
            !d.contains("read"),
            "`worker_e2e.rs`'s `full_turn_real_worker_canned_provider` asserts the \
             rendered <tools> section does not contain `read`; the section is built \
             from this description, so the substring is banned outright (this is why \
             the Windows wording says `takes`, not `reads`). got {d:?}"
        );
        assert!(
            !d.contains("subagent"),
            "same guard as above for `subagent`: the e2e asserts the <tools> section \
             with only `bash` enabled contains neither tool name, and a description \
             that trips it reddens the Worker e2e. got {d:?}"
        );
    }

    /// The parameter name the web tools' MODEL-VISIBLE prose uses for the cache
    /// handle must be a key the advertised schema REQUIRES — not merely one it
    /// tolerates.
    ///
    /// This is issue #82's residual lie-as-regression, the sibling of
    /// [`the_bash_description_names_the_shell_this_platform_actually_runs`]. The
    /// interim fix for #82 made the specs advertise what the handlers accept,
    /// which is right for the CHECKER but wrong for the SENTENCE: while
    /// `required` named `response_id`, `fetch_content`'s truncation note told the
    /// model to call `get_search_content with responseId …` and the
    /// `get_search_content` description said "by responseId". That call is
    /// schema-INVALID (`required` is not satisfied by an alias), and the #78
    /// checker is structurally blind to it — its rule 3 says `required` is
    /// irrelevant, because only `properties` defines legality, and BOTH
    /// spellings sit there. So the only feedback a strict model got was the
    /// provider's own rejection, for a call the tool itself provoked.
    ///
    /// The test is the anti-drift pin between the two files: the prose is built
    /// from `web::tools::RESPONSE_ID_PARAM`, the schema is a literal here, and
    /// the assertions use literals of their own so the test cannot be satisfied
    /// by editing the const alone. Narrowing the handler to the snake_case
    /// dialect is still open on issue #82 — the LAST assertion pins that the
    /// alias is still ADVERTISED, so if that option is ever taken the census
    /// tests (this file's two web tests plus `exec_param_keys()`) are the place
    /// that changes, not this sentence.
    #[test]
    fn the_model_facing_web_prose_names_the_key_the_advertised_schema_requires() {
        use crate::agent::web::tools::{
            search_summary_empty, search_summary_head, truncation_note, RESPONSE_ID_PARAM,
        };

        let spec = tool_specs()
            .into_iter()
            .find(|s| s.name == "get_search_content")
            .expect("`get_search_content` is advertised");

        // 1. The schema, read as the model's provider reads it.
        let required: Vec<&str> = spec.parameters["required"]
            .as_array()
            .expect("`get_search_content` declares `required`")
            .iter()
            .map(|v| v.as_str().expect("a string key"))
            .collect();
        assert_eq!(
            required,
            vec!["response_id"],
            "the required key is the snake_case one — the sentence below has to \
             agree with THIS literal, not with a const"
        );

        // 2. The prose is built from a name that IS that required key.
        assert_eq!(
            RESPONSE_ID_PARAM, required[0],
            "the web tools' model-visible sentences interpolate a parameter name \
             that `get_search_content`'s `required` does not name"
        );

        // 3. Every model-visible sentence the web tools emit names the handle in
        //    the required spelling and NEVER in the alias spelling. Literals,
        //    deliberately — see the doc above.
        let id = "resp-1";
        let prose = [
            spec.description.clone(),
            search_summary_head(id),
            search_summary_empty(id),
            truncation_note(1234, id),
        ];
        for text in &prose {
            assert!(
                text.contains("response_id"),
                "model-visible web prose must name `response_id` (the key the \
                 schema requires), got {text:?}"
            );
            assert!(
                !text.contains("responseId"),
                "model-visible web prose must not name the `responseId` alias: a \
                 call written from this sentence fails `required: \
                 [\"response_id\"]` at the provider, and the #78 checker cannot \
                 flag it because both spellings are in `properties`. got {text:?}"
            );
        }

        // 4. The alias is still ACCEPTED and still ADVERTISED (issue #82's
        //    option (b) is still open; the #78 checker must keep staying quiet
        //    about a call the handler would honour).
        assert!(
            spec.parameters["properties"]
                .as_object()
                .expect("`get_search_content` advertises properties")
                .contains_key("responseId"),
            "`exec_get_search_content` still reads `responseId`, so the alias must \
             stay advertised — removing it from the handler is issue #82's open \
             option (b), and it moves the census tests with it"
        );
    }

    /// The `mcp` tool's `server` param, read out of `tool_specs()` — the
    /// sentence the model writes its call from.
    fn mcp_server_param_description() -> String {
        tool_specs()
            .into_iter()
            .find(|s| s.name == "mcp")
            .expect("`mcp` is advertised")
            .parameters["properties"]["server"]["description"]
            .as_str()
            .expect("`server` is advertised with a description")
            .to_string()
    }

    #[test]
    fn the_mcp_server_param_prose_tells_the_model_what_the_scope_actually_does() {
        // `bd07fbf` widened the CONTRACT — `server` scopes the ANSWER on
        // `describe` as well as `search`, and a name that was never configured
        // FAILS instead of returning a clean "not found" — but the advertised
        // sentence still described the old, narrower one ("Filter to a specific
        // server (for list, search, or disambiguating calls)"). A model reading
        // that sentence has two wrong beliefs: that `describe` ignores `server`
        // (so it would not pass it, and would get a schema from the wrong
        // server), and that a guessed name is a safe probe (issue #68's
        // credibility bug — an empty scope must not read as "it doesn't exist").
        // Pinned as substrings rather than a byte-for-byte copy so the sentence
        // can still be reworded; the two CLAIMS are the contract.
        let d = mcp_server_param_description();
        assert!(
            d.contains("describe"),
            "the sentence must say `describe` is scoped, or the model will not pass `server` \
             to it: {d}"
        );
        assert!(d.contains("search"), "and `search`: {d}");
        // The requirement clause names the set the manager HOLDS, exactly the
        // way `mcp::tool`'s failure message does (`unknown_server_scope`). It
        // must NOT say "configured": `types::classify_server` drops a
        // `disabled: true` (or legacy `sse` / malformed) entry before the
        // manager ever sees it, so a server the user DID configure answers
        // `Unknown MCP server` — a sentence calling the requirement
        // "configured" contradicts the user's own settings file, which is why
        // `bd07fbf`'s other half abandoned that wording in `tool.rs`. Pinned
        // both ways: the positive claim, and the absence of the two WRONG
        // PHRASES that make membership of the config the requirement. Banning
        // the bare word "configured" would over-reach — it reddens a correct
        // rewording ("a configured-but-disabled entry is not held"), which is
        // the very distinction this clause exists to protect.
        let lower = d.to_lowercase();
        assert!(
            d.contains("this session holds"),
            "the requirement is a server the session HOLDS (the wording \
             `unknown_server_scope` fails with), not a name from the config: {d}"
        );
        assert!(
            !lower.contains("must name a configured") && !lower.contains("actually configured"),
            "the requirement must not be membership of the config — a configured-but-disabled \
             server is not held and answers Unknown: {d}"
        );
        assert!(
            d.contains("fails"),
            "and that a name outside that set is a FAILURE, not a 'not found': {d}"
        );
        // One sentence: this is the model's only briefing on the param, and the
        // surrounding workflow lines carry the rest. (No sentence-terminating
        // period mid-string — a two-sentence param description is one the model
        // skims.)
        assert!(
            !d.contains(". "),
            "the param description stays ONE sentence: {d}"
        );
    }

    /// The advertised `bash` description, read out of `tool_specs()` (the same
    /// call the loop advertises from) rather than from the const.
    fn bash_description() -> String {
        tool_specs()
            .into_iter()
            .find(|s| s.name == "bash")
            .expect("`bash` is in `tool_specs()`")
            .description
    }
}
