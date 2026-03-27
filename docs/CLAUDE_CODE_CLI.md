# Claude Code CLI — quick reference

Terminal agent: [Claude Code](https://docs.anthropic.com/en/docs/claude-code/overview).
Authoritative detail: [CLI reference](https://docs.anthropic.com/en/docs/claude-code/cli-reference) and [Model configuration](https://docs.anthropic.com/en/docs/claude-code/model-config).

Run from your project root so **`CLAUDE.md`** is in scope.

---

## Do not use `pip install claude`

PyPI has an unrelated package named **`claude`** (not Anthropic). It does **not** install the Claude Code CLI, and **`claude` will not work** in PowerShell.

**Install the real CLI on Windows (pick one):**

```powershell
# Official installer (recommended)
irm https://claude.ai/install.ps1 | iex
```

Or:

```powershell
winget install Anthropic.ClaudeCode
```

Then **close and reopen** the terminal (or sign out/in) so **PATH** picks up the new `claude` command. Verify:

```powershell
claude --version
```

If it still fails, check that your user **PATH** includes the directory the installer printed (often under `%USERPROFILE%` or `%LOCALAPPDATA%`).

---

## Model

**Priority (highest first):** session command → startup flag → env → settings file.

| How                      | Example                                                                                 |
| ------------------------ | --------------------------------------------------------------------------------------- |
| **Startup flag**   | `claude --model opus`                                                                 |
| **During session** | Type `/model` then pick or e.g. `/model sonnet`                                     |
| **Environment**    | `set ANTHROPIC_MODEL=sonnet` (Windows) / `export ANTHROPIC_MODEL=sonnet` (Unix)     |
| **Settings JSON**  | `"model": "opus"` in [settings](https://docs.anthropic.com/en/docs/claude-code/settings) |

**Common aliases:** `default`, `sonnet`, `opus`, `haiku`, `opusplan` (Opus in plan mode, Sonnet for execution), `sonnet[1m]` / `opus[1m]` for 1M context where your plan supports it.

**Full IDs:** e.g. `claude --model claude-sonnet-4-6` (see [Models overview](https://platform.claude.com/docs/en/about-claude/models/overview)).

---

## Effort / “thinking” level

Controls **adaptive reasoning** (depth vs speed/cost) on supported models (e.g. Sonnet 4.6, Opus 4.6).

| Level      | Notes                                                                    |
| ---------- | ------------------------------------------------------------------------ |
| `low`    | 1Faster, lighter reasoning                                               |
| `medium` | Balanced                                                                 |
| `high`   | Deeper reasoning                                                         |
| `max`    | Deepest;**Opus 4.6 only**; session-only, not persisted in settings |

| How                      | Example                                                                                   |
| ------------------------ | ----------------------------------------------------------------------------------------- |
| **Startup**        | `claude --effort high`                                                                  |
| **During session** | `/effort high` or `/effort auto` to reset to model default                            |
| **Model picker**   | `/model` — effort slider for supported models                                          |
| **Environment**    | `CLAUDE_CODE_EFFORT_LEVEL=high` (or `low` / `medium` / `max` / `auto`)          |
| **Settings file**  | `"effortLevel": "low"` \| `"medium"` \| `"high"` only (`max` not persisted there) |

**Precedence:** `CLAUDE_CODE_EFFORT_LEVEL` beats settings and session defaults; skill/subagent frontmatter can override session (not the env var).

**Disable adaptive reasoning** (fixed thinking budget): `CLAUDE_CODE_DISABLE_ADAPTIVE_THINKING=1` — see [env vars](https://docs.anthropic.com/en/docs/claude-code/env-vars).

---

## Sessions: one conversation or many?

- **Interactive `claude`** (no `-p`): Claude Code **persists** the session to disk so you can **resume** it. Persistence is **per project directory** (and named sessions exist).
- **Continue last chat in this folder:** `claude -c` or `claude --continue`
- **Resume by name or ID:** `claude --resume "my-feature"` or `claude -r <id>` (see docs for picker)
- **Named new session:** `claude -n "auth-refactor"`
- **Fork when resuming:** `--fork-session` (new session ID, same history context pattern — see [CLI reference](https://docs.anthropic.com/en/docs/claude-code/cli-reference))
- **Print / one-shot mode:** `claude -p "question"` — non-interactive; use **`--no-session-persistence`** if you do **not** want this run saved (print mode only)

So: **one ongoing thread per resumed session**; starting **fresh** in the same directory without `-c` begins a **new** persisted session unless you configure otherwise. Exact storage location is managed by Claude Code (not in-repo).

---

## Useful commands (cheat sheet)

| Command                               | What it does                                                           |
| ------------------------------------- | ---------------------------------------------------------------------- |
| `claude`                            | Interactive session (current directory)                                |
| `claude "initial prompt"`           | Interactive, first message pre-filled                                  |
| `claude -p "query"`                 | Answer in terminal, then exit (SDK/print mode)                         |
| `claude -c`                         | Continue**most recent** conversation **in this directory** |
| `claude -c -p "query"`              | Continue that conversation, one-shot print                             |
| `claude --resume NAME`              | Resume named (or ID) session; add prompt after if needed               |
| `claude --model opus --effort high` | Start with model + effort                                              |
| `claude --bare -p "query"`          | Faster scripted run: skips CLAUDE.md/skills discovery                  |
| `claude auth login`                 | Sign in (`--console` for API billing account)                        |
| `claude auth status`                | Who is logged in                                                       |
| `claude update`                     | Update CLI                                                             |
| `claude --version`                  | Version                                                                |

**Piping:** `type file.txt | claude -p "summarize"` (PowerShell) / `cat file | claude -p "summarize"` (bash).

**More flags:** permission modes, MCP, tools allow/deny, max budget/turns — see [CLI reference](https://docs.anthropic.com/en/docs/claude-code/cli-reference).

---

## In-session slash commands (examples)

- `/model` — switch model / effort
- `/effort` — set effort level
- `/status` — account + model info
- `/resume` — pick a past session (if available)

Full list: [Commands](https://docs.anthropic.com/en/docs/claude-code/commands).

---

## Verify your install

```bash
claude --version
claude --help
```

If `claude` is not found, ensure the install script’s bin directory is on your **PATH** (Windows: User PATH after `irm ... | iex`).
