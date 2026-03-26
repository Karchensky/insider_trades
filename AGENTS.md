# AGENTS.md

Short pointer for coding agents (Cursor, Claude Code, etc.).

**Full project context:** see **[CLAUDE.md](./CLAUDE.md)**.

**Project-local Cursor skills** (under `.cursor/skills/`):

- **`insider-trades-options-signals`** — scoring, TP100 validation, pipelines, migrations alignment.
- **`supabase-postgres-ops`** — timeouts, locks, safe SQL/migration patterns on Supabase.

Personal/global Cursor skills live under `~/.cursor/skills/`; do not edit `~/.cursor/skills-cursor/` (reserved).
