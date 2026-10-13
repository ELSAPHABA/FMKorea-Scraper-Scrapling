# Project-wide agent instructions

These instructions apply to every task and every directory in this project.

- For browser-based exploration, website inspection, UI interaction, screenshots, or browser-driven testing, use the `$agent-browser` skill and its `agent-browser` CLI. Do not use Playwright directly for these agent tasks.
- Before using `agent-browser`, follow its skill instructions and load the current CLI workflow with `agent-browser skills get core`.
- This tool rule concerns the development agent's browser exploration. The scraper's Scrapling browser fallback remains governed by the functional specification.
