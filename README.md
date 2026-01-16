# arr-tag-searcher

One container that:
- Sonarr searches by Series (show)
- Radarr searches by Movie
- Lidarr searches by Artist

Tag rules:
- Items tagged `search` -> searches missing (Wanted/Missing)
- Items tagged `done` -> searches upgrades (Wanted/Cutoff)

Selection:
- Uses a shuffle-bag per app+mode so the same items are not searched every run.
- No repeats until the current eligible pool has been searched once.

Cooldown:
- Optional, configurable via env.
- Can be set per app and per mode.

Auto-promotion:
- If enabled, items tagged `search` are retagged to `done` when they no longer appear in Wanted/Missing.

## Setup
1. Copy `.env.example` to `.env` and fill in URLs + API keys.
2. `docker compose up -d --build`

## State
Shuffle-bag + cooldown state is stored in `STATE_DIR` (default `/data/state`).
With the compose file, it persists at `./state/state.json`.
