## Agent skills

### Issue tracker

Issues are local markdown files under `.scratch/<feature>/`. See `docs/agents/issue-tracker.md`.

### Triage labels

Default five-role vocabulary (`needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`), recorded as the `Status:` line. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one `GLOSSARY.md` at the repo root plus `docs/adr/`. See `docs/agents/domain.md`.

## Architecture navigation

### Data flow
Complete pipeline (collect → derive → query) is in [DATA.md](docs/DATA.md).

### Derive order
`derive.py` defines strict order: bronze → silver → observations → current.
Must preserve this order. See GLOSSARY #4.

### Key invariants
All domain terms and invariants are in [GLOSSARY.md](GLOSSARY.md).
Add new terms there first before using them.

### Consistency mechanisms
- Watermark: `watermark.py` (timestamp-based, filters anomalies)
- Deriving atomicity: `deriving.py` (flag files, auto-recovery; recovery replays the suffix, never a single day)
- Derive provenance: `lineage.py` (per-day Raw fingerprint; staleness detection + `build --stale`)
- Layer reconcile: `doctor._layer_reconcile` (current ↔ observations ↔ silver, plus last_seen_at vs newest Raw)
- Artifact freshness: `doctor._artifact_freshness` (exports must match current id-for-id; enrich tables only report coverage)
- Card provenance: `doctor._card_provenance` (`carddata.source_sha` vs `current.sha`, active libs only)
- Change-set cards: `enrich.scan_changed_cards` (same `lastModified` watermark as collect; writes `card-*`, never Bronze)
- Single day frame: `bronze.day_frame` (Bronze partition + card-batch supplements; shared by `silver` and `state`)
- Health history: `health.py` (per-run findings log; `runs/` records command outcome, not alerts)
- Requirements: `requirement.py` (one spec per `REQ-xxxx`; `kind` selects the executor; `directory_name()` / `resolve_dir()` for semantic folders) + `delivery.py` (`EXECUTORS` dispatch, unified `Payload` → three-part archive, drift marking) + `source.py` (bottom-library stamp + input snapshot stamp, kept out of the import cycle)
- Backfill executor: `backfill.py` (`kind: backfill`; URL list → filled workbook; mechanical in engine, judgment in `overrides.json`; never edits the source file)
- Unified lifecycle: every form of delivery shares `requests/`, `request run/run-due`, `history/`, `deliveries/`. Add a new form by writing one executor and registering it in `EXECUTORS` — never a new directory, command, or ledger. See GLOSSARY #57.
- Self-contained folders: `requests/REQ-xxxx-<kind>-<title>/` holds `spec.yaml`, `method/`, `input/`, `overrides.json`, `history/`, `deliveries/`. `request new --source` copies the external file into `input/` once; absolute paths, `..` escapes and `copy_to` are rejected at load time, products never leave the folder. `manifest.fill.input` records the snapshot's relative path + sha256. See GLOSSARY #58.
- Process is part of self-containment: `method.py` owns `method/METHOD.md` (steps, criteria, machine-vs-human split), `method/method.json` (per-script purpose/run/sha256) and `method/scripts/` (the scripts that actually ran). `deliver()` refuses to ship when the method is missing, still the skeleton (`<!-- 待填：`), lacks a `## 步骤` section, has unregistered scripts, or a sha256 mismatch — no `deliveries/` directory is even created. `request new` scaffolds the draft; `request method <id> [--check]` rescans and re-fingerprints but never invents a script's purpose. Paths in the index must stay relative (absolute or `..` is rejected). Environment is pinned, not copied: `manifest.method.env` records package version, `CONTRACT_VERSION` and commit, so a code change can be caught instead of silently changing the method. `doctor` flags only requests that already delivered. See GLOSSARY #69.
- Spec versioning: `version` is not decoration. `history/vN.yaml` is the snapshot of the version that actually shipped and `manifest.spec_version` points at it, so a reused number silently rewrites history. `version_conflict()` compares the current spec with `history/v<current>.yaml` by **semantic digest**: `spec_digest` loads the file into a `Spec` (defaults filled, enums normalised) and hashes only what decides the product — kind/mode/sql/criteria/order_by/output/guard/alert/fill, with the input referenced by its relative path so the digest survives moving the folder. Comments, key order, an explicit default such as `kind: query`, and title/requester/cadence changes do not count; `下载量 > 1000` → `> 800` does. A reused number raises `DeliveryBlocked` and `doctor` flags it for delivered requests; `manifest.spec_sha256` records the digest. Historical snapshots must be loaded with `base_dir=<request dir>` — they live under `history/`, where `input/…` would otherwise resolve to `history/input/…`. Not built yet: input versioning, `request diff`, `request compare`. See GLOSSARY #70.
- Change tooling: `request input <id> --add <file>` copies a new table into `input/v<n>/`, leaves the old snapshot untouched (overwriting it would break every earlier manifest's `fill.input.sha256`), and reports header drift plus declared-but-missing columns — it never edits the spec, because changing `fill.source.path` is a calibration change and needs a version bump. `request diff <id> [vA vB]` compares two historical specs (criteria aligned by `metric`, fill columns by name, so inserting a condition does not make every later row look changed; non-product fields reported separately). `request compare <id> [A B]` diffs two deliveries — query by `id`, backfill by the key column with links normalised to `owner/name`; it reports added/removed rows, per-column changed cells with samples, and column-set differences instead of silently intersecting. `manifest.fill.input.header` records ordered column names plus a digest; a changed header is surfaced in README but never blocks. See GLOSSARY #71.
- Agent workflow: `.workbuddy/skills/hf-request/SKILL.md` is the project skill for intake → response → delivery → change, with `references/blocks.md` mapping every rejection message to its fix. Commands enforce the hard rules; the skill governs method. Do not let them blur — enforcement in prose is a suggestion, method in code is a straitjacket.
- Default delivery fields: `fields.py` is the single contract (`DEFAULT_COLUMNS`, `COLUMN_NOTES`, `PAGE_FIELDS`, `FALLBACKS`, `CONTRACT_VERSION`). `export-wide` writes `data/exports/datasets.parquet` with exactly those 31 columns; `compile_sql` projects them when `output.columns` is empty — never `SELECT *`; `doctor` reconciles the export schema with the contract. Do not read the parquet schema at query time (the export can lag behind `current`). `FALLBACKS` fills four tag columns from `card_*` in the default projection only — the 72-column view keeps both sources separate for tracing. `PAGE_FIELDS` maps HF detail-page labels to column names; `display_expr` renders machine codes as page text in CSV while parquet keeps raw values. Multi-value columns are exported as real LIST values, structurally identical to the view (v3's `;`-joined VARCHAR workaround was reverted in v5 once its upstream cause — a previewer reading `COL.LIST.ELEMENT` as a column name — was fixed; do not reintroduce flattening for tooling reasons, fix the tool). `EXTENDED_FIELDS` lists 19 more semantic fields available via `output.columns` but deliberately outside the default surface; `regions` is excluded because 99.94% is the auto-inferred `us`. Never add low-coverage columns back to the default face just because they exist. Bump `CONTRACT_VERSION` when the list or column types change. See GLOSSARY #62-#66.
- Scheduling: `scripts/daily.ps1` is the only catch-up unit — it runs collect → `build --stale` → reconcile → export → deliver → doctor in a fixed order, and only `hf-daily` keeps `-StartWhenAvailable`. Dependency is expressed by script order, never by clock time: on 2026-11-09 a boot at 19:03 made Windows replay five missed tasks in the same minute, so export finished before reconcile and published a stale projection. `hf-compact` / `hf-sizes` / `hf-archive` stay independent and deliberately do not catch up (they would only fight the chain for the write lock). Doctor's non-zero inside the chain means "alerts", not failure. See GLOSSARY #67.
- Storage lifecycle: `compact` / `archive` **move only, never delete**; deletion is a separate command `prune` that touches nothing outside `data/backup/` and ages partitions by directory name (not mtime — sync tools rewrite it). Threshold shared with `compact` via `retention.BACKUP_PRUNE_AFTER_DAYS`. Cold baselines are deduped per id, so their size is roughly constant; the unbounded layer was backup. Doctor's 「存储」 line breaks data size into raw / cold / backup / reports / rest.
- Card refresh: the only automatic path is `collect --cards` (changed set, ~8k/day). Full card sweeps cost ~1127 pages / 6.7 GB / 14 hours and the long tail's `cardData` is empirically empty, so do **not** schedule one; use `enrich --ids a,b,c` (cap 200) to fill the few datasets a delivery actually needs. Missing card fields are mostly source sparsity, not staleness. See GLOSSARY #68.
- See [ADR-0001](docs/adr/0001-metadata-refresh-consistency-hardening.md) for design decisions.

### Test naming
Test functions use English names with Chinese docstrings:
```python
def test_reasonable_timestamp() -> None:
    """测试合理的时间戳。"""
```
Not `def test_合理时间戳()` — function names are identifiers (ASCII), docstrings are documentation (any language).
