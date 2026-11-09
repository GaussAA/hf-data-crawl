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
- Self-contained folders: `requests/REQ-xxxx-<kind>-<title>/` holds `spec.yaml`, `input/`, `overrides.json`, `history/`, `deliveries/`. `request new --source` copies the external file into `input/` once; absolute paths, `..` escapes and `copy_to` are rejected at load time, products never leave the folder. `manifest.fill.input` records the snapshot's relative path + sha256. See GLOSSARY #58.
- Default delivery fields: `fields.py` is the single contract (`DEFAULT_COLUMNS`, `COLUMN_NOTES`, `PAGE_FIELDS`, `FALLBACKS`, `CONTRACT_VERSION`). `export-wide` writes `data/exports/datasets.parquet` with exactly those 31 columns; `compile_sql` projects them when `output.columns` is empty — never `SELECT *`; `doctor` reconciles the export schema with the contract. Do not read the parquet schema at query time (the export can lag behind `current`). `FALLBACKS` fills four tag columns from `card_*` in the default projection only — the 72-column view keeps both sources separate for tracing. `PAGE_FIELDS` maps HF detail-page labels to column names; `display_expr` renders machine codes as page text in CSV while parquet keeps raw values. Multi-value columns are exported as real LIST values, structurally identical to the view (v3's `;`-joined VARCHAR workaround was reverted in v5 once its upstream cause — a previewer reading `COL.LIST.ELEMENT` as a column name — was fixed; do not reintroduce flattening for tooling reasons, fix the tool). `EXTENDED_FIELDS` lists 19 more semantic fields available via `output.columns` but deliberately outside the default surface; `regions` is excluded because 99.94% is the auto-inferred `us`. Never add low-coverage columns back to the default face just because they exist. Bump `CONTRACT_VERSION` when the list or column types change. See GLOSSARY #62-#66.
- Scheduling: `scripts/daily.ps1` is the only catch-up unit — it runs collect → `build --stale` → reconcile → export → deliver → doctor in a fixed order, and only `hf-daily` keeps `-StartWhenAvailable`. Dependency is expressed by script order, never by clock time: on 2026-11-09 a boot at 19:03 made Windows replay five missed tasks in the same minute, so export finished before reconcile and published a stale projection. `hf-compact` / `hf-sizes` / `hf-archive` stay independent and deliberately do not catch up (they would only fight the chain for the write lock). Doctor's non-zero inside the chain means "alerts", not failure. See GLOSSARY #67.
- See [ADR-0001](docs/adr/0001-metadata-refresh-consistency-hardening.md) for design decisions.

### Test naming
Test functions use English names with Chinese docstrings:
```python
def test_reasonable_timestamp() -> None:
    """测试合理的时间戳。"""
```
Not `def test_合理时间戳()` — function names are identifiers (ASCII), docstrings are documentation (any language).
