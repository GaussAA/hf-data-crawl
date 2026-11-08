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
- See [ADR-0001](docs/adr/0001-metadata-refresh-consistency-hardening.md) for design decisions.

### Test naming
Test functions use English names with Chinese docstrings:
```python
def test_reasonable_timestamp() -> None:
    """测试合理的时间戳。"""
```
Not `def test_合理时间戳()` — function names are identifiers (ASCII), docstrings are documentation (any language).
