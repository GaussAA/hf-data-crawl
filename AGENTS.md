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
- Deriving atomicity: `deriving.py` (flag files, auto-recovery)
- See [ADR-0001](docs/adr/0001-metadata-refresh-consistency-hardening.md) for design decisions.

### Test naming
Test functions use English names with Chinese docstrings:
```python
def test_reasonable_timestamp() -> None:
    """测试合理的时间戳。"""
```
Not `def test_合理时间戳()` — function names are identifiers (ASCII), docstrings are documentation (any language).
