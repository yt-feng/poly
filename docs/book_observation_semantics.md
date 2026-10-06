# Book observation evidence

The legacy CSV's `sell_*_cents` fields contain bids; `buy_*_cents` contain asks.
A blank legacy bid means no price survived the legacy parser. It does not
authenticate an empty venue book. Missing/null side fields, an empty list and
all-unparseable prices can produce the same blank price, blank size and zero
parsed-level count. A truthy invalid non-iterable side may instead abort the row.

Zero price is rendered as `0.00`, not blank. Zero or malformed size can coexist
with a quoted price: the legacy price selector does not filter by size. Its
truthy alias fallback can replace numeric size zero with an `amount`/`quantity`
value. Nonfinite price can appear as textual `nan`/`inf`. These behaviors are
preserved for historical compatibility; they are not executable quote guarantees.

Transport, HTTP and JSON errors normally abort the legacy snapshot; loop modes
log the error and skip the row. An HTTP-success object with missing side fields
can instead yield blanks. Cached market `closed`/`active` fields do not gate the
legacy book parser. Header migration can fill absent columns with blanks. The
old CSV cannot distinguish those causes after raw payload/status evidence is
lost. Repeated values do not prove staleness, and absent rows do not prove closure.

The v2/v3 path now archives `polymarket_rest_book_response` before timestamp or
ladder parsing, including the requested token and decoded payload. This new
source is evidence only; it does not refresh valid-book feature state. Parsing
failure produces `polymarket_book_attempt_error`, with token, error type and
prior cached-book timestamp. The prior cache retains its original age.

V3 book HTTP evidence additionally retains request/receive wall and monotonic
times, actual status, a header allowlist and captured-body hash/length. Failed
HTTP/JSON or oversized responses retain bounded base64 body bytes. A truncated
body's hash covers only captured bytes. Successful JSON payloads remain in the
pre-parser response stream; successful raw wire bytes are not retained. No
response headers containing cookies or credentials are archived. HTTP timing
metrics remain available; a request that never reaches a response has unknown
status, not a fabricated success. Existing server-directed cooldowns remain.

Empty valid JSON books, malformed bodies and failed requests remain separate.
None establishes venue closure, fillability, freshness at a decision, or the
reason liquidity disappeared. Historical CSVs are not repaired or relabeled.
This patch does not change collection workflows or start any feed.

V3 assigns a capture-scoped attempt ID to each existing book poll. HTTP facts,
the pre-parser response, parse result and error share that ID through task-local
context, including concurrent requests. Each completed/cancelled poll emits a
small `measurement` stream record. The full successful decoded book is stored
once under `polymarket_rest_book_response`; the successful
`polymarket_rest_book` raw record has `payload_ref` instead of a duplicated
`payload`. In-process book consumers still receive the original object. Offline
readers must resolve the reference; it is not an empty book. Old archives retain
their original representation.

Raw WS records retain each batch item's available millisecond source timestamp.
A single `source_event_ms` is set only when all items have the same known time.
Missing, malformed or differently timed items never acquire an invented common
timestamp. Request/receive clocks, timestamp age and cross-side skew are
measurement diagnostics, not authenticated atomicity or one-way latency.

The offline utility creates a complete fixed calendar with ten minutes' lead,
rounded up to a five-minute boundary, and an exclusive end 48 hours later:

```bash
python measurement_v3.py plan --anchor "$APPROVED_ANCHOR_UTC" \
  --output /private/calendar.json
python measurement_v3.py audit --plan /private/calendar.json \
  --archive /path/to/existing/run1 --archive /path/to/existing/run2 \
  --output /private/measurement-report.json
```

These commands make no requests and do not start or stop any collector. Freeze
the anchor/calendar before its first window; an approval alone is not evidence
that production runs the required code. The private protocol defines the
activation prerequisite. All 576 windows remain, including periods with no
attempt evidence. The audit uses completed attempt time to select the last
decision observation, first strictly later entry, and first scheduled target;
it retains failed first attempts. Original decision/target tolerance and entry
budget are distinct. Equal-time conflicting choices stay ambiguous. No missing
window is reclassified as a failed order, an empty book or a payoff.

This compact audit verifies measurement segment hashes and references existing
raw evidence without copying it. It does not reread all raw files or certify
their integrity; verify required raw segments against the same manifests before
using them for deeper analysis. Corrupt measurement segments and conflicting IDs
fail the audit. Process crashes can leave incomplete `.part` files; missing
attempts remain unknown. This schema does not satisfy the stricter strategy
observation contract by itself, and no research/canary promotion is automatic.

Offline verification, with all HTTP interactions mocked:

```bash
python -m pip install -r requirements-test.txt
python -m unittest discover -s tests -p 'test_book_observation_semantics.py' -v
python -m unittest discover -s tests -p 'test_measurement_v3.py' -v
python -m unittest discover -s tests -p 'test_*.py' -v
```
