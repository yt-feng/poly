The JSON files in this directory are intentionally small and inspectable. CI
checks required manifest keys with the standard library so validation remains
offline and does not depend on a schema service.

## v3 observation contract

`v3_observation.schema.json` and `v3_contract.py` define a strict public
observation record. A usable row must include source event time, local receive
time, market/condition/token identities, quote or trade type, book sides and
levels, fee parameters, tick/minimum-order rules, and immutable provenance.
The validator reports latency percentiles and quarantines missing metadata,
future-label fields, stale/future timestamps, duplicate IDs, and conflicts.
Historical CSVs are not rewritten or silently upgraded.
