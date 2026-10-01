# Changelog

All notable changes to `selecto_db_duckdb` will be documented in this file.

## Unreleased

- `execute_write/3` refuses a write without the `Selecto.Write.Authorization`
  the governed entry point (`SelectoUpdato`) issued for exactly that command,
  batch or graph, with `:ungoverned_write` and no row changed. The raw
  implementation moved to `execute_write_unsafe/3`, for trusted tooling and
  adapter tests only.

## [0.5.0] - 2026-08-14

### Changed
- Normalize the custom O'Saasy Hex metadata to `LicenseRef-O-Saasy`; the
  packaged license text and licensing terms are unchanged.
- Raised the Selecto baseline to `0.5.0` and implemented the explicit runtime
  and normalized result/error/type ports.
- Unsupported dialect fragments now return structured capability evidence
  instead of inheriting PostgreSQL fallback SQL.
- Added an explicit DuckDB dialect for portable datetime formatting and
  case-insensitive comparisons, with fail-closed timezone/epoch conversion.

## [0.2.0] - 2026-08-12

### Added
- Added versioned portable insert, update, upsert, and delete compilation with
  bound values, governed predicates, foreign-key guards, and `RETURNING`.
- Added atomic batch and generated-key graph execution with complete rollback
  and logical cardinality enforcement.
- Added in-memory runtime coverage for tenant/reference isolation, upsert,
  generated keys, and rollback.

### Changed
- Reports native `MERGE` as unavailable for portable execution because the
  current Duckdbex prepared path cannot bind MERGE parameters safely; ordered
  transactional graph execution is used instead.

## [0.1.0] - Unreleased

### Changed
- Updated installation guidance to reflect the direct `selecto` dependency
  path for the adapter contract.
