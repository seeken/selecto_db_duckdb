# Changelog

All notable changes to `selecto_db_duckdb` will be documented in this file.

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
