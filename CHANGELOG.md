# Changelog

All notable changes to `selecto_db_duckdb` will be documented in this file.

## Unreleased

- Only a query's root reads a CTE or VALUES list of its name, and only when its
  domain declares no `source_schema`; joined schemas, through tables and
  declared tables stay tables (`cte_name_collision`). A root named like one of
  its own lists reads the list, even where a table of that name exists. CASE
  subqueries count like filter subqueries; a raw CTE counts by the name its text
  starts with, and no longer raises `BadMapError`. An embedded query missing its
  required tenant refuses `tenant_scope_missing` instead of raising.
- Tested against Selecto's single CTE and VALUES name rule: a filter subquery
  or a declared non-association join reads its table, and an outer list of that
  name refuses (`cte_name_collision`); a domain rooted on a CTE reads the CTE;
  lists of one name in two union members are private to each.
- TW-09 post-image: a guarded INSERT/UPDATE/UPSERT (`metadata.post_image`)
  returns its rows' keys and re-reads them under the scope in the same
  transaction; a row outside it rolls back with `query_rule_violation`. The
  relation resolves as DuckDB resolves the write (temp, then the current
  database and schema; a view is refused) and the key must be a primary key
  or a unique constraint over NOT NULL columns, else
  `query_rule_not_evaluable`.

- Fixed: `column_catalog/2` reports every column of a table nondeterministic
  when its regenerated CREATE statement declares any `COLLATE` (DuckDB's
  catalog views do not show per-column collations), so `VARCHAR COLLATE
  NOCASE` never decides `<>` in process. Table names match case-insensitively
  and may be schema-qualified.

- `column_catalog/2`: information_schema data types (with precision and
  scale) for SelectoUpdato's verified candidate check (query_enforced_scalar
  v2); a session `default_collation` marks every column nondeterministic.

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
