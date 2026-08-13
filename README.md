# SelectoDBDuckDB

DuckDB adapter package for the Selecto ecosystem.

This package provides `SelectoDBDuckDB.Adapter`, an external adapter module for
using Selecto against DuckDB via `duckdbex`.

## Installation

```elixir
def deps do
  [
    {:selecto, ">= 0.4.13 and < 0.6.0"},
    {:selecto_db_duckdb, "~> 0.2.0"}
  ]
end
```

Current package version: `0.2.0`.

## Usage

Pass the adapter explicitly when configuring Selecto:

```elixir
selecto =
  Selecto.configure(domain, [database: ":memory:"],
    adapter: SelectoDBDuckDB.Adapter
  )
```

You can also connect manually and pass the live connection:

```elixir
{:ok, conn} = SelectoDBDuckDB.Adapter.connect(database: ":memory:")

selecto =
  Selecto.configure(domain, conn,
    adapter: SelectoDBDuckDB.Adapter
  )
```

## Notes

- Placeholder style is `$N`.
- Identifier quoting uses double quotes.
- Streaming is not currently supported.
- Includes adapter callbacks for `execute_raw/3`, `validate_connection/1`,
  `connection_info/1`, and `transaction/3`.
- Implements portable flat writes, arbitrary `RETURNING`, atomic batches, and
  generated-key graphs. DuckDB `MERGE` is not advertised because Duckdbex's
  prepared path cannot bind its parameters safely; graphs use one ordered
  transaction instead.

## Local Workspace Development

For local multi-repo development against vendored ecosystem packages, set:

```bash
SELECTO_ECOSYSTEM_USE_LOCAL=true
```

When enabled, this package resolves `{:selecto, path: "../selecto"}`.
