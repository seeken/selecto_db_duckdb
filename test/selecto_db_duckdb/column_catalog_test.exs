defmodule SelectoDBDuckDB.ColumnCatalogTest do
  # column_catalog/2 reports information_schema data types with precision and
  # scale, and marks every column nondeterministic under a session
  # default_collation (query_enforced_scalar v2 verified metadata).
  use ExUnit.Case, async: true

  alias SelectoDBDuckDB.Adapter

  test "types carry precision and scale; a default collation is nondeterministic" do
    {:ok, conn} = Adapter.connect(database: ":memory:")

    {:ok, _} =
      Adapter.execute(
        conn,
        "CREATE TABLE cat (id INTEGER PRIMARY KEY, org_key VARCHAR, price DECIMAL(12,2), stamp TIMESTAMP)",
        [],
        []
      )

    {:ok, columns} = Adapter.column_catalog(conn, "cat")
    assert columns["org_key"] == %{type: "varchar", deterministic: true}
    assert columns["price"].type == "decimal(12,2)"
    assert columns["id"].type == "integer"
    assert columns["stamp"].type == "timestamp"
    assert {:ok, %{}} = Adapter.column_catalog(conn, "no_such_table")

    {:ok, _} = Adapter.execute(conn, "SET default_collation = 'nocase'", [], [])
    {:ok, columns} = Adapter.column_catalog(conn, "cat")
    assert columns["org_key"].deterministic == false
  end
end
