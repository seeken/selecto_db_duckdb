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

    # A per-column COLLATE is invisible to the catalog views: the table's
    # columns are all reported nondeterministic.
    {:ok, _} =
      Adapter.execute(
        conn,
        "CREATE TABLE coll (id INTEGER PRIMARY KEY, org_key VARCHAR COLLATE NOCASE, other VARCHAR)",
        [],
        []
      )

    {:ok, collated} = Adapter.column_catalog(conn, "coll")
    assert collated["org_key"].deterministic == false
    assert collated["other"].deterministic == false

    # Table names fold case; a schema-qualified name reads that schema.
    {:ok, _} = Adapter.execute(conn, "CREATE SCHEMA s2", [], [])

    {:ok, _} =
      Adapter.execute(conn, "CREATE TABLE s2.cat (id INTEGER, price DECIMAL(5,1))", [], [])

    assert {:ok, %{"org_key" => _}} = Adapter.column_catalog(conn, "CAT")
    assert {:ok, qualified} = Adapter.column_catalog(conn, "s2.cat")
    assert qualified["price"].type == "decimal(5,1)"

    {:ok, _} = Adapter.execute(conn, "SET default_collation = 'nocase'", [], [])
    {:ok, columns} = Adapter.column_catalog(conn, "cat")
    assert columns["org_key"].deterministic == false
  end
end
