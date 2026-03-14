defmodule SelectoDBDuckDB.AdapterTest do
  use ExUnit.Case, async: true

  test "adapter exposes the selecto adapter contract" do
    assert Code.ensure_loaded?(SelectoDBDuckDB.Adapter)
    assert function_exported?(SelectoDBDuckDB.Adapter, :name, 0)
    assert function_exported?(SelectoDBDuckDB.Adapter, :connect, 1)
    assert function_exported?(SelectoDBDuckDB.Adapter, :execute, 4)
    assert function_exported?(SelectoDBDuckDB.Adapter, :placeholder, 1)
    assert function_exported?(SelectoDBDuckDB.Adapter, :quote_identifier, 1)
    assert function_exported?(SelectoDBDuckDB.Adapter, :supports?, 1)
    assert function_exported?(SelectoDBDuckDB.Adapter, :execute_raw, 3)
    assert function_exported?(SelectoDBDuckDB.Adapter, :validate_connection, 1)
    assert function_exported?(SelectoDBDuckDB.Adapter, :connection_info, 1)
    assert function_exported?(SelectoDBDuckDB.Adapter, :transaction, 3)
  end

  test "duckdb adapter reports expected placeholder and quoting strategy" do
    assert SelectoDBDuckDB.Adapter.placeholder(3) |> IO.iodata_to_binary() == "$3"
    assert SelectoDBDuckDB.Adapter.quote_identifier("order") == "\"order\""
  end

  test "duckdb adapter executes a simple query" do
    assert {:ok, conn} = SelectoDBDuckDB.Adapter.connect(database: ":memory:")

    assert {:ok, %{rows: [[1]], columns: ["value"]}} =
             SelectoDBDuckDB.Adapter.execute(conn, "SELECT 1 AS value", [], [])

    assert :ok = Duckdbex.release(conn)
  end

  test "duckdb adapter exposes execute_raw and connection helpers" do
    assert {:ok, conn} = SelectoDBDuckDB.Adapter.connect(database: ":memory:")

    assert :ok = SelectoDBDuckDB.Adapter.validate_connection(conn)

    assert %{type: :duckdb, connection: :duckdbex, status: :connected} =
             SelectoDBDuckDB.Adapter.connection_info(conn)

    assert {:ok, %{rows: [[1]], columns: ["value"]}} =
             SelectoDBDuckDB.Adapter.execute_raw(conn, "SELECT 1 AS value", [])

    assert :ok = Duckdbex.release(conn)
  end

  test "duckdb adapter transaction commits and rolls back" do
    assert {:ok, conn} = SelectoDBDuckDB.Adapter.connect(database: ":memory:")

    assert {:ok, _} =
             SelectoDBDuckDB.Adapter.execute(
               conn,
               "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL)",
               [],
               []
             )

    assert {:ok, :inserted} =
             SelectoDBDuckDB.Adapter.transaction(conn, fn tx_conn ->
               SelectoDBDuckDB.Adapter.execute(
                 tx_conn,
                 "INSERT INTO users (id, name) VALUES ($1, $2)",
                 [1, "Ada"],
                 []
               )

               :inserted
             end)

    assert {:error, :force_rollback} =
             SelectoDBDuckDB.Adapter.transaction(conn, fn tx_conn ->
               SelectoDBDuckDB.Adapter.execute(
                 tx_conn,
                 "INSERT INTO users (id, name) VALUES ($1, $2)",
                 [2, "Grace"],
                 []
               )

               {:error, :force_rollback}
             end)

    assert {:ok, %{rows: [[count]], columns: ["total"]}} =
             SelectoDBDuckDB.Adapter.execute(conn, "SELECT COUNT(*) AS total FROM users", [], [])

    assert normalize_cell(count) == 1
    assert :ok = Duckdbex.release(conn)
  end

  test "duckdb adapter rejects invalid connection options" do
    assert SelectoDBDuckDB.Adapter.connect(123) == {:error, {:invalid_connection_options, 123}}
  end

  test "duckdb adapter rejects invalid connection values" do
    assert SelectoDBDuckDB.Adapter.execute(:invalid, "SELECT 1", [], []) ==
             {:error, {:invalid_connection, :invalid}}
  end

  test "duckdb adapter reports rollup support and no stream support" do
    assert SelectoDBDuckDB.Adapter.supports?(:rollup)
    refute SelectoDBDuckDB.Adapter.supports?(:stream)
  end

  defp normalize_cell({upper, lower}) when is_integer(upper) and is_integer(lower) do
    Duckdbex.hugeint_to_integer({upper, lower})
  end

  defp normalize_cell(value), do: value
end
