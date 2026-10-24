defmodule SelectoDBDuckDB.PostImageTest do
  use ExUnit.Case, async: false

  # TW-09 post-image: a guarded write's rows are re-read by key under the
  # trusted scope after the statement, in the relation the write resolved.

  alias SelectoDBDuckDB.Adapter
  alias Selecto.Write.{Command, Error}

  setup do
    {:ok, c} = Adapter.connect(database: ":memory:")

    exec!(
      c,
      "CREATE TABLE tw (id BIGINT PRIMARY KEY, org_key VARCHAR, site VARCHAR, name VARCHAR)"
    )

    exec!(c, "INSERT INTO tw VALUES (1, 'T1', 'S1', 'a')")
    on_exit(fn -> Duckdbex.release(c) end)
    %{connection: c}
  end

  defp exec!(c, sql), do: {:ok, _} = Adapter.execute(c, sql, [], [])

  defp update(assignments, scope, relation \\ "tw") do
    {:ok, command} =
      Command.new(%{
        operation: :update,
        relation: relation,
        predicate: {:and, [{:eq, {:field, "id"}, {:literal, 1}}, scope]},
        assignments: Enum.map(assignments, fn {f, v} -> %{field: f, value: {:literal, v}} end),
        expected_cardinality: {:exactly, 1},
        metadata: %{post_image: %{predicate: scope, key: ["id"]}}
      })

    command
  end

  defp insert(values, guard) do
    {:ok, command} =
      Command.new(%{
        operation: :insert,
        relation: "tw",
        assignments: Enum.map(values, fn {f, v} -> %{field: f, value: {:literal, v}} end),
        returning: [:id],
        metadata: %{post_image: %{predicate: guard, key: ["id"]}}
      })

    command
  end

  defp site(value), do: {:eq, {:field, "site"}, {:literal, value}}

  defp stored(c, relation \\ "memory.main.tw") do
    {:ok, %{rows: rows}} =
      Adapter.execute(c, "SELECT id, org_key, site, name FROM #{relation} ORDER BY id", [], [])

    rows
  end

  defp violation?(result),
    do: match?({:error, %Error{details: %{code: :query_rule_violation}}}, result)

  defp not_evaluable?(result),
    do: match?({:error, %Error{details: %{code: :query_rule_not_evaluable}}}, result)

  test "an update that assigns the scoped column rolls back", %{connection: c} do
    assert violation?(Adapter.execute_write_unsafe(c, update([{"site", "S9"}], site("S1")), []))
    assert stored(c) == [[1, "T1", "S1", "a"]]
  end

  test "a temp table shadowing main is the relation re-read", %{connection: c} do
    exec!(
      c,
      "CREATE TEMP TABLE tw (id BIGINT PRIMARY KEY, org_key VARCHAR, site VARCHAR, name VARCHAR)"
    )

    exec!(c, "INSERT INTO temp.main.tw VALUES (1, 'T1', 'S1', 'a')")

    # memory.main.tw still holds an in-scope row with the same key.
    assert violation?(Adapter.execute_write_unsafe(c, update([{"site", "S9"}], site("S1")), []))
    assert stored(c, "temp.main.tw") == [[1, "T1", "S1", "a"]]
    assert stored(c) == [[1, "T1", "S1", "a"]]
  end

  test "a schema-qualified relation is re-read in that schema", %{connection: c} do
    exec!(c, "CREATE SCHEMA s2")

    exec!(
      c,
      "CREATE TABLE s2.tw (id BIGINT PRIMARY KEY, org_key VARCHAR, site VARCHAR, name VARCHAR)"
    )

    exec!(c, "INSERT INTO s2.tw VALUES (1, 'T1', 'S1', 'a')")

    assert violation?(
             Adapter.execute_write_unsafe(c, update([{"site", "S9"}], site("S1"), "s2.tw"), [])
           )

    assert {:ok, %{affected_rows: 1}} =
             Adapter.execute_write_unsafe(c, update([{"name", "u"}], site("S1"), "s2.tw"), [])

    assert stored(c, "s2.tw") == [[1, "T1", "S1", "u"]]
  end

  test "a view and a table without a usable key are refused", %{connection: c} do
    exec!(c, "CREATE TABLE dk (id BIGINT, site VARCHAR, name VARCHAR)")
    exec!(c, "INSERT INTO dk VALUES (1, 'S1', 'x'), (1, 'S2', 'y')")

    assert not_evaluable?(
             Adapter.execute_write_unsafe(c, update([{"name", "u"}], site("S1"), "dk"), [])
           )

    # UNIQUE over a nullable column does not identify a row.
    exec!(c, "CREATE TABLE nk (id BIGINT UNIQUE, site VARCHAR, name VARCHAR)")
    exec!(c, "INSERT INTO nk VALUES (1, 'S1', 'x')")

    assert not_evaluable?(
             Adapter.execute_write_unsafe(c, update([{"name", "u"}], site("S1"), "nk"), [])
           )

    exec!(c, "CREATE VIEW tw_v AS SELECT * FROM tw")

    refute match?(
             {:ok, _},
             Adapter.execute_write_unsafe(c, update([{"name", "u"}], site("S1"), "tw_v"), [])
           )

    {:ok, %{rows: rows}} = Adapter.execute(c, "SELECT name FROM dk ORDER BY site", [], [])
    assert rows == [["x"], ["y"]]
  end

  test "in-scope writes apply; inserts are re-read under the full guard", %{connection: c} do
    assert {:ok, %{affected_rows: 1, rows: []}} =
             Adapter.execute_write_unsafe(c, update([{"name", "u"}], site("S1")), [])

    assert {:ok, %{affected_rows: 1, rows: [%{"id" => 2}]}} =
             Adapter.execute_write_unsafe(
               c,
               insert([{"id", 2}, {"org_key", "T1"}, {"site", "S1"}, {"name", "n"}], site("S1")),
               []
             )

    assert violation?(
             Adapter.execute_write_unsafe(
               c,
               insert([{"id", 3}, {"org_key", "T1"}, {"site", "S2"}, {"name", "n"}], site("S1")),
               []
             )
           )

    assert stored(c) == [[1, "T1", "S1", "u"], [2, "T1", "S1", "n"]]
  end
end
