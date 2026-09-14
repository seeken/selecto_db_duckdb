defmodule SelectoDBDuckDB.DecimalParameterTest do
  use ExUnit.Case, async: true
  alias SelectoDBDuckDB.Adapter

  test "decimal parameters retain their own scale through the public finalization seam" do
    {:ok, conn} = Adapter.connect(database: ":memory:")
    on_exit(fn -> Adapter.disconnect(conn) end)
    assert {:ok, _} = Adapter.execute(conn, "CREATE TABLE decimals (value DECIMAL(38,4))", [], [])

    assert {:ok, _} = Adapter.execute(conn, "INSERT INTO decimals VALUES (1.2345)", [], [])

    for {op, text, count} <- [
          {"=", "1.23451", 0},
          {"<", "1.23451", 1},
          {">", "1", 1},
          {">", "1.2", 1}
        ] do
      {query, params} =
        Selecto.SQL.Params.finalize(
          ["SELECT COUNT(*) FROM decimals WHERE value ", op, " ", {:param, Decimal.new(text)}],
          adapter: Adapter
        )

      assert {:ok, %{rows: [[^count]]}} = Adapter.execute(conn, query, params, [])
    end

    assert {:ok, _} =
             Adapter.execute(
               conn,
               "INSERT INTO decimals VALUES (9999999999999999999999999999999999.0001)",
               [],
               []
             )

    {query, params} =
      Selecto.SQL.Params.finalize(
        [
          "SELECT COUNT(*) FROM decimals WHERE value = ",
          {:param, Decimal.new("9999999999999999999999999999999999.0001")}
        ],
        adapter: Adapter
      )

    assert {:ok, %{rows: [[1]]}} = Adapter.execute(conn, query, params, [])

    assert_raise ArgumentError, fn -> Adapter.parameter_placeholder(1, Decimal.new("1e38")) end
    assert_raise ArgumentError, fn -> Adapter.parameter_placeholder(1, Decimal.new("NaN")) end
  end
end
