defmodule SelectoDBDuckDB.GroupingSetsTest do
  use ExUnit.Case, async: true

  alias Selecto.Expr, as: X
  alias SelectoDBDuckDB.Adapter

  setup do
    assert {:ok, conn} = Adapter.connect(database: ":memory:")
    on_exit(fn -> Duckdbex.release(conn) end)

    for sql <- [
          "CREATE TABLE sales (id INTEGER PRIMARY KEY, region VARCHAR, channel VARCHAR, tier INTEGER, amount INTEGER)",
          "INSERT INTO sales VALUES (1,'east','web',1,10),(2,'east','web',2,20),(3,'east','store',1,5)," <>
            "(4,'west','web',1,7),(5,'west',NULL,2,3),(6,NULL,'store',1,40)"
        ] do
      assert {:ok, _} = Adapter.execute(conn, sql, [], [])
    end

    %{base: Selecto.configure(domain(), conn, adapter: Adapter)}
  end

  test "adapter reports cube and grouping sets" do
    assert Adapter.supports?(:cube)
    assert Adapter.supports?(:grouping_sets)
  end

  test "a native cube with the GROUPING() marker", %{base: base} do
    query =
      base |> measures(["region", "channel"]) |> Selecto.group_by(X.cube(["region", "channel"]))

    {sql, []} = Selecto.to_sql(query)
    assert sql =~ "CUBE("

    assert sorted_rows(query) ==
             Enum.sort([
               ["east", "web", 2, 30, 0],
               ["east", "store", 1, 5, 0],
               ["west", "web", 1, 7, 0],
               ["west", nil, 1, 3, 0],
               [nil, "store", 1, 40, 0],
               ["east", nil, 3, 35, 1],
               ["west", nil, 2, 10, 1],
               [nil, nil, 1, 40, 1],
               [nil, "web", 3, 37, 2],
               [nil, "store", 2, 45, 2],
               [nil, nil, 1, 3, 2],
               [nil, nil, 6, 85, 3]
             ])
  end

  test "native grouping sets ordered by marker and total, limited", %{base: base} do
    query =
      base
      |> measures(["region", "channel"])
      |> Selecto.group_by(X.grouping_sets([["region"], ["channel"], []]))
      |> Selecto.order_by([{{:grouping, ["region", "channel"]}, :asc}, {{:sum, "amount"}, :desc}])
      |> Selecto.limit(4)

    {sql, _params} = Selecto.to_sql(query)
    assert sql =~ "GROUPING SETS ("

    assert rows(query) == [
             [nil, nil, 1, 40, 1],
             ["east", nil, 3, 35, 1],
             ["west", nil, 2, 10, 1],
             [nil, "store", 2, 45, 2]
           ]
  end

  defp measures(query, dimensions) do
    Selecto.select(
      query,
      dimensions ++
        [
          X.as(X.count(), "n"),
          X.as({:sum, "amount"}, "total"),
          X.as({:grouping, dimensions}, "marker")
        ]
    )
  end

  defp sorted_rows(query), do: query |> rows() |> Enum.sort()

  defp rows(query) do
    assert {:ok, {rows, _columns, _aliases}} = Selecto.execute(query)
    Enum.map(rows, fn row -> Enum.map(row, &integer/1) end)
  end

  # DuckDB returns SUM and COUNT as HUGEINT / BIGINT.
  defp integer({upper, lower}) when is_integer(upper) and is_integer(lower),
    do: Duckdbex.hugeint_to_integer({upper, lower})

  defp integer(%Decimal{} = value), do: Decimal.to_integer(value)
  defp integer(value), do: value

  defp domain do
    %{
      name: "sales",
      source: %{
        source_table: "sales",
        primary_key: :id,
        fields: [:id, :region, :channel, :tier, :amount],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          region: %{type: :string},
          channel: %{type: :string},
          tier: %{type: :integer},
          amount: %{type: :integer}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }
  end
end
