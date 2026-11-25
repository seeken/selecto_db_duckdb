defmodule SelectoDBDuckDB.CoreFixesTest do
  use ExUnit.Case, async: false
  alias SelectoDBDuckDB.Adapter

  defp domain(type, extra \\ %{}) do
    %{
      name: "Clock fixes",
      source: %{
        source_table: "fix_events",
        primary_key: :id,
        fields: [:id, :occurred, :age],
        associations: %{},
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          occurred: Map.merge(%{type: type}, extra),
          age: %{
            type: :integer,
            computed: %{
              kind: :expression,
              expression: ["days_between", ["field", "occurred"], ["now"]]
            }
          }
        }
      },
      schemas: %{},
      joins: %{}
    }
  end

  defp base(conn, domain) do
    Selecto.configure(domain, Selecto.Runtime.Context.new(Adapter, conn), adapter: Adapter)
  end

  defp rows(query) do
    assert {:ok, {rows, _, _}} = Selecto.execute(query, analyze_complexity: false)
    rows
  end

  test "unbound shortcuts use dynamic database date" do
    {:ok, conn} = Adapter.connect(database: ":memory:")
    on_exit(fn -> Adapter.disconnect(conn) end)
    # The driver's stripped build needs ICU for its database clock/timezones.
    # Hosts can supply an installed extension without changing the shared cache.
    if extension = System.get_env("SELECTO_TEST_DUCKDB_ICU") do
      assert {:ok, _} =
               Adapter.execute(
                 conn,
                 "LOAD '" <> String.replace(extension, "'", "''") <> "'",
                 [],
                 []
               )
    end

    for sql <- [
          "CREATE TABLE fix_events(id INTEGER, occurred DATE)",
          "INSERT INTO fix_events VALUES(1,CURRENT_DATE),(2,CURRENT_DATE-1)"
        ] do
      assert {:ok, _} = Adapter.execute(conn, sql, [], [])
    end

    for zone <- [nil, "Asia/Tokyo"], shortcut <- ["today", "last_month", "mtd_all_years"] do
      q = base(conn, domain(:date)) |> Selecto.select(["id"])
      q = if zone, do: Selecto.timezone(q, zone), else: q

      {sql, _, _} =
        Selecto.gen_sql(Selecto.filter(q, [Selecto.Expr.date_shortcut("occurred", shortcut)]), [])

      assert sql =~ "CURRENT_"
    end

    assert rows(
             base(conn, domain(:date))
             |> Selecto.select(["id"])
             |> Selecto.filter([Selecto.Expr.date_shortcut("occurred", "today")])
           ) == [[1]]
  end

  test "all dynamic calendar shortcuts agree with bound database date" do
    {:ok, conn} = Adapter.connect(database: ":memory:")
    on_exit(fn -> Adapter.disconnect(conn) end)

    if extension = System.get_env("SELECTO_TEST_DUCKDB_ICU"),
      do: Adapter.execute(conn, "LOAD '" <> String.replace(extension, "'", "''") <> "'", [], [])

    assert {:ok, _} =
             Adapter.execute(conn, "CREATE TABLE fix_events(id INTEGER, occurred DATE)", [], [])

    assert {:ok, _} =
             Adapter.execute(
               conn,
               "INSERT INTO fix_events SELECT n, CAST(CURRENT_DATE + CAST(n AS INTEGER) AS DATE) FROM range(-400,401,5) t(n)",
               [],
               []
             )

    assert {:ok, result} =
             Adapter.execute(
               conn,
               "SELECT CAST(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo' AS DATE)",
               [],
               []
             )

    %{rows: [[{year, month, day}]]} = result
    today = Date.new!(year, month, day) |> Date.to_iso8601()

    base =
      base(conn, domain(:date))
      |> Selecto.timezone("Asia/Tokyo")
      |> Selecto.select(["id"])
      |> Selecto.order_by(["id"])

    for shortcut <-
          ~w(today yesterday tomorrow this_week last_week next_week this_month last_month next_month mtd this_quarter last_quarter next_quarter qtd this_year last_year next_year ytd last_7_days last_14_days last_30_days last_90_days next_7_days next_30_days last_3_months this_and_last_month this_and_last_year mtd_all_years qtd_all_years ytd_all_years) do
      query = Selecto.filter(base, [Selecto.Expr.date_shortcut("occurred", shortcut)])
      assert rows(query) == rows(Selecto.as_of(query, today)), shortcut
    end
  end

  test "bound calendar age works over timestamp storage without ICU" do
    {:ok, conn} = Adapter.connect(database: ":memory:")
    on_exit(fn -> Adapter.disconnect(conn) end)

    assert {:ok, _} =
             Adapter.execute(
               conn,
               "CREATE TABLE fix_events(id INTEGER, occurred TIMESTAMP)",
               [],
               []
             )

    assert {:ok, _} =
             Adapter.execute(
               conn,
               "INSERT INTO fix_events VALUES(1,'2026-03-14 00:00:00'),(2,'2026-03-16 00:00:00')",
               [],
               []
             )

    q =
      base(conn, domain(:utc_datetime))
      |> Selecto.as_of("2026-03-15")
      |> Selecto.select(["id", "age"])
      |> Selecto.order_by(["id"])

    assert rows(q) == [[1, 1], [2, -1]]
  end
end
