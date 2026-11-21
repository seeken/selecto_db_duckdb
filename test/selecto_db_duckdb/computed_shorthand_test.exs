defmodule SelectoDBDuckDB.ComputedShorthandTest do
  use ExUnit.Case, async: false

  alias Selecto.Dialect.ComputedValue
  alias SelectoDBDuckDB.{Adapter, Dialect}

  setup do
    {:ok, conn} = Adapter.connect(database: ":memory:")
    %{conn: conn}
  end

  test "days_between takes the date difference whatever the time of day", %{conn: conn} do
    for {start, finish, expected} <- [
          {"TIMESTAMP '2026-06-30 23:59:00'", "DATE '2026-06-30'", 0},
          {"TIMESTAMP '2026-06-24 00:00:01'", "DATE '2026-06-30'", 6},
          {"DATE '2026-07-02'", "DATE '2026-06-30'", -2},
          {"NULL", "DATE '2026-06-30'", nil}
        ] do
      assert {:ok, sql} =
               Dialect.render_computed_value(
                 %ComputedValue{
                   operation: :days_between,
                   expression: [start, finish],
                   type: "integer"
                 },
                 %{}
               )

      assert scalar(conn, sql) == expected
    end
  end

  test "canonical casts bind their operand", %{conn: conn} do
    assert {:ok, sql} =
             Dialect.render_computed_value(
               %ComputedValue{operation: :cast, expression: {:param, "label"}, type: "string"},
               %{}
             )

    assert scalar(conn, sql) == "label"

    for fragment <- [
          %ComputedValue{operation: :cast, expression: {:param, 1}, type: "bytea"},
          %ComputedValue{operation: :days_between, expression: ["a"], type: "integer"},
          %ComputedValue{operation: :json_text, expression: "doc", path: [{:param, "a"}]}
        ] do
      assert {:error, %Selecto.Error{details: %{unsupported_feature: :computed_value}}} =
               Dialect.render_computed_value(fragment, %{})
    end
  end

  test "temporal_delta and bucket columns run end to end", %{conn: conn} do
    for sql <- [
          "CREATE TABLE events (id INTEGER PRIMARY KEY, starts_on DATE)",
          "CREATE TABLE tickets (id INTEGER PRIMARY KEY, event_id INTEGER, purchased_at TIMESTAMP)",
          "INSERT INTO events VALUES (1, '2026-06-30'), (2, NULL)",
          "INSERT INTO tickets VALUES (1, 1, '2026-06-30 23:59:00'), (5, 1, '2026-07-02 09:00:00'), (6, 2, '2026-03-01 09:00:00')"
        ],
        do: assert({:ok, _} = Adapter.execute(conn, sql, [], []))

    query =
      domain()
      |> Selecto.configure(conn, adapter: Adapter)
      |> Selecto.select(["id", "lead_days", "lead_bucket"])
      |> Selecto.order_by("id")

    assert {:ok, {rows, _columns, _aliases}} = Selecto.execute(query, analyze_complexity: false)
    assert rows == [[1, 0, "soon"], [5, -2, "after"], [6, nil, "Unknown"]]
  end

  defp scalar(conn, sql) do
    {sql, params} = Selecto.SQL.Params.finalize(["SELECT ", sql], adapter: Adapter)
    assert {:ok, %{rows: [[value]]}} = Adapter.execute(conn, sql, params, [])
    value
  end

  defp domain do
    %{
      name: "Tickets",
      source: %{
        source_table: "tickets",
        primary_key: :id,
        fields: [:id, :event_id, :purchased_at, :lead_days, :lead_bucket],
        columns: %{
          id: %{type: :integer},
          event_id: %{type: :integer},
          purchased_at: %{type: :naive_datetime},
          lead_days: %{
            type: :integer,
            computed: %{kind: :temporal_delta, from: "purchased_at", to: "event.starts_on"}
          },
          lead_bucket: %{
            type: :string,
            computed: %{
              kind: :bucket,
              field: "lead_days",
              otherwise: "Unknown",
              buckets: [%{label: "after", max: -1}, %{label: "soon", min: 0, max: 6}]
            }
          }
        },
        associations: %{
          event: %{queryable: :event, field: :event, owner_key: :event_id, related_key: :id}
        },
        redact_fields: []
      },
      schemas: %{
        event: %{
          source_table: "events",
          primary_key: :id,
          fields: [:id, :starts_on],
          columns: %{id: %{type: :integer}, starts_on: %{type: :date}},
          associations: %{},
          redact_fields: []
        }
      },
      joins: %{event: %{type: :left}}
    }
  end
end
