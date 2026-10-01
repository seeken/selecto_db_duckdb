defmodule SelectoDBDuckDB.TenantForeignKeyGuardTest do
  use ExUnit.Case, async: false

  alias Selecto.Write.{Command, Error, Preview, Result}
  alias SelectoDBDuckDB.Adapter

  @field_types %{tenant_id: :integer, project_id: :integer, name: :string}

  @tenant_guard %{
    field: :project_id,
    relation: :projects,
    target_field: :id,
    tenant_field: :tenant_id,
    tenant_value: 7
  }

  setup do
    {:ok, connection} = Adapter.connect(database: ":memory:")

    execute!(
      connection,
      "CREATE TABLE projects (id BIGINT PRIMARY KEY, tenant_id BIGINT NOT NULL)"
    )

    execute!(connection, "CREATE SEQUENCE task_ids START 2")

    execute!(connection, """
    CREATE TABLE tasks (
      id BIGINT PRIMARY KEY DEFAULT nextval('task_ids'),
      tenant_id BIGINT NOT NULL,
      project_id BIGINT NOT NULL,
      name VARCHAR NOT NULL
    )
    """)

    execute!(connection, "INSERT INTO projects VALUES (70, 7), (80, 8)")
    execute!(connection, "INSERT INTO tasks VALUES (1, 7, 70, 'seed')")

    on_exit(fn -> Duckdbex.release(connection) end)
    %{connection: connection}
  end

  test "a tenant guard binds the referenced row's tenant behind an alias" do
    assert {:ok, %Preview{statements: [%{text: insert_sql, params: [7, 80, "t", 80, 7]}]}} =
             Adapter.preview_write(:unused, insert!(80), [])

    assert insert_sql =~
             ~s|WHERE EXISTS (SELECT 1 FROM "projects" AS "selecto_fk_parent" | <>
               ~s|WHERE "selecto_fk_parent"."id" = (SELECT CAST($4 AS INTEGER)) | <>
               ~s|AND "selecto_fk_parent"."tenant_id" = $5)|

    assert {:ok, %Preview{statements: [%{text: update_sql, params: [80, "t", 1, 7, 80, 7]}]}} =
             Adapter.preview_write(:unused, update!(80), [])

    assert update_sql =~
             ~s|AND EXISTS (SELECT 1 FROM "projects" AS "selecto_fk_parent" | <>
               ~s|WHERE "selecto_fk_parent"."id" = (SELECT CAST($5 AS INTEGER)) | <>
               ~s|AND "selecto_fk_parent"."tenant_id" = $6)|
  end

  test "a guard naming a tenant field without a usable tenant value fails closed" do
    base = Map.drop(@tenant_guard, [:tenant_field, :tenant_value])

    for invalid <- [
          Map.put(base, :tenant_field, :tenant_id),
          Map.merge(base, %{tenant_field: :tenant_id, tenant_value: nil}),
          Map.merge(base, %{tenant_field: 7, tenant_value: 7}),
          Map.merge(base, %{tenant_field: nil, tenant_value: 7}),
          Map.merge(base, %{tenant_field: " ", tenant_value: 7})
        ] do
      assert {:error, %Error{type: :invalid_foreign_key_guard}} =
               Adapter.preview_write(:unused, insert!(80, [invalid]), [])
    end
  end

  test "a tenant-7 write cannot reference tenant 8's parent", %{connection: connection} do
    assert {:error, %Error{type: :cardinality_mismatch, details: %{actual: 0}}} =
             Adapter.execute_write(connection, insert!(80), [])

    assert {:error, %Error{type: :cardinality_mismatch, details: %{actual: 0}}} =
             Adapter.execute_write(connection, update!(80), [])

    assert rows!(connection, "SELECT id, tenant_id, project_id, name FROM tasks ORDER BY id") ==
             [[1, 7, 70, "seed"]]

    assert {:ok, %Result{affected_rows: 1}} = Adapter.execute_write(connection, insert!(70), [])
    assert {:ok, %Result{affected_rows: 1}} = Adapter.execute_write(connection, update!(70), [])

    assert rows!(connection, "SELECT tenant_id, project_id, name FROM tasks ORDER BY id") ==
             [[7, 70, "t"], [7, 70, "t"]]
  end

  test "update guards are numbered at compile time, not by rewriting identifier text", %{
    connection: connection
  } do
    execute!(connection, ~s|CREATE TABLE "projects$1" (id BIGINT PRIMARY KEY, tenant_id BIGINT)|)
    execute!(connection, ~s|INSERT INTO "projects$1" VALUES (70, 7), (80, 8)|)
    guard = %{@tenant_guard | relation: "projects$1"}

    command = %{update!(80) | metadata: %{field_types: @field_types, foreign_key_guards: [guard]}}

    assert {:ok, %Preview{statements: [%{text: sql}]}} =
             Adapter.preview_write(:unused, command, [])

    assert sql =~ ~s|FROM "projects$1" AS "selecto_fk_parent"|

    assert {:error, %Error{type: :cardinality_mismatch, details: %{actual: 0}}} =
             Adapter.execute_write(connection, command, [])

    allowed = %{update!(70) | metadata: %{field_types: @field_types, foreign_key_guards: [guard]}}
    assert {:ok, %Result{affected_rows: 1}} = Adapter.execute_write(connection, allowed, [])
  end

  defp insert!(project_id, guards \\ [@tenant_guard]) do
    command!(%{
      operation: :insert,
      relation: :tasks,
      assignments: [
        %{field: :tenant_id, value: {:literal, 7}},
        %{field: :project_id, value: {:literal, project_id}},
        %{field: :name, value: {:literal, "t"}}
      ],
      metadata: %{field_types: @field_types, foreign_key_guards: guards}
    })
  end

  defp update!(project_id) do
    command!(%{
      operation: :update,
      relation: :tasks,
      assignments: [
        %{field: :project_id, value: {:literal, project_id}},
        %{field: :name, value: {:literal, "t"}}
      ],
      predicate:
        {:and, [{:eq, {:field, :id}, {:literal, 1}}, {:eq, {:field, :tenant_id}, {:literal, 7}}]},
      metadata: %{field_types: @field_types, foreign_key_guards: [@tenant_guard]}
    })
  end

  defp command!(attrs) do
    {:ok, command} = Command.new(attrs)
    command
  end

  defp execute!(connection, sql), do: assert({:ok, _} = Adapter.execute(connection, sql, [], []))

  defp rows!(connection, sql) do
    assert {:ok, %{rows: rows}} = Adapter.execute(connection, sql, [], [])
    rows
  end
end
