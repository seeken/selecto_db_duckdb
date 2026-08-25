defmodule SelectoDBDuckDB.Adapter do
  @moduledoc """
  DuckDB adapter for Selecto backed by `Duckdbex`.
  """

  @behaviour Selecto.DB.Adapter
  @behaviour Selecto.DB.WriteAdapter

  alias Selecto.Write.{Batch, Command, Error, Graph, Preview, Result}
  alias Selecto.Write.Graph.Materializer
  alias SelectoDBDuckDB.WriteCompiler

  @missing_dependency {:adapter_dependency_missing, :duckdbex}
  @transaction_depth_key {__MODULE__, :transaction_depth}

  @impl true
  def name, do: :duckdb

  @impl true
  def dialect, do: SelectoDBDuckDB.Dialect

  @impl true
  def capability(feature), do: %{feature: feature, supported?: supports?(feature)}

  @impl true
  def normalize_type(type) when is_binary(type) do
    case type |> String.trim() |> String.downcase() do
      value when value in ["tinyint", "smallint", "integer", "bigint", "hugeint"] -> :integer
      value when value in ["real", "float", "double"] -> :float
      value when value in ["decimal", "numeric"] -> :decimal
      value when value in ["varchar", "char", "text"] -> :string
      "boolean" -> :boolean
      "date" -> :date
      "time" -> :time
      "timestamp" -> :naive_datetime
      "timestamptz" -> :utc_datetime
      "json" -> :map
      "blob" -> :binary
      "uuid" -> :uuid
      _unknown -> type
    end
  end

  def normalize_type(type), do: Selecto.TypeSystem.normalize_type(type)

  @impl true
  def type_family(type), do: type |> normalize_type() |> Selecto.TypeFamily.of()

  @impl true
  def normalize_execution_result(%{rows: rows, columns: columns} = result) do
    {:ok, %{result | rows: rows || [], columns: Enum.map(columns || [], &to_string/1)}}
  end

  def normalize_execution_result(result), do: {:error, {:invalid_adapter_result, result}}

  @impl true
  def normalize_error(%Selecto.Error{} = error), do: error
  def normalize_error(reason), do: Selecto.Error.from_reason(reason)

  @impl true
  def connect(connection) when is_reference(connection), do: {:ok, connection}
  def connect(opts) when is_map(opts), do: connect(Map.to_list(opts))

  def connect(opts) when is_list(opts) do
    if dependency_available?() do
      database = Keyword.get(opts, :database) || Keyword.get(opts, :path) || ":memory:"
      config = Keyword.get(opts, :config)

      with {:ok, db} <- open_database(database, config),
           {:ok, conn} <- Duckdbex.connection(db) do
        :ok = Duckdbex.release(db)
        {:ok, conn}
      end
    else
      {:error, @missing_dependency}
    end
  end

  def connect(other), do: {:error, {:invalid_connection_options, other}}

  @impl true
  def disconnect(connection) when is_reference(connection), do: Duckdbex.release(connection)
  def disconnect(_connection), do: :ok

  @impl true
  def execute(connection, query, params, _opts) do
    resolved_connection = resolve_connection(connection)

    cond do
      not dependency_available?() ->
        {:error, @missing_dependency}

      not is_reference(resolved_connection) ->
        {:error, {:invalid_connection, connection}}

      true ->
        execute_query(resolved_connection, normalize_query(query), normalize_params(params || []))
    end
  end

  @impl true
  def execute_raw(connection, query, params) do
    execute(connection, query, params, [])
  end

  @impl true
  def placeholder(index), do: ["$", Integer.to_string(index)]

  @impl true
  def quote_identifier(identifier) when is_binary(identifier) do
    escaped = String.replace(identifier, "\"", "\"\"")
    "\"#{escaped}\""
  end

  def quote_identifier(identifier), do: identifier |> to_string() |> quote_identifier()

  @impl true
  def rollup_sql(grouped_clauses), do: ["ROLLUP( ", grouped_clauses, " )"]

  @impl true
  def supports?(feature) do
    feature in [:cte, :window_functions, :transactions, :rollup]
  end

  @impl Selecto.DB.WriteAdapter
  def write_capabilities(connection) do
    version = server_version(connection)

    %{
      protocol_version: Selecto.Write.Capabilities.protocol_version(),
      insert: true,
      update: true,
      upsert: true,
      delete: true,
      returning: true,
      generated_keys: :returning,
      transactions: true,
      atomic_batch: true,
      write_graph: true,
      dialect: :duckdb,
      server_version: version,
      merge: false,
      merge_available: merge_version?(version),
      merge_parameter_binding: false
    }
  end

  @impl Selecto.DB.WriteAdapter
  def preview_write(connection, write, opts \\ [])

  def preview_write(_connection, %Command{} = command, opts),
    do: WriteCompiler.preview(command, opts)

  def preview_write(_connection, %Batch{} = batch, opts), do: WriteCompiler.preview(batch, opts)
  def preview_write(_connection, %Graph{} = graph, opts), do: preview_graph(graph, opts)
  def preview_write(_connection, write, _opts), do: invalid_write_input(write)

  @impl Selecto.DB.WriteAdapter
  def execute_write(connection, write, opts \\ [])

  def execute_write(connection, %Command{} = command, opts) do
    with :ok <- Command.validate(command) do
      with_write_transaction(connection, opts, fn tx ->
        execute_write_command(tx, command, opts)
      end)
    end
  end

  def execute_write(connection, %Batch{} = batch, opts) do
    with :ok <- Batch.validate(batch) do
      with_write_transaction(connection, opts, fn tx ->
        Enum.reduce_while(batch.commands, {:ok, []}, fn command, {:ok, results} ->
          case execute_write_command(tx, command, opts) do
            {:ok, result} -> {:cont, {:ok, results ++ [result]}}
            {:error, _} = error -> {:halt, error}
          end
        end)
      end)
    end
  end

  def execute_write(connection, %Graph{} = graph, opts) do
    with :ok <- Graph.validate(graph) do
      with_write_transaction(connection, opts, fn tx -> execute_graph(tx, graph, opts) end)
    end
  end

  def execute_write(_connection, write, _opts), do: invalid_write_input(write)

  defp preview_graph(%Graph{} = graph, opts) do
    graph.nodes
    |> Enum.reduce_while({:ok, [], %{}}, fn node, {:ok, statements, results} ->
      with {:ok, materialized} <- Materializer.materialize_node(node, results),
           {:ok, node_statements} <- preview_graph_node(materialized, opts) do
        next_results = Map.merge(results, Materializer.symbolic_results(materialized))
        {:cont, {:ok, statements ++ node_statements, next_results}}
      else
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, statements, _results} ->
        {:ok,
         %Preview{
           statements: statements,
           metadata: %{
             dialect: :duckdb,
             atomic?: true,
             graph?: true,
             strategy: :ordered_fallback,
             merge: false,
             merge_reason: :prepared_parameters_unsupported
           }
         }}

      error ->
        error
    end
  end

  defp preview_graph_node(node, opts) do
    row_results =
      node
      |> Materializer.symbolic_results()
      |> Map.new(fn {{_node_id, row_id}, result} -> {row_id, result} end)

    with {:ok, cleanup} <- Materializer.delete_missing_command(node, row_results) do
      commands = Enum.map(node.rows, & &1.command)
      commands = if cleanup, do: commands ++ [cleanup], else: commands

      Enum.reduce_while(commands, {:ok, []}, fn command, {:ok, statements} ->
        case WriteCompiler.compile(command, opts) do
          {:ok, statement} -> {:cont, {:ok, statements ++ [statement]}}
          {:error, _} = error -> {:halt, error}
        end
      end)
    end
  end

  defp execute_graph(connection, graph, opts) do
    graph.nodes
    |> Enum.reduce_while({:ok, %{}, 0, []}, fn node, {:ok, results, affected_rows, strategies} ->
      with {:ok, materialized} <- Materializer.materialize_node(node, results),
           {:ok, node_results, node_affected} <-
             execute_graph_node(connection, materialized, opts) do
        next_results =
          Map.merge(
            results,
            Map.new(node_results, fn {row_id, result} -> {{node.id, row_id}, result} end)
          )

        {:cont,
         {:ok, next_results, affected_rows + node_affected,
          strategies ++ [{node.id, :ordered_fallback}]}}
      else
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, results, affected_rows, strategies} ->
        {:ok,
         %Result{
           operation: :graph,
           affected_rows: affected_rows,
           rows: Materializer.root_rows(graph, results),
           metadata:
             %{
               dialect: :duckdb,
               atomic?: true,
               node_strategies: Map.new(strategies)
             }
             |> Map.merge(Materializer.outcome_metadata(graph, results))
         }}

      error ->
        error
    end
  end

  defp execute_graph_node(connection, node, opts) do
    with {:ok, row_results, affected_rows} <- execute_graph_rows(connection, node.rows, opts),
         {:ok, cleanup} <- Materializer.delete_missing_command(node, row_results),
         {:ok, cleanup_affected} <- execute_graph_cleanup(connection, cleanup, opts) do
      {:ok, row_results, affected_rows + cleanup_affected}
    end
  end

  defp execute_graph_rows(connection, rows, opts) do
    Enum.reduce_while(rows, {:ok, %{}, 0}, fn row, {:ok, results, affected_rows} ->
      case execute_write_command(connection, row.command, opts) do
        {:ok, result} ->
          {:cont, {:ok, Map.put(results, row.id, result), affected_rows + result.affected_rows}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
  end

  defp execute_graph_cleanup(_connection, nil, _opts), do: {:ok, 0}

  defp execute_graph_cleanup(connection, command, opts) do
    case execute_write_command(connection, command, opts) do
      {:ok, result} -> {:ok, result.affected_rows}
      {:error, _} = error -> error
    end
  end

  defp execute_write_command(connection, command, opts) do
    with {:ok, statement} <- WriteCompiler.compile(command, opts),
         {:ok, query_result} <- execute(connection, statement.text, statement.params, opts),
         {:ok, affected_rows} <- enforce_cardinality(command, query_result) do
      {:ok,
       %Result{
         operation: command.operation,
         affected_rows: affected_rows,
         rows: result_rows(command, query_result),
         metadata: %{dialect: :duckdb}
       }}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, write_error(:execution_failed, reason)}
    end
  end

  defp enforce_cardinality(%Command{expected_cardinality: expected}, result) do
    affected_rows = Map.get(result, :num_rows, length(Map.get(result, :rows, [])))

    if cardinality_matches?(affected_rows, expected) do
      {:ok, affected_rows}
    else
      {:error,
       Error.new(:cardinality_mismatch, "write affected an unexpected number of rows",
         details: %{expected: expected, actual: affected_rows}
       )}
    end
  end

  defp cardinality_matches?(count, {:exactly, expected}), do: count == expected
  defp cardinality_matches?(count, {:at_most, expected}), do: count <= expected
  defp cardinality_matches?(count, {:at_least, expected}), do: count >= expected
  defp cardinality_matches?(count, {:between, minimum, maximum}), do: count in minimum..maximum
  defp cardinality_matches?(_count, :many), do: true

  defp result_rows(%Command{returning: :none}, _result), do: []
  defp result_rows(_command, %{columns: ["Count"]}), do: []

  defp result_rows(_command, %{rows: rows, columns: columns}) do
    Enum.map(rows, fn row -> Map.new(Enum.zip(columns, row)) end)
  end

  defp invalid_write_input(write) do
    {:error,
     Error.new(:invalid_command, "expected a portable write command, batch, or graph",
       details: %{actual: write}
     )}
  end

  defp with_write_transaction(connection, opts, fun) do
    case transaction(connection, fun, opts) do
      {:ok, result} -> {:ok, result}
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, write_error(:transaction_failed, reason)}
    end
  end

  defp write_error(type, reason),
    do: Error.adapter_failure(type, :duckdb, reason, "DuckDB write failed")

  defp server_version(connection) do
    case execute(connection, "SELECT version()", [], []) do
      {:ok, %{rows: [["v" <> version] | _]}} -> version
      {:ok, %{rows: [[version] | _]}} when is_binary(version) -> version
      _ -> nil
    end
  rescue
    _exception -> nil
  catch
    :exit, _reason -> nil
  end

  defp merge_version?(version) when is_binary(version) do
    case version |> String.split(".") |> Enum.take(2) |> Enum.map(&Integer.parse/1) do
      [{major, ""}, {minor, ""}] -> {major, minor} >= {1, 4}
      _ -> false
    end
  end

  defp merge_version?(_version), do: false

  @impl true
  def format_datetime(sel_iodata, "YYYY-MM-DD") do
    ["strftime(CAST(", sel_iodata, " AS TIMESTAMP), '%Y-%m-%d')"]
  end

  def format_datetime(sel_iodata, "YYYY-MM") do
    ["strftime(CAST(", sel_iodata, " AS TIMESTAMP), '%Y-%m')"]
  end

  def format_datetime(sel_iodata, "YYYY") do
    ["strftime(CAST(", sel_iodata, " AS TIMESTAMP), '%Y')"]
  end

  def format_datetime(sel_iodata, "YYYY-WW") do
    ["strftime(CAST(", sel_iodata, " AS TIMESTAMP), '%G-%V')"]
  end

  def format_datetime(sel_iodata, "YYYY-Q") do
    [
      "strftime(CAST(",
      sel_iodata,
      " AS TIMESTAMP), '%Y') || '-' || CAST(quarter(CAST(",
      sel_iodata,
      " AS TIMESTAMP)) AS VARCHAR)"
    ]
  end

  def format_datetime(sel_iodata, "MM") do
    ["strftime(CAST(", sel_iodata, " AS TIMESTAMP), '%m')"]
  end

  def format_datetime(sel_iodata, "DD") do
    ["strftime(CAST(", sel_iodata, " AS TIMESTAMP), '%d')"]
  end

  def format_datetime(sel_iodata, "D") do
    ["strftime(CAST(", sel_iodata, " AS TIMESTAMP), '%u')"]
  end

  def format_datetime(sel_iodata, "HH24") do
    ["strftime(CAST(", sel_iodata, " AS TIMESTAMP), '%H')"]
  end

  def format_datetime(sel_iodata, _format) do
    ["CAST(", sel_iodata, " AS VARCHAR)"]
  end

  @impl true
  def validate_connection(connection) do
    resolved_connection = resolve_connection(connection)

    cond do
      not dependency_available?() ->
        {:error, @missing_dependency}

      is_reference(resolved_connection) ->
        case execute(resolved_connection, "SELECT 1", [], []) do
          {:ok, _} -> :ok
          {:error, reason} -> {:error, {:connection_unhealthy, reason}}
        end

      true ->
        {:error, {:invalid_connection, connection}}
    end
  end

  @impl true
  def connection_info(connection) do
    resolved_connection = resolve_connection(connection)

    cond do
      is_reference(resolved_connection) ->
        case validate_connection(resolved_connection) do
          :ok ->
            %{type: :duckdb, connection: :duckdbex, status: :connected}

          {:error, reason} ->
            %{type: :duckdb, connection: :duckdbex, status: :disconnected, reason: reason}
        end

      true ->
        %{type: :duckdb, status: :invalid, value: connection}
    end
  end

  @impl true
  def transaction(connection, fun, _opts \\ []) when is_function(fun, 1) do
    resolved_connection = resolve_connection(connection)
    depth = transaction_depth_for(resolved_connection)

    with :ok <- validate_connection(resolved_connection),
         :ok <- begin_transaction(resolved_connection, depth) do
      put_transaction_depth(resolved_connection, depth + 1)
      execute_transaction_fun(resolved_connection, fun, depth + 1)
    end
  end

  defp begin_transaction(connection, 0) do
    case Duckdbex.begin_transaction(connection) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp begin_transaction(connection, depth) do
    case execute(connection, "SAVEPOINT #{savepoint_name(depth + 1)}", [], []) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp execute_transaction_fun(connection, fun, depth) do
    case fun.(connection) do
      {:error, reason} ->
        rollback(connection, depth, reason)

      {:ok, result} ->
        case finalize_commit(connection, depth) do
          :ok -> {:ok, result}
          {:error, reason} -> rollback(connection, depth, reason)
        end

      result ->
        case finalize_commit(connection, depth) do
          :ok -> {:ok, result}
          {:error, reason} -> rollback(connection, depth, reason)
        end
    end
  rescue
    error ->
      rollback(connection, depth, error)
  catch
    kind, reason ->
      rollback(connection, depth, {kind, reason})
  after
    put_transaction_depth(connection, max(depth - 1, 0))
  end

  defp finalize_commit(connection, 1) do
    case Duckdbex.commit(connection) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp finalize_commit(connection, depth) when depth > 1 do
    case execute(connection, "RELEASE SAVEPOINT #{savepoint_name(depth)}", [], []) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp rollback(connection, 1, reason) do
    _ = Duckdbex.rollback(connection)
    {:error, reason}
  end

  defp rollback(connection, depth, reason) when depth > 1 do
    savepoint = savepoint_name(depth)
    _ = execute(connection, "ROLLBACK TO SAVEPOINT #{savepoint}", [], [])
    _ = execute(connection, "RELEASE SAVEPOINT #{savepoint}", [], [])
    {:error, reason}
  end

  defp savepoint_name(depth), do: "selecto_sp_#{depth}"

  defp transaction_depth_for(connection) do
    transaction_depths = Process.get(@transaction_depth_key, %{})
    Map.get(transaction_depths, connection, 0)
  end

  defp put_transaction_depth(connection, depth) do
    transaction_depths = Process.get(@transaction_depth_key, %{})

    updated_depths =
      if depth <= 0 do
        Map.delete(transaction_depths, connection)
      else
        Map.put(transaction_depths, connection, depth)
      end

    Process.put(@transaction_depth_key, updated_depths)
    :ok
  end

  defp open_database(":memory:", nil), do: Duckdbex.open()
  defp open_database(database, nil), do: Duckdbex.open(database)

  defp open_database(database, %module{} = config) do
    if module == Duckdbex.Config do
      if database == ":memory:" do
        Duckdbex.open(config)
      else
        Duckdbex.open(database, config)
      end
    else
      {:error, {:invalid_duckdb_config, config}}
    end
  end

  defp open_database(_database, config), do: {:error, {:invalid_duckdb_config, config}}

  defp execute_query(connection, query, params) do
    with {:ok, result_ref} <- Duckdbex.query(connection, query, params),
         {:ok, result} <- fetch_result(result_ref) do
      {:ok, result}
    end
  end

  defp fetch_result(result_ref) do
    columns_result = Duckdbex.columns(result_ref)
    rows_result = Duckdbex.fetch_all(result_ref)
    :ok = Duckdbex.release(result_ref)

    with columns when is_list(columns) <- columns_result,
         rows when is_list(rows) <- rows_result do
      columns = Enum.map(columns, &to_string/1)
      {:ok, %{rows: rows, columns: columns, num_rows: mutation_count(columns, rows)}}
    else
      {:error, reason} -> {:error, reason}
      other -> {:error, {:unexpected_duckdb_result, other}}
    end
  end

  defp mutation_count(["Count"], [[count]]) when is_integer(count), do: count

  defp mutation_count(["Count"], [[{upper, lower}]])
       when is_integer(upper) and is_integer(lower),
       do: Duckdbex.hugeint_to_integer({upper, lower})

  defp mutation_count(["Count"], []), do: 0
  defp mutation_count(_columns, rows), do: length(rows)

  defp resolve_connection(%{adapter: _adapter, connection: nested_connection}) do
    resolve_connection(nested_connection)
  end

  defp resolve_connection(%{conn: conn}) when is_reference(conn), do: conn
  defp resolve_connection(connection), do: connection

  defp normalize_query(query) when is_binary(query), do: query
  defp normalize_query(query), do: IO.iodata_to_binary(query)

  defp normalize_params(params) when is_list(params), do: Enum.map(params, &normalize_param/1)
  defp normalize_params(params), do: params

  defp normalize_param(%Date{} = value), do: Date.to_iso8601(value)
  defp normalize_param(%NaiveDateTime{} = value), do: NaiveDateTime.to_iso8601(value)
  defp normalize_param(%DateTime{} = value), do: DateTime.to_iso8601(value)

  defp normalize_param(value) when is_map(value) do
    if Map.get(value, :__struct__) == Decimal and Code.ensure_loaded?(Decimal) do
      apply(Decimal, :to_string, [value, :normal])
    else
      value
    end
  end

  defp normalize_param(value), do: value

  defp dependency_available? do
    Code.ensure_loaded?(Duckdbex) and function_exported?(Duckdbex, :open, 0) and
      function_exported?(Duckdbex, :open, 1) and function_exported?(Duckdbex, :connection, 1) and
      function_exported?(Duckdbex, :query, 3) and function_exported?(Duckdbex, :columns, 1) and
      function_exported?(Duckdbex, :fetch_all, 1) and function_exported?(Duckdbex, :release, 1) and
      function_exported?(Duckdbex, :begin_transaction, 1) and
      function_exported?(Duckdbex, :commit, 1) and
      function_exported?(Duckdbex, :rollback, 1)
  end
end
