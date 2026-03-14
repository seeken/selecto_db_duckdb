defmodule SelectoDBDuckDB.Adapter do
  @moduledoc """
  DuckDB adapter for Selecto backed by `Duckdbex`.
  """

  @behaviour Selecto.DB.Adapter

  @missing_dependency {:adapter_dependency_missing, :duckdbex}
  @transaction_depth_key {__MODULE__, :transaction_depth}

  @impl true
  def name, do: :duckdb

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
  def execute(connection, query, params, _opts) do
    resolved_connection = resolve_connection(connection)

    cond do
      not dependency_available?() ->
        {:error, @missing_dependency}

      not is_reference(resolved_connection) ->
        {:error, {:invalid_connection, connection}}

      true ->
        execute_query(resolved_connection, normalize_query(query), params || [])
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
  def supports?(feature) do
    feature in [:cte, :window_functions, :transactions, :rollup]
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

    begin_result =
      if depth == 0 do
        case Duckdbex.begin_transaction(resolved_connection) do
          :ok -> :ok
          {:error, reason} -> {:error, reason}
        end
      else
        case execute(resolved_connection, "SAVEPOINT #{savepoint_name(depth + 1)}", [], []) do
          {:ok, _} -> :ok
          {:error, reason} -> {:error, reason}
        end
      end

    with :ok <- validate_connection(resolved_connection),
         :ok <- begin_result do
      put_transaction_depth(resolved_connection, depth + 1)
      execute_transaction_fun(resolved_connection, fun, depth + 1)
    end
  end

  defp execute_transaction_fun(connection, fun, depth) do
    case fun.(connection) do
      {:error, reason} ->
        rollback(connection, depth, reason)

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
      {:ok, %{rows: rows || [], columns: Enum.map(columns || [], &to_string/1)}}
    else
      {:error, reason} -> {:error, reason}
      other -> {:error, {:unexpected_duckdb_result, other}}
    end
  end

  defp resolve_connection(%{adapter: _adapter, connection: nested_connection}) do
    resolve_connection(nested_connection)
  end

  defp resolve_connection(%{conn: conn}) when is_reference(conn), do: conn
  defp resolve_connection(connection), do: connection

  defp normalize_query(query) when is_binary(query), do: query
  defp normalize_query(query), do: IO.iodata_to_binary(query)

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
