defmodule SelectoDBDuckDB.PostImage do
  @moduledoc false

  # TW-09 post-image check for guarded INSERT, UPDATE and UPSERT commands
  # (`metadata.post_image`, set by SelectoUpdato: the engine tenant and the
  # trusted and host scope, plus the full guard for an insert).
  #
  # DuckDB has no triggers or rules; an update that assigns a scoped column
  # or a generated scope column is what can move a row out of the scope. The
  # statement returns the written rows' keys (RETURNING; tagged columns when
  # it returns none itself) and they are re-read by key under the scope
  # inside the same transaction. The relation is resolved as DuckDB resolves
  # the write's name: a temp table shadows first, then the current database
  # and schema (attached databases are never searched implicitly); a view is
  # refused. The key must be exactly a PRIMARY KEY or a UNIQUE constraint
  # over NOT NULL columns of that table. Unless every written row still
  # matches, the transaction rolls back.

  alias Selecto.Write.{Command, Error}
  alias SelectoDBDuckDB.WriteCompiler

  @key_alias "selecto_post_image_key_"

  def prepare(%Command{metadata: %{post_image: %{key: [_ | _]}}, operation: operation} = command)
      when operation in [:insert, :update, :upsert],
      do: {:post_image, command}

  def prepare(command), do: {:plain, command}

  def returning(text, %Command{metadata: %{post_image: %{key: key}}, returning: returning}) do
    if returns_keys?(returning, key) do
      text
    else
      keys =
        key
        |> Enum.with_index(1)
        |> Enum.map_join(", ", fn {column, index} ->
          "#{quote_id(column)} AS #{@key_alias}#{index}"
        end)

      if returning == :none, do: text <> " RETURNING " <> keys, else: text <> ", " <> keys
    end
  end

  defp returns_keys?(:all, _key), do: true

  defp returns_keys?(fields, key) when is_list(fields),
    do: Enum.all?(key, &(&1 in Enum.map(fields, fn field -> to_string(field) end)))

  defp returns_keys?(_returning, _key), do: false

  def split_result(%{columns: columns, rows: rows} = result, %Command{
        metadata: %{post_image: %{key: key}}
      })
      when is_list(columns) and is_list(rows) do
    tagged =
      columns
      |> Enum.with_index()
      |> Enum.filter(fn {column, _index} ->
        String.starts_with?(to_string(column), @key_alias)
      end)
      |> Enum.map(&elem(&1, 1))

    if tagged == [] do
      indexes =
        Enum.map(key, fn column -> Enum.find_index(columns, &(to_string(&1) == column)) end)

      if Enum.any?(indexes, &is_nil/1),
        do: {result, :unknown},
        else: {result, Enum.map(rows, fn row -> Enum.map(indexes, &Enum.at(row, &1)) end)}
    else
      keys = Enum.map(rows, fn row -> Enum.map(tagged, &Enum.at(row, &1)) end)

      keep = fn list ->
        list
        |> Enum.with_index()
        |> Enum.reject(&(elem(&1, 1) in tagged))
        |> Enum.map(&elem(&1, 0))
      end

      result =
        case keep.(columns) do
          [] -> %{result | columns: [], rows: []}
          kept -> %{result | columns: kept, rows: Enum.map(rows, keep)}
        end

      {Map.put(result, :num_rows, length(rows)), keys}
    end
  end

  def split_result(result, _command), do: {result, :unknown}

  def verify(_command, :unknown, _execute, _opts),
    do: refused(:query_rule_not_evaluable, "cannot identify the written rows")

  def verify(%Command{} = command, keys, execute, opts) do
    %{predicate: predicate, key: key} = command.metadata.post_image

    with {:ok, schema, table} <- resolve(command.relation, execute),
         :ok <- keyed(schema, table, key, execute),
         {match, params} = key_match(key, keys),
         {:ok, scope} <-
           WriteCompiler.compile_predicate(
             predicate,
             [context: Keyword.get(opts, :context, %{})],
             length(params)
           ) do
      {database, schema_name} = schema
      relation = "#{quote_id(database)}.#{quote_id(schema_name)}.#{quote_id(table)}"

      # The key placeholders ($1..$k) are reused by both counts.
      sql =
        "SELECT (SELECT count(*) FROM #{relation} WHERE #{match} AND (#{scope.text})), " <>
          "(SELECT count(*) FROM #{relation} WHERE #{match})"

      case execute.(sql, params ++ scope.params) do
        {:ok, %{rows: [[matched, rows]]}} ->
          if matched == length(keys) and rows == length(keys),
            do: :ok,
            else: refused(:query_rule_violation, "the written rows left the trusted write scope")

        _ ->
          refused(:query_rule_not_evaluable, "the relation cannot prove the written rows' scope")
      end
    else
      {:error, %Error{}} = error -> error
      _ -> refused(:query_rule_not_evaluable, "the relation cannot prove the written rows' scope")
    end
  end

  defp resolve(relation, execute) do
    case String.split(to_string(relation), ".") do
      [name] ->
        with {:ok, %{rows: [[database, schema]]}} <-
               execute.("SELECT current_database(), current_schema()", []) do
          first_table([{"temp", "main", name}, {database, schema, name}], execute)
        else
          _ -> :error
        end

      [schema, name] ->
        with {:ok, %{rows: [[database]]}} <- execute.("SELECT current_database()", []) do
          first_table([{database, schema, name}], execute)
        else
          _ -> :error
        end

      _ ->
        :error
    end
  end

  defp first_table(candidates, execute) do
    Enum.find_value(candidates, :error, fn {database, schema, name} ->
      case execute.(
             "SELECT 'table', table_name FROM duckdb_tables() WHERE database_name = $1 AND schema_name = $2 AND lower(table_name) = lower($3) " <>
               "UNION ALL SELECT 'view', view_name FROM duckdb_views() WHERE database_name = $1 AND schema_name = $2 AND lower(view_name) = lower($3)",
             [database, schema, name]
           ) do
        {:ok, %{rows: [["table", actual]]}} -> {:ok, {database, schema}, actual}
        {:ok, %{rows: [_ | _]}} -> :error
        _ -> nil
      end
    end)
  end

  defp keyed({database, schema}, table, key, execute) do
    with {:ok, %{rows: constraints}} <-
           execute.(
             "SELECT constraint_type, constraint_column_names FROM duckdb_constraints() WHERE database_name = $1 AND schema_name = $2 AND table_name = $3 AND constraint_type IN ('PRIMARY KEY', 'UNIQUE')",
             [database, schema, table]
           ),
         {:ok, %{rows: columns}} <-
           execute.(
             "SELECT column_name, is_nullable FROM duckdb_columns() WHERE database_name = $1 AND schema_name = $2 AND table_name = $3",
             [database, schema, table]
           ) do
      key = key |> Enum.map(&String.downcase/1) |> Enum.sort()
      not_null = for [name, nullable] <- columns, nullable == false, do: String.downcase(name)

      matches? =
        Enum.any?(constraints, fn [type, names] ->
          names = names |> List.wrap() |> Enum.map(&String.downcase(to_string(&1))) |> Enum.sort()
          names == key and (type == "PRIMARY KEY" or Enum.all?(key, &(&1 in not_null)))
        end)

      if matches?, do: :ok, else: :error
    else
      _ -> :error
    end
  end

  defp key_match([column], keys) do
    case keys do
      [] ->
        {"false", []}

      keys ->
        markers = keys |> Enum.with_index(1) |> Enum.map_join(", ", fn {_key, i} -> "$#{i}" end)
        {"#{quote_id(column)} IN (#{markers})", Enum.map(keys, &hd/1)}
    end
  end

  defp key_match(columns, keys) do
    case keys do
      [] ->
        {"false", []}

      keys ->
        width = length(columns)

        rows =
          keys
          |> Enum.with_index()
          |> Enum.map_join(" OR ", fn {_row, r} ->
            "(" <>
              (columns
               |> Enum.with_index()
               |> Enum.map_join(" AND ", fn {column, c} ->
                 "#{quote_id(column)} = $#{r * width + c + 1}"
               end)) <>
              ")"
          end)

        {"(" <> rows <> ")", List.flatten(keys)}
    end
  end

  defp refused(code, message),
    do:
      {:error,
       Error.new(:query_enforcement, "SQLite " <> message,
         details: %{code: code, adapter: :sqlite}
       )}

  defp quote_id(identifier),
    do: "\"" <> String.replace(to_string(identifier), "\"", "\"\"") <> "\""
end
