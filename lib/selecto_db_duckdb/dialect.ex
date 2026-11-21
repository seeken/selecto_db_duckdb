defmodule SelectoDBDuckDB.Dialect do
  @moduledoc false

  @behaviour Selecto.DB.Dialect

  alias Selecto.Dialect.DateTime.Operation, as: DateTimeOperation
  alias Selecto.Dialect.Predicate.Comparison

  @impl true
  def render_datetime_operation(%DateTimeOperation{operation: :format} = operation, _selecto) do
    if temporal_conversion_requested?(operation.options) do
      unsupported_datetime(operation)
    else
      {:ok,
       SelectoDBDuckDB.Adapter.format_datetime(
         operation.expression,
         Map.fetch!(operation.options, :format)
       )}
    end
  end

  def render_datetime_operation(%DateTimeOperation{} = operation, _selecto),
    do: unsupported_datetime(operation)

  @impl true
  def render_comparison(%Comparison{} = comparison, _selecto) do
    operator = if comparison.operation == :case_insensitive_not_like, do: "NOT LIKE", else: "LIKE"
    {:ok, ["LOWER(", comparison.left, ") ", operator, " LOWER(", comparison.right, ")"]}
  end

  @computed_casts %{
    "string" => "VARCHAR",
    "integer" => "BIGINT",
    "decimal" => "DECIMAL(38, 10)",
    "boolean" => "BOOLEAN",
    "date" => "DATE",
    "utc_datetime" => "TIMESTAMP"
  }

  # Governed computed values: the canonical casts and days_between. Other
  # computed-value fragments (json_text) stay unsupported.
  @impl true
  def render_computed_value(
        %Selecto.Dialect.ComputedValue{
          operation: :cast,
          expression: expression,
          type: type,
          path: []
        },
        _selecto
      ) do
    with {:ok, target} <- Map.fetch(@computed_casts, type),
         true <- computed_sql?(expression) do
      {:ok, ["CAST(", expression, " AS ", target, ")"]}
    else
      _ -> invalid_computed_value(:cast)
    end
  end

  # Whole calendar days from start to end: the difference of their dates.
  def render_computed_value(
        %Selecto.Dialect.ComputedValue{
          operation: :days_between,
          expression: [start, finish],
          type: "integer",
          path: []
        },
        _selecto
      ) do
    if computed_sql?(start) and computed_sql?(finish),
      do: {:ok, ["date_diff('day', CAST(", start, " AS DATE), CAST(", finish, " AS DATE))"]},
      else: invalid_computed_value(:days_between)
  end

  def render_computed_value(fragment, _selecto),
    do: invalid_computed_value(Map.get(fragment, :operation, :unknown))

  # Compiled SQL iodata with bound parameter markers, not empty.
  defp computed_sql?(value), do: sql?(value) and present?(value)

  defp sql?(value) when is_binary(value), do: true
  defp sql?(value) when is_integer(value), do: value in 0..255
  defp sql?({:param, _value}), do: true
  defp sql?(value) when is_list(value), do: Enum.all?(value, &sql?/1)
  defp sql?(_value), do: false

  defp present?(value) when is_binary(value), do: byte_size(value) > 0
  defp present?(value) when is_list(value), do: Enum.any?(value, &present?/1)
  defp present?(_value), do: true

  defp invalid_computed_value(operation) do
    {:error,
     Selecto.Error.validation_error("Invalid DuckDB computed-value fragment", %{
       operation: operation,
       unsupported_feature: :computed_value
     })}
  end

  defp temporal_conversion_requested?(options) do
    Map.get(options, :epoch_storage) not in [nil, false] or
      Map.get(options, :timezone) not in [nil, ""]
  end

  defp unsupported_datetime(operation) do
    {:error,
     Selecto.Error.validation_error("DuckDB does not support this datetime operation", %{
       operation: operation.operation,
       unsupported_feature: :datetime_operation
     })}
  end
end
