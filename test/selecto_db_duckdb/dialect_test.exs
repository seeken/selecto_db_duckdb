defmodule SelectoDBDuckDB.DialectTest do
  use ExUnit.Case, async: true

  alias Selecto.Dialect.DateTime.Operation, as: DateTimeOperation
  alias Selecto.Dialect.Predicate.Comparison
  alias SelectoDBDuckDB.Dialect

  test "renders date formatting and case-insensitive matching in DuckDB syntax" do
    datetime = %DateTimeOperation{
      operation: :format,
      clause: :select,
      expression: ~s("created_at"),
      options: %{format: "YYYY-MM", epoch_storage: nil}
    }

    comparison = %Comparison{
      operation: :case_insensitive_not_like,
      left: ~s("title"),
      right: {:param, "%office%"}
    }

    assert {:ok, formatted} = Dialect.render_datetime_operation(datetime, %{})
    assert IO.iodata_to_binary(formatted) =~ "strftime"
    assert {:ok, compared} = Dialect.render_comparison(comparison, %{})

    assert compared == [
             "LOWER(",
             ~s("title"),
             ") ",
             "NOT LIKE",
             " LOWER(",
             {:param, "%office%"},
             ")"
           ]
  end

  test "rejects unimplemented epoch conversion explicitly" do
    datetime = %DateTimeOperation{
      operation: :format,
      clause: :select,
      expression: ~s("created_at"),
      options: %{format: "YYYY", epoch_storage: :unix_seconds}
    }

    assert {:error, %Selecto.Error{details: %{unsupported_feature: :datetime_operation}}} =
             Dialect.render_datetime_operation(datetime, %{})
  end
end
