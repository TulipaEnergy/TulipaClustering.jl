"""
    DataValidationException

Exception related to data validation of the Tulipa Energy Model input data.
"""
mutable struct DataValidationException <: Exception
  error_messages::Vector{String}
end

function Base.showerror(io::IO, ex::DataValidationException)
  println(io, "DataValidationException: The following issues were found in the data:")
  for error_message in ex.error_messages
    println(io, "- " * error_message)
  end
end

"""
    validate_data!(connection)

Validate that the required data in `connection` exists and is correct.
Throws a `DataValidationException` if any error is found.
"""
function validate_data!(
  connection;
  input_database_schema::String = "input",
  table_names::Dict = Dict("profiles" => "profiles"),
)
  error_messages = String[]

  for (log_msg, validation_function, fail_fast) in
      (("has required tables and columns", _validate_required_tables_and_columns!, true),)
    @debug log_msg
    append!(
      error_messages,
      validation_function(connection, input_database_schema, table_names),
    )
    if fail_fast && length(error_messages) > 0
      break
    end
  end

  if length(error_messages) > 0
    throw(DataValidationException(error_messages))
  end

  return
end

function _validate_required_tables_and_columns!(
  connection,
  input_database_schema,
  table_names,
)
  error_messages = String[]
  table_name = table_names["profiles"]

  columns_from_connection = [
    row.column_name for row in DuckDB.query(
      connection,
      "SELECT column_name FROM duckdb_columns() WHERE table_name = '$table_name' AND schema_name = '$input_database_schema'",
    )
  ]
  if length(columns_from_connection) == 0
    # Just to make sure that this is not a random case with no columns but the table exists
    has_table =
      only([
        row.count for row in DuckDB.query(
          connection,
          "SELECT COUNT(table_name) as count FROM duckdb_tables() WHERE table_name = '$table_name' AND schema_name = '$input_database_schema'",
        )
      ]) == 1
    if !has_table
      push!(
        error_messages,
        "Table '$table_name' expected but not found in schema '$input_database_schema'",
      )
      return error_messages
    end
  end

  for column in ["profile_name", "year", "timestep", "value"]
    if !(column in columns_from_connection)
      push!(
        error_messages,
        "Column '$column' is missing from table '$table_name' in schema '$input_database_schema'",
      )
    end
  end

  return error_messages
end
