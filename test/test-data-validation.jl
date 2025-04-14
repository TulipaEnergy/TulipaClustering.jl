const TC = TulipaClustering

@testset "Test DataValidationException print" begin
  # Mostly to appease codecov
  error_msg = "DataValidationException: The following issues were found in the data:\n- example"
  @test_throws error_msg throw(TC.DataValidationException(["example"]))
end

@testset "Test having all tables and columns" begin
  @testset "_new_connection passes validation" begin
    connection = _new_connection()
    @test TC.validate_data!(connection) === nothing
  end

  db_schema = "input"
  table_names = Dict("profiles" => "profiles")

  @testset "Missing table" begin
    connection = _new_connection()
    DuckDB.query(connection, "ALTER TABLE input.profiles RENAME TO bad_name")
    @test_throws TC.DataValidationException TC.validate_data!(connection)
    @test TC._validate_required_tables_and_columns!(connection, db_schema, table_names) ==
          ["Table 'profiles' expected but not found in schema 'input'"]
  end

  @testset "Missing column" begin
    for column in ("profile_name", "year", "timestep", "value")
      connection = _new_connection()
      DuckDB.query(connection, "ALTER TABLE input.profiles RENAME $column TO badname")
      @test_throws TC.DataValidationException TC.validate_data!(connection)
      @test TC._validate_required_tables_and_columns!(connection, db_schema, table_names) ==
            ["Column '$column' is missing from table 'profiles' in schema 'input'"]
    end
  end
end
