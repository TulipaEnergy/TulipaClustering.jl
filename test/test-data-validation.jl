const TC = TulipaClustering

@testset "Test DataValidationException print" begin
  # Mostly to appease codecov
  error_msg = "DataValidationException: The following issues were found in the data:\n- example"
  @test_throws error_msg throw(TC.DataValidationException(["example"]))
end

@testset "Test having all tables and columns with database_schema='$database_schema'" for database_schema in
                                                                                          [
  "",
  "input",
]
  @testset "_new_connection passes validation" begin
    connection = _new_connection(; database_schema)
    @test TC.validate_data!(connection; input_database_schema = database_schema) === nothing
  end

  prefix = database_schema == "" ? "" : "$database_schema."
  table_names = Dict("profiles" => "profiles")

  @testset "Missing table" begin
    connection = _new_connection(; database_schema)
    DuckDB.query(connection, "ALTER TABLE $(prefix)profiles RENAME TO bad_name")
    @test_throws TC.DataValidationException TC.validate_data!(
      connection,
      input_database_schema = database_schema,
    )
    @test TC._validate_required_tables_and_columns!(
      connection,
      database_schema,
      table_names,
    ) == ["Table 'profiles' expected but not found in schema '$database_schema'"]
  end

  @testset "Missing column" begin
    for column in ("profile_name", "year", "timestep", "value")
      connection = _new_connection(; database_schema)
      DuckDB.query(connection, "ALTER TABLE $(prefix)profiles RENAME $column TO badname")
      @test_throws TC.DataValidationException TC.validate_data!(
        connection,
        input_database_schema = database_schema,
      )
      @test TC._validate_required_tables_and_columns!(
        connection,
        database_schema,
        table_names,
      ) == [
        "Column '$column' is missing from table 'profiles' in schema '$database_schema'",
      ]
    end
  end
end
