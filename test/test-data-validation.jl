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
        @test TC.validate_data!(connection; input_database_schema = database_schema) ===
              nothing
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
            TC.ProfilesTableLayout(),
            DataFrame(),
        ) == ["Table 'profiles' expected but not found in schema '$database_schema'"]
    end

    @testset "Missing column" begin
        for column in ("profile_name", "timestep", "value")
            connection = _new_connection(; database_schema)
            DuckDB.query(
                connection,
                "ALTER TABLE $(prefix)profiles RENAME $column TO badname",
            )
            @test_throws TC.DataValidationException TC.validate_data!(
                connection,
                input_database_schema = database_schema,
            )
            @test TC._validate_required_tables_and_columns!(
                connection,
                database_schema,
                table_names,
                TC.ProfilesTableLayout(),
                DataFrame(),
            ) == [
                "Column '$column' is missing from table 'profiles' in schema '$database_schema'",
            ]
        end
    end

    @testset "Missing column in initial representatives" begin
        connection = _new_connection(; database_schema)
        schema_addendum = if database_schema != ""
            "AND schema_name = '$database_schema'"
        else
            ""
        end
        columns_from_connection = [
            row.column_name for row in DuckDB.query(
                connection,
                "SELECT column_name FROM duckdb_columns() WHERE table_name = 'profiles' $schema_addendum",
            )
        ]
        required_columns = columns_from_connection ∪ ["period"]
        for column in required_columns
            initial_representatives = DataFrame(;
                profile_name = ["a", "b"],
                timestep = [1, 2],
                value = [0.5, 0.6],
                period = [1, 1],
            )
            select!(initial_representatives, Not(column))
            @test_throws TC.DataValidationException TC.validate_data!(
                connection,
                input_database_schema = database_schema,
                initial_representatives = initial_representatives,
            )
            @test TC._validate_required_tables_and_columns!(
                connection,
                database_schema,
                table_names,
                TC.ProfilesTableLayout(),
                initial_representatives,
            ) == [
                "Column '$column' is missing from the initial representatives DataFrame. Hint! It must have the same columns as the 'profiles' table plus the 'period' column.",
            ]
        end
    end
end
