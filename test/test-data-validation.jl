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
                year = [2020, 2020],
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

    @testset "Test fail_fast option" begin
        connection = _new_connection(; database_schema)
        DuckDB.query(
            connection,
            "ALTER TABLE $(prefix)profiles RENAME profile_name TO badname",
        )
        try
            TC.validate_data!(
                connection;
                input_database_schema = database_schema,
                fail_fast = true,
            )
            @test false  # Should not reach here
        catch e
            @test isa(e, TC.DataValidationException)
            @test length(e.error_messages) == 1
        end
    end
end

@testset "Default layout passes validation" begin
    layout = TC.ProfilesTableLayout()
    @test isempty(TC._validate_layout!(layout))
end

@testset "Custom layout passes validation" begin
    layout = TC.ProfilesTableLayout(;
        profile_name = :prof_name,
        timestep = :time_step,
        value = :val,
        period = :per,
        year = :yr,
        scenario = :scen,
        cols_to_groupby = [:yr, :scen],
        cols_to_crossby = [],
    )
    @test isempty(TC._validate_layout!(layout))
end

@testset "Invalid layout fails validation" begin
    connection = _new_connection()

    @testset "Invalid cols_to_groupby" begin
        layout = TC.ProfilesTableLayout(; cols_to_groupby = [:year, :invalid_col])
        @test_throws TC.DataValidationException TC.validate_data!(connection; layout)
        @test TC._validate_layout!(layout) ==
              ["Column 'invalid_col' in 'cols_to_groupby' is not defined in the layout"]
    end

    @testset "Invalid cols_to_crossby" begin
        layout = TC.ProfilesTableLayout(; cols_to_crossby = [:invalid_col])
        @test_throws TC.DataValidationException TC.validate_data!(connection; layout)
        @test TC._validate_layout!(layout) ==
              ["Column 'invalid_col' in 'cols_to_crossby' is not defined in the layout"]
    end

    @testset "Overlapping cols_to_groupby and cols_to_crossby" begin
        layout = TC.ProfilesTableLayout(;
            cols_to_groupby = [:year],
            cols_to_crossby = [:year, :scenario],
        )
        @test_throws TC.DataValidationException TC.validate_data!(connection; layout)
        @test TC._validate_layout!(layout) == [
            "Columns [:year] are present in both 'cols_to_groupby' and 'cols_to_crossby'. These should be disjoint.",
        ]

        layout = TC.ProfilesTableLayout(;
            cols_to_groupby = [:year, :scenario],
            cols_to_crossby = [:year, :scenario],
        )
        @test_throws TC.DataValidationException TC.validate_data!(connection; layout)
        @test TC._validate_layout!(layout) == [
            "Columns [:year, :scenario] are present in both 'cols_to_groupby' and 'cols_to_crossby'. These should be disjoint.",
        ]
    end

    @testset "Test fail_fast option" begin
        layout = TC.ProfilesTableLayout(;
            cols_to_groupby = [:year, :invalid_col1],
            cols_to_crossby = [:invalid_col2],
        )
        try
            TC.validate_data!(connection; layout, fail_fast = true)
            @test false  # Should not reach here
        catch e
            @test isa(e, TC.DataValidationException)
            @test length(e.error_messages) == 2
        end
    end
end
