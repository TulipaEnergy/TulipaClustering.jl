@testset "Output saving with single ClusteringResult (no groups)" begin
    @testset "Make sure clustering result is saved for database_schema = '$database_schema'" for database_schema in
                                                                                                 [
        "",
        "cluster",
    ]
        dir = joinpath(OUTPUT_FOLDER, "temp")

        profile_names = ["solar", "wind"]
        timeframe_duration = 20

        profiles = DataFrame(;
            profile_name = repeat(profile_names; inner = timeframe_duration),
            timestep = repeat(1:timeframe_duration; outer = length(profile_names)),
            value = rand(length(profile_names) * timeframe_duration),
        )

        num_rep_periods = 3
        period_duration = 6

        split_into_periods!(profiles; period_duration = period_duration)
        clustering_data = find_representative_periods(profiles, num_rep_periods)

        connection = DBInterface.connect(DuckDB.DB)
        TulipaClustering.write_clustering_result_to_tables(
            connection,
            clustering_data;
            database_schema,
        )
        prefix = ""
        where_string = ""
        if database_schema == ""
            where_string = "WHERE schema_name = 'main'"
        else
            prefix = "$database_schema."
            where_string = "WHERE schema_name = '$database_schema'"
        end

        tables = [
            row.table_name for row in DBInterface.execute(
                connection,
                "SELECT table_name
                FROM duckdb_tables()
                $where_string
                ORDER BY table_name",
            )
        ]
        @test tables == Union{Missing, String}[
            "profiles_rep_periods",
            "rep_periods_data",
            "rep_periods_mapping",
            "timeframe_data",
        ]

        @testset "rep_periods_data" begin
            rep_periods_data_df =
                DBInterface.execute(
                    connection,
                    "SELECT * FROM $(prefix)rep_periods_data",
                ) |> DataFrame
            @test rep_periods_data_df.rep_period == 1:num_rep_periods
            @test rep_periods_data_df.num_timesteps == [
                fill(period_duration, num_rep_periods - 1)
                timeframe_duration % period_duration
            ]
            @test all(rep_periods_data_df.resolution .== 1)
        end

        @testset "ClusteringResult constructor without auxiliary data" begin
            clustering_result = TulipaClustering.ClusteringResult(
                clustering_data.profiles,
                clustering_data.weight_matrix,
            )

            @test clustering_result.profiles == clustering_data.profiles
            @test clustering_result.weight_matrix == clustering_data.weight_matrix
        end
    end

    @testset "Test with custom layout" begin
        # Create test data with custom column names
        profile_names = ["wind", "solar"]
        timeframe_duration = 24

        profiles = DataFrame(;
            profile_name = repeat(profile_names; inner = timeframe_duration),
            ts = repeat(1:timeframe_duration; outer = length(profile_names)),  # custom timestep column
            val = rand(length(profile_names) * timeframe_duration),            # custom value column
            years = fill(2030, length(profile_names) * timeframe_duration),
            scn = fill(1, length(profile_names) * timeframe_duration),
        )

        num_rep_periods = 2
        period_duration = 12
        layout = ProfilesTableLayout(;
            timestep = :ts,
            value = :val,
            year = :years,
            scenario = :scn,
        )

        split_into_periods!(profiles; period_duration = period_duration, layout = layout)
        clustering_data =
            find_representative_periods(profiles, num_rep_periods; layout = layout)

        connection = DBInterface.connect(DuckDB.DB)

        # Test the single ClusteringResult version with custom layout
        TulipaClustering.write_clustering_result_to_tables(
            connection,
            clustering_data;
            database_schema = "",
            layout = layout,
        )

        # Verify tables were created correctly
        tables = [
            row.table_name for row in DBInterface.execute(
                connection,
                "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main' ORDER BY table_name",
            )
        ]
        @test tables == Union{Missing, String}[
            "profiles_rep_periods",
            "rep_periods_data",
            "rep_periods_mapping",
            "timeframe_data",
        ]

        # Test profiles_rep_periods table structure with custom layout
        profiles_rep_df =
            DBInterface.execute(connection, "SELECT * FROM profiles_rep_periods") |>
            DataFrame
        @test sort(names(profiles_rep_df)) ==
              ["profile_name", "rep_period", "scn", "ts", "val", "years"]
        @test nrow(profiles_rep_df) ==
              length(profile_names) * period_duration * num_rep_periods

        # Test rep_periods_mapping table
        mapping_df =
            DBInterface.execute(connection, "SELECT * FROM rep_periods_mapping") |>
            DataFrame
        @test sort(names(mapping_df)) == ["period", "rep_period", "weight"]
        @test all(mapping_df.weight .>= 0)  # weights should be non-negative

        # Test rep_periods_data table
        rep_data_df =
            DBInterface.execute(connection, "SELECT * FROM rep_periods_data") |> DataFrame
        @test sort(names(rep_data_df)) == ["num_timesteps", "rep_period", "resolution"]
        @test rep_data_df.rep_period == 1:num_rep_periods
        @test all(rep_data_df.resolution .== 1.0)

        # Test timeframe_data table
        timeframe_df =
            DBInterface.execute(connection, "SELECT * FROM timeframe_data") |> DataFrame
        @test sort(names(timeframe_df)) == ["num_timesteps", "period"]
        @test nrow(timeframe_df) == size(clustering_data.weight_matrix, 1)
    end
end
