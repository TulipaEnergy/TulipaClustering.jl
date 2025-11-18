@testset "Transform wide in long" begin
    connection = DBInterface.connect(DuckDB.DB)
    DuckDB.query(
        connection,
        "CREATE TABLE t_wide AS
        SELECT
            2030 AS year,
               i AS timestep,
           2.0*i AS name1,
           i * i AS name2,
             0.0 AS name3,
        FROM
          generate_series(1, 24) AS s(i)
        ",
    )

    transform_wide_to_long!(connection, "t_wide", "t_long")

    df = DuckDB.query(
        connection,
        "FROM t_long
        ORDER BY profile_name, year, timestep
        ",
    ) |> DataFrame
    @test size(df) == (72, 4)
    @test sort(names(df)) == ["profile_name", "timestep", "value", "year"]
    @test df.value == [2.0 * (1:24); (1:24) .* (1:24); fill(0.0, 24)]

    @testset "It doesn't throw when called twice" begin
        transform_wide_to_long!(connection, "t_wide", "t_long")
    end

    @test_throws AssertionError transform_wide_to_long!(
        connection,
        "t_wide",
        "t_long";
        exclude_columns = String[],
    )
end

@testset "Transform wide in long with scenario column" begin
    connection = DBInterface.connect(DuckDB.DB)
    DuckDB.query(
        connection,
        "CREATE TABLE t_wide AS
        SELECT
               1 AS scenario,
            2030 AS year,
               i AS timestep,
           2.0*i AS name1,
           i * i AS name2,
             0.0 AS name3,
        FROM
          generate_series(1, 24) AS s(i)
        ",
    )

    transform_wide_to_long!(
        connection,
        "t_wide",
        "t_long";
        exclude_columns = ["scenario", "year", "timestep"],
    )

    df = DuckDB.query(
        connection,
        "FROM t_long
        ORDER BY profile_name, year, timestep
        ",
    ) |> DataFrame
    @test size(df) == (72, 5)
    @test sort(names(df)) == ["profile_name", "scenario", "timestep", "value", "year"]
    @test df.value == [2.0 * (1:24); (1:24) .* (1:24); fill(0.0, 24)]

    @testset "It doesn't throw when called twice" begin
        transform_wide_to_long!(
            connection,
            "t_wide",
            "t_long";
            exclude_columns = ["scenario", "year", "timestep"],
        )
    end
end

@testset "cluster! with database_schema '$database_schema'" for database_schema in
                                                                ("", "cluster")
    period_duration = 24
    num_periods = 7
    num_timesteps = period_duration * num_periods
    num_rps = 4
    profile_names = ["name1", "name2", "name3"]
    layout = ProfilesTableLayout(; cols_to_groupby = [])

    connection = _new_connection(; profile_names, num_timesteps)
    clustering_kwargs = Dict(:display => :iter)
    weight_fitting_kwargs =
        Dict(:niters => 100, :learning_rate => 0.001, :adaptive_grad => false)

    clusters = cluster!(
        connection,
        period_duration,
        num_rps;
        database_schema,
        clustering_kwargs,
        weight_fitting_kwargs,
        layout,
    )
    prefix = database_schema != "" ? "$(database_schema)." : ""

    @testset "rep_periods_data" begin
        df_rep_periods_data =
            DuckDB.query(
                connection,
                "FROM $(prefix)rep_periods_data
                ORDER BY rep_period",
            ) |> DataFrame

        @test sort(names(df_rep_periods_data)) ==
              ["num_timesteps", "rep_period", "resolution"]

        @test df_rep_periods_data.rep_period == repeat(1:num_rps)
        @test all(df_rep_periods_data.resolution .== 1.0)
        @test all(df_rep_periods_data.num_timesteps .== period_duration)
    end

    @testset "rep_periods_mapping" begin
        df_rep_periods_mapping =
            DuckDB.query(
                connection,
                "FROM $(prefix)rep_periods_mapping
                ORDER BY period, rep_period",
            ) |> DataFrame

        @test sort(names(df_rep_periods_mapping)) == ["period", "rep_period", "weight"]

        @test size(df_rep_periods_mapping, 1) ≥ num_periods
    end

    @testset "timeframe_data" begin
        df_timeframe_data =
            DuckDB.query(
                connection,
                "FROM $(prefix)timeframe_data
                ORDER BY period",
            ) |> DataFrame

        @test sort(names(df_timeframe_data)) == ["num_timesteps", "period"]

        @test df_timeframe_data.period == repeat(1:num_periods)
        @test all(df_timeframe_data.num_timesteps .== period_duration)
    end

    @testset "profiles_rep_periods" begin
        df_profiles_rep_periods =
            DuckDB.query(
                connection,
                "FROM $(prefix)profiles_rep_periods
                ORDER BY profile_name, rep_period, timestep",
            ) |> DataFrame

        @test sort(names(df_profiles_rep_periods)) ==
              ["profile_name", "rep_period", "timestep", "value", "year"]

        @test df_profiles_rep_periods.profile_name ==
              repeat(profile_names; inner = period_duration * num_rps)
        @test df_profiles_rep_periods.rep_period ==
              repeat(1:num_rps; inner = period_duration, outer = length(profile_names))
        @test df_profiles_rep_periods.timestep ==
              repeat(1:period_duration; outer = length(profile_names) * num_rps)
    end

    @testset "It doesn't throw when called twice" begin
        cluster!(connection, period_duration, num_rps; database_schema, layout)
    end
end

@testset "dummy_cluster! with database_schema '$database_schema'" for database_schema in
                                                                      ("", "cluster")
    period_duration = 24 * 7
    num_periods = 1
    num_timesteps = period_duration * num_periods
    num_rps = 1
    profile_names = ["name1", "name2", "name3"]
    layout = ProfilesTableLayout(; cols_to_groupby = [])

    connection = _new_connection(; profile_names, num_timesteps)

    clusters = dummy_cluster!(connection; database_schema, layout)
    prefix = database_schema != "" ? "$(database_schema)." : ""

    df_rep_periods_data =
        DuckDB.query(
            connection,
            "FROM $(prefix)rep_periods_data
            ORDER BY rep_period",
        ) |> DataFrame
    df_rep_periods_mapping =
        DuckDB.query(
            connection,
            "FROM $(prefix)rep_periods_mapping
            ORDER BY period, rep_period",
        ) |> DataFrame
    df_profiles_rep_periods =
        DuckDB.query(
            connection,
            "FROM $(prefix)profiles_rep_periods
            ORDER BY profile_name, rep_period, timestep",
        ) |> DataFrame

    @test sort(names(df_rep_periods_data)) == ["num_timesteps", "rep_period", "resolution"]
    @test sort(names(df_rep_periods_mapping)) == ["period", "rep_period", "weight"]
    @test sort(names(df_profiles_rep_periods)) ==
          ["profile_name", "rep_period", "timestep", "value", "year"]

    @test df_rep_periods_data.rep_period == repeat(1:num_rps)
    @test all(df_rep_periods_data.resolution .== 1.0)
    @test all(df_rep_periods_data.num_timesteps .== period_duration)

    @test size(df_rep_periods_mapping, 1) ≥ num_periods

    @test df_profiles_rep_periods.profile_name ==
          repeat(profile_names; inner = period_duration * num_rps)
    @test df_profiles_rep_periods.rep_period ==
          repeat(1:num_rps; inner = period_duration, outer = length(profile_names))
    @test df_profiles_rep_periods.timestep ==
          repeat(1:period_duration; outer = length(profile_names) * num_rps)

    @testset "It doesn't throw when called twice" begin
        dummy_cluster!(connection; database_schema, layout)
    end
end

@testset "cluster! with custom layout" begin
    period_duration = 24
    num_periods = 5
    num_timesteps = period_duration * num_periods
    num_rps = 3
    years = [2020, 2025]
    scenarios = [1, 2]
    profile_names = ["name1", "name2"]
    layout = ProfilesTableLayout(; timestep = :ts, value = :val, scenario = :scn) # default :year col and cols_to_groupby = [:year]

    # Create a connection with custom column names to match the custom layout
    connection = _new_connection_multi_scenario_year(;
        profile_names,
        num_timesteps,
        years,
        scenarios,
        layout,
    )

    clusters = cluster!(
        connection,
        period_duration,
        num_rps;
        layout,
        clustering_kwargs = Dict(:display => :iter),
        weight_fitting_kwargs = Dict(:niters => 20, :learning_rate => 0.001),
    )

    # Verify DB outputs keep custom layout column names
    df_profiles_rep_periods =
        DuckDB.query(
            connection,
            "FROM profiles_rep_periods
            ORDER BY profile_name, rep_period, ts",
        ) |> DataFrame

    @test sort(names(df_profiles_rep_periods)) ==
          ["profile_name", "rep_period", "scn", "ts", "val", "year"]

    @test size(df_profiles_rep_periods, 1) ==
          length(profile_names) *
          period_duration *
          num_rps *
          length(years) *
          length(scenarios)

    df_rep_periods_mapping =
        DuckDB.query(
            connection,
            "FROM rep_periods_mapping
            ORDER BY period, rep_period",
        ) |> DataFrame

    @test sort(names(df_rep_periods_mapping)) == ["period", "rep_period", "weight", "year"]
    @test size(df_rep_periods_mapping, 1) ≥ num_periods * length(years)
end

@testset "cluster! with groups for multi-scenario and multi-year data" begin
    period_duration = 24
    num_periods = 3  # Reduced to make tests faster
    num_timesteps = period_duration * num_periods
    num_rps = 2
    profile_names = ["profile_A", "profile_B"]
    years = [2020, 2021]
    scenarios = [1, 2]

    @testset "Test1: using default layout (cols_to_groupby = [:year])" begin
        connection = _new_connection_multi_scenario_year(;
            profile_names,
            num_timesteps,
            years,
            scenarios,
        )

        clusters = cluster!(
            connection,
            period_duration,
            num_rps;
            clustering_kwargs = Dict(:display => :none),
            weight_fitting_kwargs = Dict(:niters => 50),
        )

        # Verify that clustering was done by year (default behavior)
        # Should have separate clustering results for each year
        @test length(clusters) == length(years)

        # Check rep_periods_data table
        df_rep_periods_data =
            DuckDB.query(connection, "FROM rep_periods_data ORDER BY year, rep_period") |>
            DataFrame

        @test sort(names(df_rep_periods_data)) ==
              ["num_timesteps", "rep_period", "resolution", "year"]
        @test length(unique(df_rep_periods_data.year)) == length(years)
        @test all(df_rep_periods_data.num_timesteps .== period_duration)

        # Check profiles_rep_periods table
        df_profiles_rep_periods =
            DuckDB.query(
                connection,
                "FROM profiles_rep_periods ORDER BY year, scenario, profile_name, rep_period, timestep",
            ) |> DataFrame

        @test sort(names(df_profiles_rep_periods)) ==
              ["profile_name", "rep_period", "scenario", "timestep", "value", "year"]

        # Should have data for all combinations: years × scenarios × profiles × num_rps × period_duration
        expected_rows =
            length(years) *
            length(scenarios) *
            length(profile_names) *
            num_rps *
            period_duration
        @test nrow(df_profiles_rep_periods) == expected_rows

        @testset "It doesn't throw when called twice" begin
            cluster!(connection, period_duration, num_rps)
        end
    end

    @testset "Test2: cols_to_groupby = [:year, :scenario]" begin
        connection = _new_connection_multi_scenario_year(;
            profile_names,
            num_timesteps,
            years,
            scenarios,
        )

        layout = ProfilesTableLayout(; cols_to_groupby = [:year, :scenario])

        clusters = cluster!(
            connection,
            period_duration,
            num_rps;
            layout,
            clustering_kwargs = Dict(:display => :none),
            weight_fitting_kwargs = Dict(:niters => 50),
        )

        # Verify that clustering was done by both year and scenario
        # Should have separate clustering results for each year-scenario combination
        @test length(clusters) == length(years) * length(scenarios)

        # Check rep_periods_data table
        df_rep_periods_data =
            DuckDB.query(connection, "FROM rep_periods_data ORDER BY year, rep_period") |>
            DataFrame

        @test sort(names(df_rep_periods_data)) ==
              ["num_timesteps", "rep_period", "resolution", "scenario", "year"]
        # Should have rep periods for each year-scenario combination
        @test nrow(df_rep_periods_data) == num_rps * length(years) * length(scenarios)

        # Check profiles_rep_periods table structure
        df_profiles_rep_periods =
            DuckDB.query(
                connection,
                "FROM profiles_rep_periods ORDER BY year, scenario, profile_name, rep_period, timestep",
            ) |> DataFrame

        @test sort(names(df_profiles_rep_periods)) ==
              ["profile_name", "rep_period", "scenario", "timestep", "value", "year"]

        # Should have data for all combinations: years × scenarios × profiles × num_rps × period_duration
        expected_rows =
            length(years) *
            length(scenarios) *
            length(profile_names) *
            num_rps *
            period_duration
        @test nrow(df_profiles_rep_periods) == expected_rows

        # Check that all year-scenario combinations are present
        unique_combinations = unique(df_profiles_rep_periods[:, [:year, :scenario]])
        @test nrow(unique_combinations) == length(years) * length(scenarios)
        @test Set(unique_combinations.year) == Set(years)
        @test Set(unique_combinations.scenario) == Set(scenarios)

        @testset "It doesn't throw when called twice" begin
            cluster!(connection, period_duration, num_rps; layout)
        end
    end
end

@testset "cluster! with initial representatives" begin
    period_duration = 24
    num_periods = 3  # Reduced to make tests faster
    num_timesteps = period_duration * num_periods
    num_rps = 2
    profile_names = ["profile_A", "profile_B"]
    years = [2020, 2021]
    scenarios = [1, 2]

    @testset "Test1: using default layout (cols_to_groupby = [:year])" begin
        @testset "Test1.1: initial representatives for all years" begin
            connection = _new_connection_multi_scenario_year(;
                profile_names,
                num_timesteps,
                years,
                scenarios,
            )

            # Create initial representatives for all years and all scenarios
            # Key columns are: [:timestep, :profile_name, :year, :scenario]
            initial_representatives = DataFrame()
            for year in years
                for scenario in scenarios
                    for profile in profile_names
                        rep_data = DataFrame([
                            :period => ones(Int, period_duration),
                            :timestep => 1:period_duration,
                            :year => fill(year, period_duration),
                            :scenario => fill(scenario, period_duration),
                            :profile_name => fill(profile, period_duration),
                            :value => fill(
                                10.0 + year * 0.1 + scenario * 0.01,
                                period_duration,
                            ),
                        ])
                        initial_representatives = vcat(initial_representatives, rep_data)
                    end
                end
            end

            clusters = cluster!(
                connection,
                period_duration,
                num_rps;
                initial_representatives,
                clustering_kwargs = Dict(:display => :none),
                weight_fitting_kwargs = Dict(:niters => 20),
            )

            # Should have separate clustering results for each year
            @test length(clusters) == length(years)

            # Check that initial representatives are used - they may get different rep_period numbers
            # based on the clustering algorithm and grouping
            df_profiles_rep_periods =
                DuckDB.query(
                    connection,
                    "FROM profiles_rep_periods
                    ORDER BY year, scenario, profile_name, rep_period, timestep",
                ) |> DataFrame

            # Verify structure
            @test sort(names(df_profiles_rep_periods)) ==
                  ["profile_name", "rep_period", "scenario", "timestep", "value", "year"]

            # Should have data for all combinations: years × scenarios × profiles × num_rps × period_duration
            expected_rows =
                length(years) *
                length(scenarios) *
                length(profile_names) *
                num_rps *
                period_duration
            @test nrow(df_profiles_rep_periods) == expected_rows

            # Verify that initial representatives' values are present in the results
            # Since we provided initial representatives for all year-scenario combinations,
            # we should find some representative periods with values close to our initial values
            for year in years
                for scenario in scenarios
                    for profile in profile_names
                        expected_value = 10.0 + year * 0.1 + scenario * 0.01
                        # Find representative periods for this combination
                        matching_rows = df_profiles_rep_periods[
                            (df_profiles_rep_periods.year .== year) .& (df_profiles_rep_periods.scenario .== scenario) .& (df_profiles_rep_periods.profile_name .== profile),
                            :,
                        ]
                        @test nrow(matching_rows) > 0
                        # Check if any representative period has values close to our initial representatives
                        unique_values = unique(matching_rows.value)
                        @test any(abs.(unique_values .- expected_value) .< 1e-10)
                    end
                end
            end
        end

        @testset "Test1.2: initial representatives for only one year" begin
            connection = _new_connection_multi_scenario_year(;
                profile_names,
                num_timesteps,
                years,
                scenarios,
            )

            # Create initial representatives only for 2020 but for all scenarios
            # Key columns are: [:timestep, :profile_name, :year, :scenario]
            initial_representatives = DataFrame()
            for scenario in scenarios
                for profile in profile_names
                    rep_data = DataFrame([
                        :period => ones(Int, period_duration),
                        :timestep => 1:period_duration,
                        :year => fill(2020, period_duration),
                        :scenario => fill(scenario, period_duration),
                        :profile_name => fill(profile, period_duration),
                        :value => fill(
                            30.0 + (profile == "profile_A" ? 0.0 : 10.0),
                            period_duration,
                        ),
                    ])
                    initial_representatives = vcat(initial_representatives, rep_data)
                end
            end

            clusters = cluster!(
                connection,
                period_duration,
                num_rps;
                initial_representatives,
                clustering_kwargs = Dict(:display => :none),
                weight_fitting_kwargs = Dict(:niters => 20),
            )

            # Should still have clustering results for each year
            @test length(clusters) == length(years)

            # Check that initial representatives are only used for 2020 (all scenarios)
            # They may get different rep_period numbers based on clustering
            df_profiles_rep_periods =
                DuckDB.query(
                    connection,
                    "FROM profiles_rep_periods
                    WHERE year = 2020
                    ORDER BY scenario, profile_name, rep_period, timestep",
                ) |> DataFrame

            # Should have data for 2020 all scenarios: scenarios × profiles × num_rps × period_duration
            expected_rows =
                length(scenarios) * length(profile_names) * num_rps * period_duration
            @test nrow(df_profiles_rep_periods) == expected_rows

            # Verify that initial representatives' values are present in the 2020 results
            # We provided initial representatives only for 2020, so they should be found there
            for scenario in scenarios
                for profile in profile_names
                    expected_value = 30.0 + (profile == "profile_A" ? 0.0 : 10.0)
                    # Find representative periods for this combination in 2020
                    matching_rows = df_profiles_rep_periods[
                        (df_profiles_rep_periods.scenario .== scenario) .& (df_profiles_rep_periods.profile_name .== profile),
                        :,
                    ]
                    @test nrow(matching_rows) > 0
                    # Check if any representative period has values close to our initial representatives
                    unique_values = unique(matching_rows.value)
                    @test any(abs.(unique_values .- expected_value) .< 1e-10)
                end
            end
        end
    end

    @testset "Test2: cols_to_groupby = [:year, :scenario]" begin
        connection = _new_connection_multi_scenario_year(;
            profile_names,
            num_timesteps,
            years,
            scenarios,
        )

        layout = ProfilesTableLayout(; cols_to_groupby = [:year, :scenario])

        # Create initial representatives for specific year-scenario combinations
        # Key columns are: [:timestep, :profile_name, :year, :scenario]
        initial_representatives = DataFrame()

        # Add for year 2020, scenario 1
        for profile in profile_names
            rep_data = DataFrame([
                :period => ones(Int, period_duration),
                :timestep => 1:period_duration,
                :year => fill(2020, period_duration),
                :scenario => fill(1, period_duration),
                :profile_name => fill(profile, period_duration),
                :value =>
                    fill(50.0 + (profile == "profile_A" ? 0.0 : 10.0), period_duration),
            ])
            initial_representatives = vcat(initial_representatives, rep_data)
        end

        # Add for year 2021, scenario 2
        for profile in profile_names
            rep_data = DataFrame([
                :period => ones(Int, period_duration),
                :timestep => 1:period_duration,
                :profile_name => fill(profile, period_duration),
                :value =>
                    fill(70.0 + (profile == "profile_A" ? 0.0 : 10.0), period_duration),
                :year => fill(2021, period_duration),
                :scenario => fill(2, period_duration),
            ])
            initial_representatives = vcat(initial_representatives, rep_data)
        end

        clusters = cluster!(
            connection,
            period_duration,
            num_rps;
            layout,
            initial_representatives,
            clustering_kwargs = Dict(:display => :none),
            weight_fitting_kwargs = Dict(:niters => 20),
        )

        # Should have clustering results for each year-scenario combination
        @test length(clusters) == length(years) * length(scenarios)

        # Check rep_periods_data table
        df_rep_periods_data =
            DuckDB.query(
                connection,
                "FROM rep_periods_data ORDER BY year, scenario, rep_period",
            ) |> DataFrame

        @test sort(names(df_rep_periods_data)) ==
              ["num_timesteps", "rep_period", "resolution", "scenario", "year"]
        # Should have rep periods for each year-scenario combination
        @test nrow(df_rep_periods_data) == num_rps * length(years) * length(scenarios)

        # Check that initial representatives are used for the specific combinations
        df_profiles_rep_periods =
            DuckDB.query(
                connection,
                "FROM profiles_rep_periods
                WHERE ((year = 2020 AND scenario = 1) OR (year = 2021 AND scenario = 2))
                ORDER BY year, scenario, profile_name, rep_period, timestep",
            ) |> DataFrame

        # Should have data for the two specific year-scenario combinations where we provided initial reps
        # Each combination should have num_rps × length(profile_names) × period_duration rows
        expected_rows = 2 * num_rps * length(profile_names) * period_duration  # 2 combinations
        @test nrow(df_profiles_rep_periods) == expected_rows

        # Verify that initial representatives' values are present in the specific combinations
        # Check (2020, scenario 1)
        for profile in profile_names
            expected_value = 50.0 + (profile == "profile_A" ? 0.0 : 10.0)
            matching_rows = df_profiles_rep_periods[
                (df_profiles_rep_periods.year .== 2020) .& (df_profiles_rep_periods.scenario .== 1) .& (df_profiles_rep_periods.profile_name .== profile),
                :,
            ]
            @test nrow(matching_rows) > 0
            unique_values = unique(matching_rows.value)
            @test any(abs.(unique_values .- expected_value) .< 1e-10)
        end

        # Check (2021, scenario 2)
        for profile in profile_names
            expected_value = 70.0 + (profile == "profile_A" ? 0.0 : 10.0)
            matching_rows = df_profiles_rep_periods[
                (df_profiles_rep_periods.year .== 2021) .& (df_profiles_rep_periods.scenario .== 2) .& (df_profiles_rep_periods.profile_name .== profile),
                :,
            ]
            @test nrow(matching_rows) > 0
            unique_values = unique(matching_rows.value)
            @test any(abs.(unique_values .- expected_value) .< 1e-10)
        end
    end

    @testset "Test3: cols_to_groupby = [] (empty)" begin
        connection =
            _new_connection(; year = years[1], profile_names, num_timesteps = num_timesteps)

        layout = ProfilesTableLayout(; cols_to_groupby = [])

        # Create initial representatives without grouping columns
        # Key columns are: [:timestep, :profile_name]
        initial_representatives = DataFrame()
        for profile in profile_names
            rep_data = DataFrame([
                :period => ones(Int, period_duration),
                :timestep => 1:period_duration,
                :year => fill(years[1], period_duration),
                :profile_name => fill(profile, period_duration),
                :value => fill(
                    100.0 + (profile == "profile_A" ? 0.0 : 100.0),
                    period_duration,
                ),
            ])
            initial_representatives = vcat(initial_representatives, rep_data)
        end

        clusters = cluster!(
            connection,
            period_duration,
            num_rps;
            layout,
            initial_representatives,
            clustering_kwargs = Dict(:display => :none),
            weight_fitting_kwargs = Dict(:niters => 20),
        )

        # Should have only one clustering result (no grouping)
        @test length(clusters) == 1

        # Check rep_periods_data table
        df_rep_periods_data =
            DuckDB.query(connection, "FROM rep_periods_data ORDER BY rep_period") |>
            DataFrame

        @test sort(names(df_rep_periods_data)) ==
              ["num_timesteps", "rep_period", "resolution"]
        @test nrow(df_rep_periods_data) == num_rps

        # Check that initial representatives are used
        df_profiles_rep_periods =
            DuckDB.query(
                connection,
                "FROM profiles_rep_periods
                WHERE rep_period = 2  -- This should be our initial representative
                ORDER BY profile_name, timestep",
            ) |> DataFrame

        @test sort(names(df_profiles_rep_periods)) ==
              ["profile_name", "rep_period", "timestep", "value", "year"]

        # Should have data for our initial representative
        expected_rows = length(profile_names) * period_duration
        @test nrow(df_profiles_rep_periods) == expected_rows

        # Verify that initial representatives' values are present in the results
        # Check that rep_period = 2 contains our initial representative values
        for profile in profile_names
            expected_value = 100.0 + (profile == "profile_A" ? 0.0 : 100.0)
            matching_rows =
                df_profiles_rep_periods[df_profiles_rep_periods.profile_name .== profile, :]
            @test nrow(matching_rows) > 0
            # All values should be exactly our initial representative values
            @test all(abs.(matching_rows.value .- expected_value) .< 1e-10)
        end

        @testset "It doesn't throw when called twice" begin
            cluster!(connection, period_duration, num_rps; layout, initial_representatives)
        end
    end
end

@testset "cluster! function with cols_to_crossby parameter" begin
    period_duration = 24
    num_periods = 3
    num_timesteps = period_duration * num_periods
    num_rps = 2
    profile_names = ["profile_A", "profile_B"]
    years = [2020, 2021]
    scenarios = [1, 2]

    @testset "Test1: cols_to_groupby by default ([:year]) and cols_to_crossby = [:scenario]" begin
        connection = _new_connection_multi_scenario_year(;
            profile_names,
            num_timesteps,
            years,
            scenarios,
        )

        # Default layout has cols_to_groupby = [:year]
        layout = ProfilesTableLayout(; cols_to_crossby = [:scenario])

        clusters = cluster!(
            connection,
            period_duration,
            num_rps;
            layout,
            clustering_kwargs = Dict(:display => :none),
            weight_fitting_kwargs = Dict(:niters => 50),
        )

        # Verify clustering was done by year (groupby), crossing by scenario
        # Should have separate clustering results for each year
        @test length(clusters) == length(years)

        # Check rep_periods_data table
        df_rep_periods_data =
            DuckDB.query(connection, "FROM rep_periods_data ORDER BY year, rep_period") |>
            DataFrame

        @test sort(names(df_rep_periods_data)) ==
              ["num_timesteps", "rep_period", "resolution", "year"]
        @test length(unique(df_rep_periods_data.year)) == length(years)
        @test all(df_rep_periods_data.num_timesteps .== period_duration)

        # Should have num_rps for each year
        @test nrow(df_rep_periods_data) == num_rps * length(years)

        # Check rep_periods_mapping table - should include scenario column
        df_rep_periods_mapping =
            DuckDB.query(
                connection,
                "FROM rep_periods_mapping ORDER BY year, scenario, period, rep_period",
            ) |> DataFrame

        @test sort(names(df_rep_periods_mapping)) ==
              ["period", "rep_period", "scenario", "weight", "year"]

        # For each year-scenario combination, should have entries for all periods
        expected_mapping_rows = length(years) * length(scenarios) * num_periods
        @test nrow(df_rep_periods_mapping) == expected_mapping_rows

        # The scenario column should have all scenarios for each year
        unique_mapping_combinations = unique(df_rep_periods_mapping[:, [:year, :scenario]])
        @test nrow(unique_mapping_combinations) == length(years) * length(scenarios)

        # The maximum period per year-scenario should equal num_periods
        grouped = groupby(df_rep_periods_mapping, [:year, :scenario])
        max_periods = combine(grouped, :period => maximum => :max_period)
        @test all(max_periods.max_period .== num_periods)

        # Check profiles_rep_periods table - should NOT include scenario column (it's in cols_to_crossby)
        # but should include year column (it's in cols_to_groupby)
        df_profiles_rep_periods =
            DuckDB.query(
                connection,
                "FROM profiles_rep_periods ORDER BY year, profile_name, rep_period, timestep",
            ) |> DataFrame

        @test sort(names(df_profiles_rep_periods)) ==
              ["profile_name", "rep_period", "timestep", "value", "year"]

        # Should have data for: years × profiles × num_rps × period_duration
        # Note: scenarios are NOT multiplied here since scenario column is not in profiles_rep_periods
        expected_rows = length(years) * length(profile_names) * num_rps * period_duration
        @test nrow(df_profiles_rep_periods) == expected_rows

        # Verify all years are present
        unique_years = unique(df_profiles_rep_periods.year)
        @test Set(unique_years) == Set(years)

        @testset "It doesn't throw when called twice" begin
            cluster!(connection, period_duration, num_rps; layout)
        end
    end

    @testset "Test2: cols_to_groupby = [] and cols_to_crossby = [:year, :scenario]" begin
        connection = _new_connection_multi_scenario_year(;
            profile_names,
            num_timesteps,
            years,
            scenarios,
        )

        # No grouping, crossing by both year and scenario
        layout = ProfilesTableLayout(;
            cols_to_groupby = [],
            cols_to_crossby = [:year, :scenario],
        )

        clusters = cluster!(
            connection,
            period_duration,
            num_rps;
            layout,
            clustering_kwargs = Dict(:display => :none),
            weight_fitting_kwargs = Dict(:niters => 50),
        )

        # Verify clustering was done globally (no grouping)
        # Should have only one clustering result
        @test length(clusters) == 1

        # Check rep_periods_data table - should NOT have year/scenario columns
        df_rep_periods_data =
            DuckDB.query(connection, "FROM rep_periods_data ORDER BY rep_period") |>
            DataFrame

        @test sort(names(df_rep_periods_data)) ==
              ["num_timesteps", "rep_period", "resolution"]
        @test nrow(df_rep_periods_data) == num_rps
        @test all(df_rep_periods_data.num_timesteps .== period_duration)

        # Check rep_periods_mapping table - should include year and scenario columns
        df_rep_periods_mapping =
            DuckDB.query(
                connection,
                "FROM rep_periods_mapping ORDER BY year, scenario, period, rep_period",
            ) |> DataFrame

        @test sort(names(df_rep_periods_mapping)) ==
              ["period", "rep_period", "scenario", "weight", "year"]

        # Since crossing by year and scenario, each combination should have period mappings
        expected_mapping_rows = length(years) * length(scenarios) * num_periods
        @test nrow(df_rep_periods_mapping) == expected_mapping_rows

        # Verify all year-scenario combinations are present in mapping
        unique_mapping_combinations = unique(df_rep_periods_mapping[:, [:year, :scenario]])
        @test nrow(unique_mapping_combinations) == length(years) * length(scenarios)

        # The maximum period per year-scenario should equal num_periods
        grouped = groupby(df_rep_periods_mapping, [:year, :scenario])
        max_periods = combine(grouped, :period => maximum => :max_period)
        @test all(max_periods.max_period .== num_periods)

        # Check profiles_rep_periods table - should NOT have year or scenario columns
        # since both are in cols_to_crossby (and cols_to_groupby is empty)
        df_profiles_rep_periods =
            DuckDB.query(
                connection,
                "FROM profiles_rep_periods ORDER BY profile_name, rep_period, timestep",
            ) |> DataFrame

        @test sort(names(df_profiles_rep_periods)) ==
              ["profile_name", "rep_period", "timestep", "value"]

        # Should have data for: profiles × num_rps × period_duration
        # Note: years and scenarios are NOT multiplied here since neither column is in profiles_rep_periods
        expected_rows = length(profile_names) * num_rps * period_duration
        @test nrow(df_profiles_rep_periods) == expected_rows

        @testset "It doesn't throw when called twice" begin
            cluster!(connection, period_duration, num_rps; layout)
        end
    end
end
