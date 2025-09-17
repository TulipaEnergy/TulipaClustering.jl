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

    @test sort(names(df_rep_periods_data)) == ["num_timesteps", "rep_period", "resolution"]

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
    df_timeframe_data = DuckDB.query(
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
          ["profile_name", "rep_period", "timestep", "value"]

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
        ["profile_name", "rep_period", "timestep", "value"]

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

@testset "cluster! with bad cols to group by" begin
  period_duration = 24
  num_periods = 5
  num_timesteps = period_duration * num_periods
  num_rps = 3
  years = [2020, 2025]
  scenarios = [1, 2]
  profile_names = ["name1", "name2"]
  layout = ProfilesTableLayout(;
    timestep = :ts,
    value = :val,
    year = :years,
    scenario = :scn,
    cols_to_groupby = [:year], # incorrect column name 'year' instead of 'years'
  )

  # Create a connection with custom column names to match the custom layout
  connection = _new_connection_multi_scenario_year(;
    profile_names,
    num_timesteps,
    years,
    scenarios,
    layout,
  )

  error_msg = "ArgumentError: Column 'year' in 'cols_to_groupby' is not defined in the layout"
  @test_throws error_msg throw(cluster!(connection, period_duration, num_rps; layout))
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
    connection =
      _new_connection_multi_scenario_year(; profile_names, num_timesteps, years, scenarios)

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
      length(years) * length(scenarios) * length(profile_names) * num_rps * period_duration
    @test nrow(df_profiles_rep_periods) == expected_rows

    @testset "It doesn't throw when called twice" begin
      cluster!(connection, period_duration, num_rps)
    end
  end

  @testset "Test2: cols_to_groupby = [:year, :scenario]" begin
    connection =
      _new_connection_multi_scenario_year(; profile_names, num_timesteps, years, scenarios)

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
      length(years) * length(scenarios) * length(profile_names) * num_rps * period_duration
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
