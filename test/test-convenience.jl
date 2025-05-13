@testset "Trasform wide in long" begin
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
end

@testset "cluster! with database_schema '$database_schema'" for database_schema in
                                                                ("", "cluster")
  period_duration = 24
  num_periods = 7
  num_timesteps = period_duration * num_periods
  num_rps = 4
  profile_names = ["name1", "name2", "name3"]
  years = [2030, 2050]

  connection = _new_connection(; profile_names, years, num_timesteps)
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
  )
  prefix = database_schema != "" ? "$(database_schema)." : ""

  @testset "rep_periods_data" begin
    df_rep_periods_data =
      DuckDB.query(
        connection,
        "FROM $(prefix)rep_periods_data
        ORDER BY year, rep_period",
      ) |> DataFrame

    @test sort(names(df_rep_periods_data)) ==
          ["num_timesteps", "rep_period", "resolution", "year"]

    @test df_rep_periods_data.year == repeat(years; inner = num_rps)
    @test df_rep_periods_data.rep_period == repeat(1:num_rps; outer = length(years))
    @test all(df_rep_periods_data.resolution .== 1.0)
    @test all(df_rep_periods_data.num_timesteps .== period_duration)
  end

  @testset "rep_periods_mapping" begin
    df_rep_periods_mapping =
      DuckDB.query(
        connection,
        "FROM $(prefix)rep_periods_mapping
        ORDER BY year, period, rep_period",
      ) |> DataFrame

    @test sort(names(df_rep_periods_mapping)) == ["period", "rep_period", "weight", "year"]

    @test size(df_rep_periods_mapping, 1) ≥ length(years) * num_periods
  end

  @testset "timeframe_data" begin
    df_timeframe_data = DuckDB.query(
      connection,
      "FROM $(prefix)timeframe_data
      ORDER BY year, period",
    ) |> DataFrame

    @test sort(names(df_timeframe_data)) == ["num_timesteps", "period", "year"]

    @test df_timeframe_data.year == repeat(years; inner = num_periods)
    @test df_timeframe_data.period == repeat(1:num_periods; outer = length(years))
    @test all(df_timeframe_data.num_timesteps .== period_duration)
  end

  @testset "profiles_rep_periods" begin
    df_profiles_rep_periods =
      DuckDB.query(
        connection,
        "FROM $(prefix)profiles_rep_periods
        ORDER BY profile_name, year, rep_period, timestep",
      ) |> DataFrame

    @test sort(names(df_profiles_rep_periods)) ==
          ["profile_name", "rep_period", "timestep", "value", "year"]

    @test df_profiles_rep_periods.profile_name ==
          repeat(profile_names; inner = period_duration * num_rps * length(years))
    @test df_profiles_rep_periods.year ==
          repeat(years; inner = period_duration * num_rps, outer = length(profile_names))
    @test df_profiles_rep_periods.rep_period == repeat(
      1:num_rps;
      inner = period_duration,
      outer = length(profile_names) * length(years),
    )
    @test df_profiles_rep_periods.timestep ==
          repeat(1:period_duration; outer = length(profile_names) * length(years) * num_rps)
  end

  @testset "It doesn't throw when called twice" begin
    cluster!(connection, period_duration, num_rps; database_schema)
  end
end

@testset "dummy_cluster! with database_schema '$database_schema'" for database_schema in
                                                                      ("", "cluster")
  period_duration = 24 * 7
  num_periods = 1
  num_timesteps = period_duration * num_periods
  num_rps = 1
  profile_names = ["name1", "name2", "name3"]
  years = [2030, 2050]

  connection = _new_connection(; profile_names, years, num_timesteps)

  clusters = dummy_cluster!(connection; database_schema)
  prefix = database_schema != "" ? "$(database_schema)." : ""

  df_rep_periods_data =
    DuckDB.query(
      connection,
      "FROM $(prefix)rep_periods_data
      ORDER BY year, rep_period",
    ) |> DataFrame
  df_rep_periods_mapping =
    DuckDB.query(
      connection,
      "FROM $(prefix)rep_periods_mapping
      ORDER BY year, period, rep_period",
    ) |> DataFrame
  df_profiles_rep_periods =
    DuckDB.query(
      connection,
      "FROM $(prefix)profiles_rep_periods
      ORDER BY profile_name, year, rep_period, timestep",
    ) |> DataFrame

  @test sort(names(df_rep_periods_data)) ==
        ["num_timesteps", "rep_period", "resolution", "year"]
  @test sort(names(df_rep_periods_mapping)) == ["period", "rep_period", "weight", "year"]
  @test sort(names(df_profiles_rep_periods)) ==
        ["profile_name", "rep_period", "timestep", "value", "year"]

  @test df_rep_periods_data.year == repeat(years; inner = num_rps)
  @test df_rep_periods_data.rep_period == repeat(1:num_rps; outer = length(years))
  @test all(df_rep_periods_data.resolution .== 1.0)
  @test all(df_rep_periods_data.num_timesteps .== period_duration)

  @test size(df_rep_periods_mapping, 1) ≥ length(years) * num_periods

  @test df_profiles_rep_periods.profile_name ==
        repeat(profile_names; inner = period_duration * num_rps * length(years))
  @test df_profiles_rep_periods.year ==
        repeat(years; inner = period_duration * num_rps, outer = length(profile_names))
  @test df_profiles_rep_periods.rep_period == repeat(
    1:num_rps;
    inner = period_duration,
    outer = length(profile_names) * length(years),
  )
  @test df_profiles_rep_periods.timestep ==
        repeat(1:period_duration; outer = length(profile_names) * length(years) * num_rps)

  @testset "It doesn't throw when called twice" begin
    dummy_cluster!(connection; database_schema)
  end
end
