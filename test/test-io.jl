@testset "Output saving" begin
  @testset "Make sure clustering result is saved for database_schema = '$database_schema'" for database_schema in
                                                                                               [
    "",
    "cluster",
  ]
    dir = joinpath(OUTPUT_FOLDER, "temp")

    profile_names = ["solar", "wind"]
    timeframe_duration = 20

    profiles = DataFrame(;
      profile_name = repeat(
        profile_names;
        inner = timeframe_duration,
      ),
      timestep = repeat(
        1:timeframe_duration;
        outer = length(profile_names),
      ),
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

    tables = [row.table_name for row in DBInterface.execute(
      connection,
      "SELECT table_name
      FROM duckdb_tables()
      $where_string
      ORDER BY table_name",
    )]
    @test tables == Union{Missing, String}[
      "profiles_rep_periods",
      "rep_periods_data",
      "rep_periods_mapping",
      "timeframe_data",
    ]

    @testset "rep_periods_data" begin
      rep_periods_data_df =
        DBInterface.execute(connection, "SELECT * FROM $(prefix)rep_periods_data") |>
        DataFrame
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
end
