@testset "Output saving" begin
  @testset "Make sure clustering result is saved" begin
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
    TulipaClustering.write_clustering_result_to_tables(connection, clustering_data)

    tables = DBInterface.execute(connection, "SHOW TABLES") |> DataFrame |> df -> df.name
    @test sort(tables) ==
          Union{Missing, String}["profiles_rep_periods", "rep_periods_data", "rep_periods_mapping"]

    @testset "rep_periods_data" begin
      rep_periods_data_df =
        DBInterface.execute(connection, "SELECT * FROM rep_periods_data") |> DataFrame
      @test rep_periods_data_df.rep_period == 1:num_rep_periods
      @test rep_periods_data_df.num_timesteps == [
        fill(period_duration, num_rep_periods - 1)
        timeframe_duration % period_duration
      ]
      @test all(rep_periods_data_df.resolution .== 1)
    end
  end
end
