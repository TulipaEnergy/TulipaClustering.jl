@testset "Input validation" begin
  @testset "Make sure that input validation fails for bad files" begin
    dir = joinpath(INPUT_FOLDER, "bad")
    @test_throws CSV.Error TulipaClustering.read_csv_with_schema(
      joinpath(dir, "demand.csv"),
      TulipaClustering.DemandData,
    )
  end

  @testset "Make sure that input files are read into dataframes" begin
    @test begin
      dir = joinpath(INPUT_FOLDER, "EU")
      clustering_data = TulipaClustering.read_clustering_data_from_csv_folder(dir)
      size(clustering_data.demand) == (175200, 3) &&
        size(clustering_data.generation_availability) == (525600, 4)
    end
  end
end

@testset "Output saving" begin
  @testset "Make sure clustering result is saved" begin
    dir = joinpath(OUTPUT_FOLDER, "temp")

    n_periods = 3
    n_steps = 2
    nodes = ["a", "b"]
    n_nodes = length(nodes)
    demand_df_length = n_periods * n_steps * n_nodes
    technologies = ["solar", "wind"]
    n_technologies = length(technologies)
    generation_df_length = demand_df_length * n_technologies
    demand_df = DataFrame(
      rep_period = repeat(1:n_periods, inner = n_steps * n_nodes),
      time_step = repeat(1:n_steps, inner = n_nodes, outer = n_periods),
      node = repeat(nodes, outer = n_periods * n_steps),
      value = convert.(Float64, 1:demand_df_length),
    )
    generation_availability_df = DataFrame(
      rep_period = repeat(1:n_periods, inner = n_steps * n_nodes * n_technologies),
      time_step = repeat(1:n_steps, inner = n_nodes * n_technologies, outer = n_periods),
      node = repeat(nodes, outer = n_periods * n_steps, inner = n_technologies),
      technology = repeat(technologies, outer = demand_df_length),
      value = (1:generation_df_length) ./ generation_df_length,
    )
    weight_matrix = repeat(Matrix{Float64}(I, 3, 3), 10) |> sparse
    clustering_data =
      TulipaClustering.ClusteringResult(demand_df, generation_availability_df, weight_matrix)

    @test begin
      TulipaClustering.write_clustering_result_to_csv_folder(dir, clustering_data)
      ["demand.csv", "generation-availability.csv", "rp-weights.csv"] ⊆ readdir(dir)
    end

    @test begin
      TulipaClustering.write_csv_with_prefixes(joinpath(dir, "no-prefix.csv"), demand_df)
      "no-prefix.csv" ∈ readdir(dir)
    end
    rm(dir; force = true, recursive = true)
  end
end
