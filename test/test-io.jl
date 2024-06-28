@testset "Output saving" begin
  @testset "Make sure clustering result is saved" begin
    dir = joinpath(OUTPUT_FOLDER, "temp")

    n_periods = 3
    n_steps = 2
    nodes = ["a", "b"]
    n_nodes = length(nodes)
    technologies = ["solar", "wind"]
    n_technologies = length(technologies)
    df_length = n_periods * n_steps * n_technologies
    df = DataFrame(
      rep_period = repeat(1:n_periods, inner = n_steps * n_nodes),
      time_step = repeat(1:n_steps, inner = n_nodes, outer = n_periods),
      asset = repeat(nodes, outer = n_periods * n_steps),
      value = convert.(Float64, 1:df_length) ./ df_length,
    )
    weight_matrix = repeat(Matrix{Float64}(I, 3, 3), 10) |> sparse
    clustering_data = TulipaClustering.ClusteringResult(df, weight_matrix)

    @test begin
      TulipaClustering.write_clustering_result_to_csv_folder(dir, clustering_data)
      ["assets-profiles.csv", "rp-weights.csv"] ⊆ readdir(dir)
    end

    @test begin
      TulipaClustering.write_csv_with_prefixes(joinpath(dir, "no-prefix.csv"), df)
      "no-prefix.csv" ∈ readdir(dir)
    end
    rm(dir; force = true, recursive = true)
  end
end
