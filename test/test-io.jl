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
      size(clustering_data.demand.data) == (175200, 3) &&
        size(clustering_data.generation_availability.data) == (525600, 4)
    end
  end
end
