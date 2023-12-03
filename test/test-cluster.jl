@testset "Period combination" begin
  @testset "Make sure that combining perids returns a correct data frame" begin
    @test begin
      df = DataFrame([:period => [1, 1, 2], :time_step => [1, 2, 1], :value => 1:3])
      TulipaClustering.combine_periods!(df)

      size(df) == (3, 2) && df.time_step == collect(1:3) && df.value == collect(1:3)
    end
  end

  @testset "Make sure that combining perids fails when there is no time step in the data frame" begin
    @test_throws DomainError begin
      df = DataFrame([:period => [1, 1, 2], :value => 1:3])
      TulipaClustering.combine_periods!(df)
    end
  end

  @testset "Make sure that combining perids does nothing when there are time steps but no periods" begin
    @test begin
      df = DataFrame([:time_step => [1, 2], :value => 1:2])
      TulipaClustering.combine_periods!(df)

      size(df) == (2, 2) && df.time_step == collect(1:2) && df.value == collect(1:2)
    end
  end
end

@testset "Period splitting" begin
  @testset "Make sure that splitting perids works as expected" begin
    @test begin
      df = DataFrame([:time_step => 1:3, :value => 1:3])
      TulipaClustering.split_into_periods!(df; period_duration = 2)

      size(df) == (3, 3) &&
        df.period == [1, 1, 2] &&
        df.time_step == [1, 2, 1] &&
        df.value == collect(1:3)
    end
  end

  @testset "Make sure that there is only one period when period_duration is not provided" begin
    @test begin
      df = DataFrame([:time_step => 1:3, :value => 1:3])
      TulipaClustering.split_into_periods!(df)

      size(df) == (3, 3) &&
        df.period == [1, 1, 1] &&
        df.time_step == [1, 2, 3] &&
        df.value == collect(1:3)
    end
  end

  @testset "Make sure that period splitting works on clustering data" begin
    @test begin
      demand = DataFrame([:time_step => 1:4, :value => 1:4])
      generation_availability = DataFrame([
        :time_step => repeat(1:4, inner = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])
      clustering_data = TulipaClustering.ClusteringData(demand, generation_availability)
      TulipaClustering.split_into_periods!(clustering_data; period_duration = 2)

      size(clustering_data.demand) == (4, 3) &&
        size(clustering_data.generation_availability) == (8, 4)
    end
  end
end
