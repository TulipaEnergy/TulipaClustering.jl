@testset "Clustering validation" begin
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
