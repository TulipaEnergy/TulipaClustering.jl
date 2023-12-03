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

@testset "Data valudation" begin
  @testset "Make sure that when the columns are right validation works and the key columns are found" begin
    @test begin
      df = DataFrame([:period => [1, 1, 2], :time_step => [1, 2, 1], :a .=> "a", :value => 1:3])
      keys = TulipaClustering.validate_df_and_find_key_columns(df)

      keys == [:time_step, :a]
    end
  end

  @testset "Make sure that the validation fails when `time_step` column is absent" begin
    @test_throws DomainError begin
      df = DataFrame([:value => 1])
      keys = TulipaClustering.validate_df_and_find_key_columns(df)
    end
  end

  @testset "Make sure that the validation fails when `value` column is absent" begin
    @test_throws DomainError begin
      df = DataFrame([:a => 1])
      keys = TulipaClustering.validate_df_and_find_key_columns(df)
    end
  end

  @testset "Make sure that the validation fails when `period` column is absent" begin
    @test_throws DomainError begin
      df = DataFrame([:time_step => 1, :value => 1])
      keys = TulipaClustering.validate_df_and_find_key_columns(df)
    end
  end

  @testset "Make sure that finding auxiliary information fails when dataframes have different number of periods" begin
    @test_throws DomainError begin
      demand = DataFrame([:period => [1, 1], :time_step => 1:2, :value => 1:2])
      generation_availability = DataFrame([
        :period => repeat(1:2, inner = 4),
        :time_step => repeat(1:2, inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])
      clustering_data = TulipaClustering.ClusteringData(demand, generation_availability)
      TulipaClustering.find_auxiliary_data(clustering_data)
    end
  end

  @testset "Make sure that finding auxiliary information fails when dataframes have different period durations" begin
    @test_throws DomainError begin
      demand =
        DataFrame([:period => repeat(1:2, inner = 3), :time_step => repeat(1:3, 2), :value => 1:6])
      generation_availability = DataFrame([
        :period => repeat(1:2, inner = 4),
        :time_step => repeat(1:2, inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])
      clustering_data = TulipaClustering.ClusteringData(demand, generation_availability)
      TulipaClustering.find_auxiliary_data(clustering_data)
    end
  end

  @testset "Make sure that finding auxiliary information fails when dataframes have different period durations" begin
    @test_throws DomainError begin
      demand = DataFrame([:period => [1, 1, 2], :time_step => [1, 2, 1], :value => 1:3])
      generation_availability = DataFrame([
        :period => repeat(1:2, inner = 4),
        :time_step => repeat(1:2, inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])
      clustering_data = TulipaClustering.ClusteringData(demand, generation_availability)
      TulipaClustering.find_auxiliary_data(clustering_data)
    end
  end
end

@testset "K-means clustering" begin
  @testset "Make sure that k-means returns the original periods when n_rp == n_periods" begin
    @test begin
      demand =
        DataFrame([:period => repeat(1:2, inner = 2), :time_step => repeat(1:2, 2), :value => 1:4])
      generation_availability = DataFrame([
        :period => repeat(1:2, inner = 4),
        :time_step => repeat(1:2, inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])
      clustering_data = TulipaClustering.ClusteringData(demand, generation_availability)
      clustering_result =
        find_representative_periods(clustering_data, 2; method = :k_means, init = :kmcen)

      clustering_result.weight_matrix == [1.0 0.0; 0.0 1.0]
    end

    @test begin
      demand = DataFrame([:period => [1, 1, 2], :time_step => [1, 2, 1], :value => 1:3])
      generation_availability = DataFrame([
        :period => repeat(1:2, inner = 4),
        :time_step => repeat(1:2, inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])
      generation_availability = generation_availability[1:(end - 2), :]
      clustering_data = TulipaClustering.ClusteringData(demand, generation_availability)
      clustering_result =
        find_representative_periods(clustering_data, 2; method = :k_means, init = :kmcen)

      clustering_result.weight_matrix == [1.0 0.0; 0.0 0.5]
    end

    @test begin
      demand = DataFrame([:period => [1, 1, 2, 2, 3], :time_step => [1, 2, 1, 2, 1], :value => 1:5])
      generation_availability = DataFrame([
        :period => repeat(1:3, inner = 4),
        :time_step => repeat(1:2, inner = 2, outer = 3),
        :technology => repeat(["Solar", "Nuclear"], 6),
        :value => 5:16,
      ])
      generation_availability = generation_availability[1:(end - 2), :]
      clustering_data = TulipaClustering.ClusteringData(demand, generation_availability)
      clustering_result = find_representative_periods(
        clustering_data,
        2;
        drop_incomplete_last_period = true,
        method = :k_means,
        init = :kmcen,
      )

      clustering_result.weight_matrix == [1.25 0.0; 0.0 1.25]
    end
  end
end
