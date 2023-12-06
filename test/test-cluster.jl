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
      clustering_data = DataFrame([
        :time_step => repeat(1:4, inner = 2),
        :asset => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])
      TulipaClustering.split_into_periods!(clustering_data; period_duration = 2)

      size(clustering_data) == (8, 4)
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
end

@testset "K-means clustering" begin
  @testset "Make sure that k-means returns the original periods when n_rp == n_periods" begin
    @test begin
      clustering_data = DataFrame([
        :period => repeat(1:2, inner = 4),
        :time_step => repeat(1:2, inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])
      clustering_result =
        find_representative_periods(clustering_data, 2; method = :k_means, init = :kmcen)

      clustering_result.weight_matrix == [1.0 0.0; 0.0 1.0]
    end

    @test begin
      clustering_data = DataFrame([
        :period => repeat(1:2, inner = 4),
        :time_step => repeat(1:2, inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])
      clustering_data = clustering_data[1:(end - 2), :]
      clustering_result =
        find_representative_periods(clustering_data, 2; method = :k_means, init = :kmcen)

      clustering_result.weight_matrix == [1.0 0.0; 0.0 1.0]
    end

    @test begin
      clustering_data = DataFrame([
        :period => repeat(1:3, inner = 4),
        :time_step => repeat(1:2, inner = 2, outer = 3),
        :technology => repeat(["Solar", "Nuclear"], 6),
        :value => 5:16,
      ])
      clustering_data = clustering_data[1:(end - 2), :]
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

@testset "K-medoids clustering" begin
  @testset "Make sure that k-medoids returns the original periods when n_rp == n_periods" begin
    @test begin
      clustering_data = DataFrame([
        :period => repeat(1:2, inner = 4),
        :time_step => repeat(1:2, inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])
      clustering_result =
        find_representative_periods(clustering_data, 2; method = :k_medoids, init = :kmcen)

      clustering_result.weight_matrix == [1.0 0.0; 0.0 1.0]
    end

    @test begin
      clustering_data = DataFrame([
        :period => repeat(1:2, inner = 4),
        :time_step => repeat(1:2, inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])
      clustering_data = clustering_data[1:(end - 2), :]
      clustering_result =
        find_representative_periods(clustering_data, 2; method = :k_medoids, init = :kmcen)

      clustering_result.weight_matrix == [1.0 0.0; 0.0 1.0]
    end

    @test begin
      clustering_data = DataFrame([
        :period => repeat(1:3, inner = 4),
        :time_step => repeat(1:2, inner = 2, outer = 3),
        :technology => repeat(["Solar", "Nuclear"], 6),
        :value => 5:16,
      ])
      clustering_data = clustering_data[1:(end - 2), :]
      clustering_result = find_representative_periods(
        clustering_data,
        2;
        drop_incomplete_last_period = true,
        method = :k_medoids,
        init = :kmcen,
      )

      clustering_result.weight_matrix == [1.25 0.0; 0.0 1.25]
    end
  end
end

@testset "Bad clustering method" begin
  @testset "Make sure that clustering fails when incorrect method is given" begin
    @test_throws ArgumentError begin
      clustering_data = DataFrame([
        :period => repeat(1:2, inner = 4),
        :time_step => repeat(1:2, inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])
      clustering_result =
        find_representative_periods(clustering_data, 2; method = :bad_method, init = :kmcen)
    end
  end
end
