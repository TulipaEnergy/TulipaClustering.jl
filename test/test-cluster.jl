@testset "Period combination" begin
  @testset "Make sure that combining periods returns a correct data frame" begin
    @test begin
      df = DataFrame([:period => [1, 1, 2], :timestep => [1, 2, 1], :value => 1:3])
      TulipaClustering.combine_periods!(df)

      size(df) == (3, 2) && df.timestep == collect(1:3) && df.value == collect(1:3)
    end
  end

  @testset "Make sure that combining periods returns a correct data frame using layout" begin
    @test begin
      df = DataFrame([:periods => [1, 1, 2], :time_step => [1, 2, 1], :value => 1:3])
      TulipaClustering.combine_periods!(
        df;
        layout = DataFrameLayout(; timestep = :time_step, period = :periods),
      )

      size(df) == (3, 2) && df.time_step == collect(1:3) && df.value == collect(1:3)
    end
  end

  @testset "Make sure that combining periods fails when there is no time step in the data frame" begin
    @test_throws DomainError begin
      df = DataFrame([:period => [1, 1, 2], :value => 1:3])
      TulipaClustering.combine_periods!(df)
    end
  end

  @testset "Make sure that combining periods does nothing when there are time steps but no periods" begin
    @test begin
      df = DataFrame([:timestep => [1, 2], :value => 1:2])
      TulipaClustering.combine_periods!(df)

      size(df) == (2, 2) && df.timestep == collect(1:2) && df.value == collect(1:2)
    end
  end
end

@testset "Auxiliary data" begin
  @testset "Default layout computes durations and keys" begin
    @test begin
      df = DataFrame([
        :period => [1, 1, 2, 2, 3],
        :timestep => [1, 2, 1, 2, 1],
        :a .=> "x",
        :value => 10:14,
      ])
      aux = find_auxiliary_data(df)
      (
        aux.key_columns == [:timestep, :a] &&
        aux.period_duration == 2 &&
        aux.last_period_duration == 1 &&
        aux.n_periods == 3
      )
    end
  end

  @testset "Custom layout computes durations and keys" begin
    @test begin
      layout = DataFrameLayout(; period = :p, timestep = :ts, value = :val)
      df = DataFrame([
        :p => [1, 1, 2, 2, 3],
        :ts => [1, 2, 1, 2, 1],
        :a .=> "x",
        :val => 10:14,
      ])
      aux = find_auxiliary_data(df; layout)
      (
        aux.key_columns == [:ts, :a] &&
        aux.period_duration == 2 &&
        aux.last_period_duration == 1 &&
        aux.n_periods == 3
      )
    end
  end

  @testset "Missing columns errors respect layout" begin
    @test_throws DomainError begin
      layout = DataFrameLayout(; period = :p, timestep = :ts, value = :val)
      df = DataFrame([:val => 1]) # missing :ts and :p
      find_auxiliary_data(df; layout)
    end

    @test_throws DomainError begin
      layout = DataFrameLayout(; period = :p, timestep = :ts, value = :val)
      df = DataFrame([:p => 1, :ts => 1]) # missing :val
      find_auxiliary_data(df; layout)
    end

    @test_throws DomainError begin
      layout = DataFrameLayout(; period = :p, timestep = :ts, value = :val)
      df = DataFrame([:ts => 1, :val => 1]) # missing :p
      find_auxiliary_data(df; layout)
    end
  end
end

@testset "Period splitting" begin
  @testset "Make sure that splitting perids works as expected" begin
    @test begin
      df = DataFrame([:timestep => 1:3, :value => 1:3])
      TulipaClustering.split_into_periods!(df; period_duration = 2)

      size(df) == (3, 3) &&
        df.period == [1, 1, 2] &&
        df.timestep == [1, 2, 1] &&
        df.value == collect(1:3)
    end
  end

  @testset "Make sure that splitting periods works with custom layout" begin
    @test begin
      df = DataFrame([:time_step => 1:3, :value => 1:3])
      TulipaClustering.split_into_periods!(
        df;
        period_duration = 2,
        layout = DataFrameLayout(; timestep = :time_step, period = :periods),
      )

      size(df) == (3, 3) &&
        df.periods == [1, 1, 2] &&
        df.time_step == [1, 2, 1] &&
        df.value == collect(1:3)
    end
  end

  @testset "Make sure that there is only one period when period_duration is not provided" begin
    @test begin
      df = DataFrame([:timestep => 1:3, :value => 1:3])
      TulipaClustering.split_into_periods!(df)

      size(df) == (3, 3) &&
        df.period == [1, 1, 1] &&
        df.timestep == [1, 2, 3] &&
        df.value == collect(1:3)
    end
  end

  @testset "Make sure that one period is created when duration not provided with custom layout" begin
    @test begin
      df = DataFrame([:time_step => 1:3, :value => 1:3])
      TulipaClustering.split_into_periods!(
        df;
        layout = DataFrameLayout(; timestep = :time_step, period = :periods),
      )

      size(df) == (3, 3) &&
        df.periods == [1, 1, 1] &&
        df.time_step == [1, 2, 3] &&
        df.value == collect(1:3)
    end
  end

  @testset "Make sure that period splitting works on clustering data" begin
    @test begin
      clustering_data = DataFrame([
        :timestep => repeat(1:4; inner = 2),
        :profile_name => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])
      TulipaClustering.split_into_periods!(clustering_data; period_duration = 2)

      size(clustering_data) == (8, 4)
    end
  end
end

@testset "Data validation" begin
  @testset "Make sure that when the columns are right validation works and the key columns are found" begin
    @test begin
      df =
        DataFrame([:period => [1, 1, 2], :timestep => [1, 2, 1], :a .=> "a", :value => 1:3])
      keys = TulipaClustering.validate_df_and_find_key_columns(df)

      keys == [:timestep, :a]
    end
  end

  @testset "Make sure validation works with custom layout and finds key columns" begin
    @test begin
      layout = DataFrameLayout(; period = :p, timestep = :ts, value = :val)
      df = DataFrame([:p => [1, 1, 2], :ts => [1, 2, 1], :a .=> "a", :val => 1:3])
      keys = TulipaClustering.validate_df_and_find_key_columns(df; layout)

      keys == [:ts, :a]
    end
  end

  @testset "Make sure that the validation fails when `timestep` column is absent" begin
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
      df = DataFrame([:timestep => 1, :value => 1])
      keys = TulipaClustering.validate_df_and_find_key_columns(df)
    end
  end

  @testset "Make sure that the validation fails with custom layout when columns are absent" begin
    @test_throws DomainError begin
      layout = DataFrameLayout(; period = :p, timestep = :ts, value = :val)
      df = DataFrame([:val => 1]) # missing :ts and :p
      TulipaClustering.validate_df_and_find_key_columns(df; layout)
    end

    @test_throws DomainError begin
      layout = DataFrameLayout(; period = :p, timestep = :ts, value = :val)
      df = DataFrame([:p => 1, :ts => 1]) # missing :val
      TulipaClustering.validate_df_and_find_key_columns(df; layout)
    end

    @test_throws DomainError begin
      layout = DataFrameLayout(; period = :p, timestep = :ts, value = :val)
      df = DataFrame([:ts => 1, :val => 1]) # missing :p
      TulipaClustering.validate_df_and_find_key_columns(df; layout)
    end
  end
end

@testset "K-means clustering" begin
  @testset "Make sure that k-means returns the original periods when n_rp == n_periods" begin
    @test begin
      clustering_data = DataFrame([
        :period => repeat(1:2; inner = 4),
        :timestep => repeat(1:2; inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])
      clustering_result =
        find_representative_periods(clustering_data, 2; method = :k_means, init = :kmcen)

      clustering_result.weight_matrix == [1.0 0.0; 0.0 1.0]
    end

    @test begin
      clustering_data = DataFrame([
        :period => repeat(1:2; inner = 4),
        :timestep => repeat(1:2; inner = 2, outer = 2),
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
        :period => repeat(1:3; inner = 4),
        :timestep => repeat(1:2; inner = 2, outer = 3),
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

  @testset "K-means works with custom layout" begin
    @test begin
      layout = DataFrameLayout(; period = :p, timestep = :ts, value = :val)
      clustering_data = DataFrame([
        :p => repeat(1:2; inner = 4),
        :ts => repeat(1:2; inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :val => 5:12,
      ])
      result = find_representative_periods(
        clustering_data,
        2;
        method = :k_means,
        init = :kmcen,
        layout,
      )
      result.weight_matrix == [1.0 0.0; 0.0 1.0]
    end
  end
end

@testset "K-medoids clustering" begin
  @testset "Make sure that k-medoids returns the original periods when n_rp == n_periods" begin
    @test begin
      clustering_data = DataFrame([
        :period => repeat(1:2; inner = 4),
        :timestep => repeat(1:2; inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])
      clustering_result =
        find_representative_periods(clustering_data, 2; method = :k_medoids, init = :kmcen)

      clustering_result.weight_matrix == [1.0 0.0; 0.0 1.0]
    end

    @test begin
      clustering_data = DataFrame([
        :period => repeat(1:2; inner = 4),
        :timestep => repeat(1:2; inner = 2, outer = 2),
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
        :period => repeat(1:3; inner = 4),
        :timestep => repeat(1:2; inner = 2, outer = 3),
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

  @testset "K-medoids works with custom layout" begin
    @test begin
      layout = DataFrameLayout(; period = :p, timestep = :ts, value = :val)
      clustering_data = DataFrame([
        :p => repeat(1:2; inner = 4),
        :ts => repeat(1:2; inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :val => 5:12,
      ])
      result = find_representative_periods(
        clustering_data,
        2;
        method = :k_medoids,
        init = :kmcen,
        layout,
      )
      result.weight_matrix == [1.0 0.0; 0.0 1.0]
    end
  end
end

@testset "Convex hull clustering" begin
  @testset "Make sure that convex hull clustering finds the hull" begin
    @test begin
      clustering_data = DataFrame([
        :period => repeat(1:2; inner = 4),
        :timestep => repeat(1:2; inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])
      clustering_result =
        find_representative_periods(clustering_data, 2; method = :convex_hull)
      clustering_result.weight_matrix == [1.0 0.0; 0.0 1.0]
    end
  end

  @testset "Convex hull works with custom layout" begin
    @test begin
      layout = DataFrameLayout(; period = :p, timestep = :ts, value = :val)
      clustering_data = DataFrame([
        :p => repeat(1:2; inner = 4),
        :ts => repeat(1:2; inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :val => 5:12,
      ])
      result =
        find_representative_periods(clustering_data, 2; method = :convex_hull, layout)
      result.weight_matrix == [1.0 0.0; 0.0 1.0]
    end
  end
end

@testset "Convex hull with null clustering" begin
  @testset "Make sure that the furthest point from 0 is found as first representative" begin
    @test begin
      clustering_data = DataFrame(; period = [1, 2], value = [1.0, 0.5], timestep = [1, 1])
      clustering_result = find_representative_periods(
        clustering_data,
        1;
        distance = SqEuclidean(),
        method = :convex_hull_with_null,
      )
      clustering_result.profiles[!, :value] == [1.0]
    end
  end

  @testset "Convex hull with null works with custom layout" begin
    @test begin
      layout = DataFrameLayout(; period = :p, timestep = :ts, value = :val)
      clustering_data = DataFrame(; p = [1, 2], val = [1.0, 0.5], ts = [1, 1])
      result = find_representative_periods(
        clustering_data,
        1;
        method = :convex_hull_with_null,
        layout,
      )
      result.profiles[!, :val] == [1.0]
    end
  end
end

@testset "Bad clustering method" begin
  @testset "Make sure that clustering fails when incorrect method is given" begin
    @test_throws ArgumentError begin
      clustering_data = DataFrame([
        :period => repeat(1:2; inner = 4),
        :timestep => repeat(1:2; inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])
      clustering_result =
        find_representative_periods(clustering_data, 2; method = :bad_method, init = :kmcen)
    end
  end

  @testset "Make sure that clustering fails with cosine distance" begin
    @test_throws ArgumentError begin
      clustering_data = DataFrame([
        :period => repeat(1:2; inner = 4),
        :timestep => repeat(1:2; inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])
      clustering_result = find_representative_periods(
        clustering_data,
        2;
        method = :convex_hull_with_null,
        distance = CosineDist(),
      )
    end
  end
end

@testset "Bad number of representative periods" begin
  @testset "Test that non-positive numbers of RPs throw correctly" begin
    clustering_data = DataFrame([
      :period => repeat(1:2; inner = 4),
      :timestep => repeat(1:2; inner = 2, outer = 2),
      :technology => repeat(["Solar", "Nuclear"], 4),
      :value => 5:12,
    ])
    @test_throws ArgumentError find_representative_periods(clustering_data, 0)
    @test_throws ArgumentError find_representative_periods(clustering_data, -1)
    @test_throws ArgumentError find_representative_periods(clustering_data, 3)
  end
end

@testset "Greedy convex hull" begin
  @testset "Test the case where points cannot be found" begin
    @test_throws ArgumentError TulipaClustering.greedy_convex_hull(
      [1.0 0.0; 0.0 1.0];
      n_points = 10,
      distance = Euclidean(),
      initial_indices = [1, 2],
      mean_vector = nothing,
    )
  end

  @testset "Test the case when there are more initial indices than points" begin
    @test size(
      TulipaClustering.greedy_convex_hull(
        [1.0 0.0; 0.0 1.0];
        n_points = 1,
        distance = Euclidean(),
        initial_indices = [1, 2],
        mean_vector = nothing,
      ),
    ) == (1,)
  end
end

@testset "Validating initial representatives" begin
  @testset "Custom layout: DataFrame without periods passed for initial representatives" begin
    layout = DataFrameLayout(; period = :p, timestep = :ts, value = :val)
    initial_representatives = DataFrame([:ts => 1:2, :val => 1:2])
    @test_throws DomainError(
      initial_representatives,
      "DataFrame must contain column `p`; call split_into_periods! to split it into periods.",
    ) begin
      clustering_data = DataFrame([
        :p => repeat(1:2; inner = 4),
        :ts => repeat(1:2; inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :val => 5:12,
      ])
      aux_clustering = find_auxiliary_data(clustering_data; layout)
      validate_initial_representatives(
        initial_representatives,
        clustering_data,
        aux_clustering,
        false,
        1,
        layout,
      )
    end
  end

  @testset "Custom layout: Dataframe with different key columns passed for initial representatives" begin
    @test_throws ArgumentError(
      "Key columns of initial represenatives do not match clustering data\nExpected was: [:ts, :technology] \nFound was: [:ts, :demand]",
    ) begin
      layout = DataFrameLayout(; period = :p, timestep = :ts, value = :val)
      clustering_data = DataFrame([
        :p => repeat(1:2; inner = 4),
        :ts => repeat(1:2; inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :val => 5:12,
      ])
      aux_clustering = find_auxiliary_data(clustering_data; layout)
      initial_representatives = DataFrame([
        :p => repeat(1:2; inner = 4),
        :ts => repeat(1:2; inner = 2, outer = 2),
        :demand => repeat(["Solar", "Nuclear"], 4),
        :val => 5:12,
      ])
      validate_initial_representatives(
        initial_representatives,
        clustering_data,
        aux_clustering,
        false,
        1,
        layout,
      )
    end
  end

  @testset "DataFrame without periods passed for initial representatives" begin
    initial_representatives = DataFrame([:timestep => 1:2, :value => 1:2])
    @test_throws DomainError(
      initial_representatives,
      "DataFrame must contain column `period`; call split_into_periods! to split it into periods.",
    ) begin
      clustering_data = DataFrame([
        :period => repeat(1:2; inner = 4),
        :timestep => repeat(1:2; inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])
      aux_clustering = find_auxiliary_data(clustering_data)
      validate_initial_representatives(
        initial_representatives,
        clustering_data,
        aux_clustering,
        false,
        1,
      )
    end
  end

  @testset "Dataframe with different key columns passed for initial representatives" begin
    @test_throws ArgumentError(
      "Key columns of initial represenatives do not match clustering data\nExpected was: [:timestep, :technology] \nFound was: [:timestep, :demand]",
    ) begin
      clustering_data = DataFrame([
        :period => repeat(1:2; inner = 4),
        :timestep => repeat(1:2; inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])
      aux_clustering = find_auxiliary_data(clustering_data)
      initial_representatives = DataFrame([
        :period => repeat(1:2; inner = 4),
        :timestep => repeat(1:2; inner = 2, outer = 2),
        :demand => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])
      validate_initial_representatives(
        initial_representatives,
        clustering_data,
        aux_clustering,
        false,
        1,
      )
    end
  end

  @testset "Initial representatives with incomplete period" begin
    @test_throws ArgumentError(
      "Initial representatives have an incomplete last period, which is not allowed",
    ) begin
      clustering_data = DataFrame([
        :period => repeat(1:2; inner = 6),
        :timestep => repeat(1:3; inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 6),
        :value => 5:16,
      ])
      aux_clustering = find_auxiliary_data(clustering_data)
      initial_representatives = DataFrame([
        :period => [1, 1, 1, 1],
        :timestep => [1, 1, 2, 2],
        :technology => repeat(["Solar", "Nuclear"], 2),
        :value => 5:8,
      ])
      validate_initial_representatives(
        initial_representatives,
        clustering_data,
        aux_clustering,
        false,
        1,
      )
    end
  end

  @testset "Dataframe with different keys passed for initial representatives" begin
    @test_throws ArgumentError(
      "Initial representatives and clustering data do not have the same keys\n" *
      "There are 0 extra keys in initial representatives\n" *
      "and 6 extra keys in clustering data.",
    ) begin
      clustering_data = DataFrame([
        :period => repeat(1:2; inner = 6),
        :timestep => repeat(1:3; inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 6),
        :value => 5:16,
      ])
      aux_clustering = find_auxiliary_data(clustering_data)
      initial_representatives = DataFrame([
        :period => repeat(1:2; inner = 3),
        :timestep => repeat(1:3; outer = 2),
        :technology => repeat(["Solar"], 6),
        :value => 5:10,
      ])
      validate_initial_representatives(
        initial_representatives,
        clustering_data,
        aux_clustering,
        false,
        1,
      )
    end
  end

  @testset "Initial representatives more than n_rp" begin
    @test_throws ArgumentError(
      "The number of representative periods is 1 but has to be at least 2.",
    ) begin
      clustering_data = DataFrame([
        :period => repeat(1:2; inner = 4),
        :timestep => repeat(1:2; inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])
      aux_clustering = find_auxiliary_data(clustering_data)
      initial_representatives = DataFrame([
        :period => repeat(1:2; inner = 4),
        :timestep => repeat(1:2; inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])
      validate_initial_representatives(
        initial_representatives,
        clustering_data,
        aux_clustering,
        false,
        1,
      )
    end
  end

  @testset "Initial representatives more than n_rp" begin
    @test_throws ArgumentError(
      "The number of representative periods is 2 but has to be at least 3.",
    ) begin
      clustering_data = DataFrame([
        :period => repeat(1:2; inner = 4),
        :timestep => repeat(1:2; inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])
      push!(clustering_data, [3, 1, "Solar", 6])
      push!(clustering_data, [3, 1, "Nuclear", 6])
      aux_clustering = find_auxiliary_data(clustering_data)
      initial_representatives = DataFrame([
        :period => repeat(1:2; inner = 4),
        :timestep => repeat(1:2; inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])
      validate_initial_representatives(
        initial_representatives,
        clustering_data,
        aux_clustering,
        true,
        2,
      )
    end
  end
end

@testset "K-means and k-medoids with initial representatives" begin
  @testset "K-means and complete periods" begin
    @test begin
      clustering_data = DataFrame([
        :period => repeat(1:2; inner = 4),
        :timestep => repeat(1:2; inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])

      representatives = DataFrame([
        :period => repeat([1], 4),
        :timestep => repeat(1:2; inner = 2, outer = 1),
        :technology => repeat(["Solar", "Nuclear"], 2),
        :value => 1:4,
      ])

      clustering_result = find_representative_periods(
        clustering_data,
        2;
        method = :k_means,
        initial_representatives = representatives,
      )

      clustering_result.profiles[clustering_result.profiles.rep_period .== 2, :value] ==
      [1.0, 2.0, 3.0, 4.0]
    end
  end

  @testset "K-medoids and incomplete periods" begin
    @test begin
      clustering_data = DataFrame([
        :period => repeat(1:2; inner = 4),
        :timestep => repeat(1:2; inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])

      representatives = DataFrame([
        :period => repeat([1], 4),
        :timestep => repeat(1:2; inner = 2, outer = 1),
        :technology => repeat(["Solar", "Nuclear"], 2),
        :value => 1:4,
      ])

      push!(clustering_data, [3, 1, "Solar", 6])
      push!(clustering_data, [3, 1, "Nuclear", 6])

      clustering_result = find_representative_periods(
        clustering_data,
        3;
        method = :k_medoids,
        initial_representatives = representatives,
      )

      (
        clustering_result.profiles[clustering_result.profiles.rep_period .== 2, :value] ==
        [1.0, 2.0, 3.0, 4.0]
      ) && (
        clustering_result.profiles[clustering_result.profiles.rep_period .== 3, :value] ==
        [6.0, 6.0]
      )
    end
  end

  @testset "K-means and incomplete period" begin
    @test begin
      clustering_data = DataFrame([
        :period => repeat(1:2; inner = 4),
        :timestep => repeat(1:2; inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])

      representatives = DataFrame([
        :period => repeat([1], 4),
        :timestep => repeat(1:2; inner = 2, outer = 1),
        :technology => repeat(["Solar", "Nuclear"], 2),
        :value => 1:4,
      ])

      push!(clustering_data, [3, 1, "Solar", 6])
      push!(clustering_data, [3, 1, "Nuclear", 6])

      clustering_result = find_representative_periods(
        clustering_data,
        3;
        method = :k_means,
        initial_representatives = representatives,
      )

      (
        clustering_result.profiles[clustering_result.profiles.rep_period .== 2, :value] ==
        [1.0, 2.0, 3.0, 4.0]
      ) && (
        clustering_result.profiles[clustering_result.profiles.rep_period .== 3, :value] ==
        [6.0, 6.0]
      )
    end
  end

  @testset "K-medoids and complete period" begin
    @test begin
      clustering_data = DataFrame([
        :period => repeat(1:2; inner = 4),
        :timestep => repeat(1:2; inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])

      representatives = DataFrame([
        :period => repeat([1], 4),
        :timestep => repeat(1:2; inner = 2, outer = 1),
        :technology => repeat(["Solar", "Nuclear"], 2),
        :value => 1:4,
      ])

      clustering_result = find_representative_periods(
        clustering_data,
        2;
        method = :k_medoids,
        initial_representatives = representatives,
      )

      clustering_result.profiles[clustering_result.profiles.rep_period .== 2, :value] ==
      [1.0, 2.0, 3.0, 4.0]
    end
  end

  @testset "rp_matrix also contains initial representatives and at the right place in the right order" begin
    @test begin
      clustering_data = DataFrame([
        :period => repeat(1:2; inner = 4),
        :timestep => repeat(1:2; inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])

      representatives = DataFrame([
        :period => repeat([1], 4),
        :timestep => repeat(1:2; inner = 2, outer = 1),
        :technology => repeat(["Nuclear", "Solar"], 2),
        :value => [2, 1, 4, 3],
      ])

      clustering_result = find_representative_periods(
        clustering_data,
        2;
        method = :k_medoids,
        initial_representatives = representatives,
      )

      clustering_result.rp_matrix[:, 2] == [1, 2, 3, 4]
    end
  end

  @testset "Initial representatives already all representatives" begin
    @test begin
      clustering_data = DataFrame([
        :period => repeat(1:2; inner = 4),
        :timestep => repeat(1:2; inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])

      representatives = DataFrame([
        :period => repeat([1], 4),
        :timestep => repeat(1:2; inner = 2, outer = 1),
        :technology => repeat(["Solar", "Nuclear"], 2),
        :value => 1:4,
      ])

      clustering_result = find_representative_periods(
        clustering_data,
        1;
        method = :k_medoids,
        initial_representatives = representatives,
      )

      clustering_result.profiles[clustering_result.profiles.rep_period .== 1, :value] ==
      [1.0, 2.0, 3.0, 4.0]
    end
  end
end

@testset "Hulls with initial representatives" begin
  @testset "Initial representatives already all representatives convex hull" begin
    @test begin
      clustering_data = DataFrame([
        :period => repeat(1:2; inner = 4),
        :timestep => repeat(1:2; inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])

      representatives = DataFrame([
        :period => repeat(1:2; inner = 4),
        :timestep => repeat(1:2; inner = 2, outer = 2),
        :technology => repeat(["Nuclear", "Solar"], 4),
        :value => [2, 1, 4, 3, 14, 13, 16, 15],
      ])

      clustering_result = find_representative_periods(
        clustering_data,
        2;
        method = :convex_hull,
        initial_representatives = representatives,
      )

      (
        clustering_result.profiles[clustering_result.profiles.rep_period .== 1, :value] ==
        [2.0, 1.0, 4.0, 3.0]
      ) && (
        clustering_result.profiles[clustering_result.profiles.rep_period .== 2, :value] ==
        [14.0, 13.0, 16.0, 15.0]
      )
    end
  end

  @testset "Initial representatives already all representatives convex hull with null" begin
    @test begin
      clustering_data = DataFrame([
        :period => repeat(1:2; inner = 4),
        :timestep => repeat(1:2; inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])

      representatives = DataFrame([
        :period => repeat(1:2; inner = 4),
        :timestep => repeat(1:2; inner = 2, outer = 2),
        :technology => repeat(["Nuclear", "Solar"], 4),
        :value => [2, 1, 4, 3, 14, 13, 16, 15],
      ])

      clustering_result = find_representative_periods(
        clustering_data,
        2;
        method = :convex_hull_with_null,
        initial_representatives = representatives,
      )

      (
        clustering_result.profiles[clustering_result.profiles.rep_period .== 1, :value] ==
        [2.0, 1.0, 4.0, 3.0]
      ) && (
        clustering_result.profiles[clustering_result.profiles.rep_period .== 2, :value] ==
        [14.0, 13.0, 16.0, 15.0]
      )
    end
  end

  @testset "Initial representatives already all representatives conical hull" begin
    @test begin
      clustering_data = DataFrame([
        :period => repeat(1:2; inner = 4),
        :timestep => repeat(1:2; inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])

      representatives = DataFrame([
        :period => repeat(1:2; inner = 4),
        :timestep => repeat(1:2; inner = 2, outer = 2),
        :technology => repeat(["Nuclear", "Solar"], 4),
        :value => [2, 1, 4, 3, 14, 13, 16, 15],
      ])

      clustering_result = find_representative_periods(
        clustering_data,
        2;
        method = :conical_hull,
        initial_representatives = representatives,
      )

      (
        clustering_result.profiles[clustering_result.profiles.rep_period .== 1, :value] ==
        [2.0, 1.0, 4.0, 3.0]
      ) && (
        clustering_result.profiles[clustering_result.profiles.rep_period .== 2, :value] ==
        [14.0, 13.0, 16.0, 15.0]
      )
    end
  end

  @testset "Convex hull with one initial representative picks correct initial and second representative" begin
    @test begin
      clustering_data = DataFrame([
        :period => repeat(1:3; inner = 4),
        :timestep => repeat(1:2; inner = 2, outer = 3),
        :technology => repeat(["Solar", "Nuclear"], 6),
        :value => repeat(1:2:12; inner = 2),
      ])

      representatives = DataFrame([
        :period => repeat([1]; inner = 4),
        :timestep => repeat(1:2; inner = 2),
        :technology => repeat(["Nuclear", "Solar"], 2),
        :value => [11, 11, 13, 13],
      ])

      clustering_result = find_representative_periods(
        clustering_data,
        2;
        method = :convex_hull,
        initial_representatives = representatives,
      )

      (
          clustering_result.profiles[clustering_result.profiles.rep_period .== 1, :value] ==
          [11.0, 11.0, 13.0, 13.0]
        ) &&
        (
          clustering_result.profiles[clustering_result.profiles.rep_period .== 2, :value] ==
          [1.0, 1.0, 3.0, 3.0]
        ) &&
        (clustering_result.weight_matrix == [0.0 1.0; 0.0 1.0; 1.0 0.0])
    end
  end

  @testset "Convex hull with null with one initial representative picks correct initial and second representative" begin
    @test begin
      clustering_data = DataFrame([
        :period => repeat(1:3; inner = 4),
        :timestep => repeat(1:2; inner = 2, outer = 3),
        :technology => repeat(["Solar", "Nuclear"], 6),
        :value => repeat(1:2:12; inner = 2),
      ])

      representatives = DataFrame([
        :period => repeat([1]; inner = 4),
        :timestep => repeat(1:2; inner = 2),
        :technology => repeat(["Nuclear", "Solar"], 2),
        :value => [11, 11, 13, 13],
      ])

      clustering_result = find_representative_periods(
        clustering_data,
        2;
        method = :convex_hull_with_null,
        initial_representatives = representatives,
      )

      clustering_result.profiles[clustering_result.profiles.rep_period .== 1, :value] ==
      [11.0, 11.0, 13.0, 13.0]
    end
  end

  @testset "Conical hull with one initial representative picks correct initial and second representative" begin
    @test begin
      clustering_data = DataFrame([
        :period => repeat(1:3; inner = 4),
        :timestep => repeat(1:2; inner = 2, outer = 3),
        :technology => repeat(["Solar", "Nuclear"], 6),
        :value => repeat(1:2:12; inner = 2),
      ])

      representatives = DataFrame([
        :period => repeat([1]; inner = 4),
        :timestep => repeat(1:2; inner = 2),
        :technology => repeat(["Nuclear", "Solar"], 2),
        :value => [11, 11, 13, 13],
      ])

      clustering_result = find_representative_periods(
        clustering_data,
        2;
        method = :convex_hull_with_null,
        initial_representatives = representatives,
      )

      clustering_result.profiles[clustering_result.profiles.rep_period .== 1, :value] ==
      [11.0, 11.0, 13.0, 13.0]
    end
  end

  @testset "Convex hull does not contain intial representative in clustering matrix" begin
    @test begin
      clustering_data = DataFrame([
        :period => repeat(1:3; inner = 4),
        :timestep => repeat(1:2; inner = 2, outer = 3),
        :technology => repeat(["Solar", "Nuclear"], 6),
        :value => repeat(1:2:12; inner = 2),
      ])

      representatives = DataFrame([
        :period => repeat([1]; inner = 4),
        :timestep => repeat(1:2; inner = 2),
        :technology => repeat(["Nuclear", "Solar"], 2),
        :value => [11, 11, 13, 13],
      ])

      clustering_result = find_representative_periods(
        clustering_data,
        2;
        method = :convex_hull,
        initial_representatives = representatives,
      )

      !any(==(13), clustering_result.clustering_matrix)
    end
  end
end
@testset "Weights are correctly calculated with representatives" begin
  @testset "Weights are equal for each method when representatives don't need to be found" begin
    @test begin
      clustering_data = DataFrame([
        :period => repeat(1:2; inner = 4),
        :timestep => repeat(1:2; inner = 2, outer = 2),
        :technology => repeat(["Solar", "Nuclear"], 4),
        :value => 5:12,
      ])

      representatives = DataFrame([
        :period => repeat(1:2; inner = 4),
        :timestep => repeat(1:2; inner = 2, outer = 2),
        :technology => repeat(["Nuclear", "Solar"], 4),
        :value => [2, 1, 4, 3, 14, 13, 16, 15],
      ])

      clustering_result = find_representative_periods(
        clustering_data,
        2;
        method = :convex_hull,
        initial_representatives = representatives,
        distance = CosineDist(),
      )

      clustering_result_2 = find_representative_periods(
        clustering_data,
        2;
        method = :k_medoids,
        initial_representatives = representatives,
        distance = CosineDist(),
      )

      weight_matrix_1 = clustering_result.weight_matrix
      weight_matrix_2 = clustering_result_2.weight_matrix
      weight_matrix_1 == weight_matrix_2
    end
  end
end
