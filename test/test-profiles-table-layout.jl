@testset "Basic usage" begin
  layout = ProfilesTableLayout()
  @test layout.value == :value
  @test layout.timestep == :timestep
  @test layout.period == :period
  @test layout.profile_name == :profile_name
  @test layout.year == :year
  @test layout.default_year == 2000
  @test layout.scenario == :scenario
  @test layout.default_scenario == 1
  @test layout.cols_to_groupby == [:year]
end

@testset "Override defaults via kwargs" begin
  layout = ProfilesTableLayout(;
    value = :val,
    timestep = :ts,
    period = :per,
    profile_name = :name,
    year = :yr,
    default_year = 2025,
    scenario = :scen,
    default_scenario = 5,
    cols_to_groupby = [:year, :scenario],
  )
  @test layout.value == :val
  @test layout.timestep == :ts
  @test layout.period == :per
  @test layout.profile_name == :name
  @test layout.year == :yr
  @test layout.default_year == 2025
  @test layout.scenario == :scen
  @test layout.default_scenario == 5
  @test layout.cols_to_groupby == [:year, :scenario]
end

@testset "Read from file" begin
  path = joinpath(@__DIR__, "inputs", "dataframe-layout-example.toml")
  layout = ProfilesTableLayout(path)
  data = TOML.parsefile(path)
  for (key, value) in data
    @test Symbol(value) == getfield(layout, Symbol(key))
  end

  @testset "explicit keywords take precedence" begin
    layout2 = ProfilesTableLayout(path; value = :override_value)
    @test layout2.value == :override_value
    # others from file preserved
    @test layout2.timestep == :time_index
    @test layout2.period == :scenario_period
  end

  @testset "Errors for bad paths" begin
    # empty string
    @test_throws ArgumentError ProfilesTableLayout("")
    # invalid path
    @test_throws ArgumentError ProfilesTableLayout("nonexistent.toml")
  end
end
