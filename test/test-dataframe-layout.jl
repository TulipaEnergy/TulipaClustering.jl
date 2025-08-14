@testset "Basic usage" begin
  layout = DataFrameLayout()
  @test layout.value == :value
  @test layout.timestep == :timestep
  @test layout.period == :period
end

@testset "Override defaults via kwargs" begin
  layout = DataFrameLayout(; value = :val, timestep = :ts, period = :per)
  @test layout.value == :val
  @test layout.timestep == :ts
  @test layout.period == :per
end

@testset "Read from file" begin
  path = joinpath(@__DIR__, "inputs", "dataframe-layout-example.toml")
  layout = DataFrameLayout(path)
  data = TOML.parsefile(path)
  for (key, value) in data
    @test Symbol(value) == getfield(layout, Symbol(key))
  end

  @testset "explicit keywords take precedence" begin
    layout2 = DataFrameLayout(path; value = :override_value)
    @test layout2.value == :override_value
    # others from file preserved
    @test layout2.timestep == :time_index
    @test layout2.period == :scenario_period
  end

  @testset "Errors if path does not exist" begin
    @test_throws ArgumentError DataFrameLayout("nonexistent.toml")
  end
end
