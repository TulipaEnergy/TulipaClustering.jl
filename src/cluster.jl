export find_representative_periods, reshape_clustering_data!

"""
    combine_periods!(df)

Modifies a dataframe `df` by combining the columns `time_step` and `period`
into a single column `time_step` of global time steps. The period duration is
inferred automatically from the maximum time step value, assuming that
periods start with time step 1.

# Examples
```jldoctest
julia> df = DataFrame([:period => [1, 1, 2], :time_step => [1, 2, 1], :value => 1:3])
3×3 DataFrame
 Row │ period  time_step  value
     │ Int64   Int64      Int64
─────┼──────────────────────────
   1 │      1          1      1
   2 │      1          2      2
   3 │      2          1      3

julia> combine_periods!(df)
3×2 DataFrame
 Row │ time_step  value
     │ Int64      Int64
─────┼──────────────────
   1 │         1      1
   2 │         2      2
   3 │         3      3
```
"""
function combine_periods!(df::AbstractDataFrame)
  # First check that df contains a time_step column
  if columnindex(df, :time_step) == 0
    throw(DomainError(df, "DataFrame does not contain a column `time_step`"))
  end
  if columnindex(df, :period) == 0
    return  # if there is no column df.period, leave df as is
  end
  max_t = maximum(df.time_step)
  @. df.time_step = (df.period - 1) * max_t + df.time_step
  select!(df, Not(:period))
end

"""
    split_into_periods!(df; period_duration=nothing)

Modifies a dataframe `df` by separating the column `time_step` into periods of
length `period_duration`. The new data is written into two columns:

  - `period`: the period ID;
  - `time_step`: the time step within the current period.

If `period_duration` is `nothing`, then all of the time steps are within the
same period with index 1.

# Examples
```jldoctest
julia> df = DataFrame([:time_step => 1:4, :value => 5:8])
4×2 DataFrame
 Row │ time_step  value
     │ Int64      Int64
─────┼──────────────────
   1 │         1      5
   2 │         2      6
   3 │         3      7
   4 │         4      8

julia> split_into_periods!(df; period_duration=2)
4×3 DataFrame
 Row │ period  time_step  value
     │ Int64   Int64      Int64
─────┼──────────────────────────
   1 │      1          1      5
   2 │      1          2      6
   3 │      2          1      7
   4 │      2          2      8

julia> df = DataFrame([:period => [1, 1, 2], :time_step => [1, 2, 1], :value => 1:3])
3×3 DataFrame
 Row │ period  time_step  value
     │ Int64   Int64      Int64
─────┼──────────────────────────
   1 │      1          1      1
   2 │      1          2      2
   3 │      2          1      3

julia> split_into_periods!(df; period_duration=1)
3×3 DataFrame
 Row │ period  time_step  value
     │ Int64   Int64      Int64
─────┼──────────────────────────
   1 │      1          1      1
   2 │      2          1      2
   3 │      3          1      3

julia> split_into_periods!(df)
3×3 DataFrame
 Row │ period  time_step  value
     │ Int64   Int64      Int64
─────┼──────────────────────────
   1 │      1          1      1
   2 │      1          2      2
   3 │      1          3      3
```
"""
function split_into_periods!(df::AbstractDataFrame; period_duration::Union{Int, Nothing} = nothing)
  # If the periods already exist, combine them into the time steps if necessary
  combine_periods!(df)

  if isnothing(period_duration)
    # If period_duration is nothing, then leave the time steps as is and
    # everything is just the same period with index 1.
    df.period .= 1
  else
    # Otherwise, split the time step index using 1-based modular arithmetic
    indices = fldmod1.(df.time_step, period_duration)  # find the new indices
    indices = reinterpret(reshape, Int, indices)       # change to an array for slicing

    df.period = indices[1, :]     # first row is the floor quotients, i.e., the period indices
    df.time_step = indices[2, :]  # second row is the remainders, i.e., the new time steps
  end
  select!(df, :period, :time_step, :)  # move the time-related columns to the front
end

"""
    reshape_time_series!(df)

"""
function reshape_time_series!(
  time_series::TimeSeriesData;
  period_duration::Union{Int, Nothing} = nothing,
)

  # Check that all the required columns are present in the data frame
  df = time_series.data
  if columnindex(df, :time_step) == 0
    throw(DomainError(df, "DataFrame does not contain a column `time_step`"))
  end
  if columnindex(df, :value) == 0
    if isnothing(time_series.key_columns)
      throw(
        DomainError(
          time_series,
          "Time series data does not contain a column `value`, and the values cannot be inferred because `key_columns` is nothing",
        ),
      )
    end
    df = stack(df, Not(time_series.key_columns), variable_name = :period)
    df[!, :period] = parse.(Int, df[!, :period])
    dropmissing!(df, :value)
  end

  # Reinterptet the periods and time_steps using the new `period_duration`
  # and compute the associated statistics on the time steps and periods
  if isnothing(time_series.total_time_steps)
    # If the total number of time steps is not known, find it.
    # Otherwise, this step can be skipped.
    combine_periods!(df)
    time_series.total_time_steps = maximum(df.time_step)
  end
  split_into_periods!(df; period_duration)
  time_series.period_duration =
    isnothing(period_duration) ? time_series.total_time_steps : period_duration
  time_series.n_periods = maximum(df.period)

  # Find all columns that act as row keys, that is, all columns except for
  # df.period (which is used as the column index) and df.value (which contains
  # the actual data).
  time_series.key_columns = filter!(e -> e ∉ ["period", "value"], names(df)) .|> Symbol

  # Finally, convert the data from long to wide format, because this is how
  # clustering data needs to be shaped
  time_series.data = unstack(df, time_series.key_columns, :period, :value)

  return
end

"""
    reshape_clustering_data!(clustering_data; period_duration)

Modifies a [`TulipaClustering.ClusteringData`](@ref) structure by separating
time steps into periods of length `period_duration` in the dataframes
`clustering_data.demand` and `clustering_data.generation_availability`.
"""
function reshape_clustering_data!(
  clustering_data::ClusteringData;
  period_duration::Union{Int, Nothing} = nothing,
)
  # Split the data frames inside the clustering data into periods
  reshape_time_series!(clustering_data.demand; period_duration)
  reshape_time_series!(clustering_data.generation_availability; period_duration)
end

function _matrix_to_df(prefix_columns::AbstractDataFrame, centroids::AbstractDataFrame)
  result = hcat(prefix_columns, centroids)            # prepend the previously deleted columns
  result = stack(result, variable_name = :rep_period) |> dropmissing # convert from wide to long format
  result.rep_period = parse.(Int, result.rep_period)  # change the type of rep_period column to Int
  select!(result, :rep_period, :time_step, :)         # move the rep_period column to the front
  return result
end

function get_df_without_key_columns(time_series::TimeSeriesData)::DataFrame
  return time_series.data[!, Not(time_series.key_columns)]
end

function find_incomplete_columns(df::AbstractDataFrame)::Vector{String}
  return filter(c -> any(ismissing, df[:, c]), names(df))
end

function find_representative_periods(
  clustering_data::ClusteringData;
  n_rp::Int = 10,
  rescale_demand_data::Bool = true,
  discard_incomplete_periods::Bool = true,
  method::Symbol = :k_means,
  args...,
)
  # Check that the time series use the same periods and time steps
  n_periods = clustering_data.demand.n_periods
  period_duration = clustering_data.demand.period_duration
  total_time_steps = clustering_data.demand.total_time_steps
  if clustering_data.generation_availability.n_periods != n_periods ||
     clustering_data.generation_availability.period_duration != period_duration ||
     clustering_data.generation_availability.total_time_steps != total_time_steps
    throw(ArgumentError("Time series for demand and generation must have the same time steps"))
  end

  # Check if there are any incomplete periods that are not the last period
  demand_df = get_df_without_key_columns(clustering_data.demand)
  generation_df = get_df_without_key_columns(clustering_data.generation_availability)

  incomplete_periods = find_incomplete_columns(demand_df) ∪ find_incomplete_columns(generation_df)

  n_incomplete_periods = length(incomplete_periods)
  if n_incomplete_periods > 1
    throw(DomainError(incomplete_periods, "Multiple periods have missing data"))
  elseif n_incomplete_periods == 1
    incomplete_period = parse(Int, incomplete_periods[1])
    if incomplete_period != n_periods
      throw(DomainError(incomplete_period, "A period has missing data, but it is not the last one"))
    end
  end

  # Compute the clustering matrix by concatenating the data matrices
  demand_matrix = select(demand_df, Not(incomplete_periods)) |> dropmissing |> Matrix{Float64}

  if rescale_demand_data
    # Generation availability is on a scale from 0 to 1, but demand is not;
    # rescale the demand profiles by dividing them by the largest possible value,
    # and remember this value so that the demands can be computed back from the
    # normalized values later on.
    demand_scaling_factor = maximum(demand_matrix[map(!ismissing, demand_matrix)])
    demand_matrix ./= demand_scaling_factor
  end

  generation_matrix =
    select(generation_df, Not(incomplete_periods)) |> dropmissing |> Matrix{Float64}
  clustering_matrix = vcat(demand_matrix, generation_matrix)

  needs_discarding = n_incomplete_periods == 1 && !discard_incomplete_periods
  if needs_discarding
    n_rp -= 1
  end

  # Do the clustering
  if method ≡ :k_means
    kmeans_result = kmeans(clustering_matrix, n_rp; args...)

    # Reinterpret the results
    centroids = kmeans_result.centers
    centroids = DataFrame(centroids, string.(1:n_rp))
    if needs_discarding
      n_rp += 1
    end
    n_demand = size(demand_matrix)[1]

    demand_columns = select(clustering_data.demand.data, clustering_data.demand.key_columns)
    demand_centroids = centroids[1:n_demand, :]
    if rescale_demand_data
      demand_centroids .*= demand_scaling_factor
    end
    if needs_discarding
      demand_centroids =
        hcat(demand_centroids, select(demand_df, incomplete_periods[1] => string(n_rp)))
    end
    demand = _matrix_to_df(demand_columns, demand_centroids)

    generation_columns = select(
      clustering_data.generation_availability.data,
      clustering_data.generation_availability.key_columns,
    )
    generation_centroids = centroids[(n_demand + 1):end, :]
    if needs_discarding
      generation_centroids =
        hcat(generation_centroids, select(generation_df, incomplete_periods[1] => string(n_rp)))
    end
    generation = _matrix_to_df(generation_columns, generation_centroids)

    if needs_discarding
      w = 1.0
      weights =
        [
          t < n_periods && kmeans_result.assignments[t] == r ? w : 0.0 for t = 1:n_periods,
          r = 1:n_rp
        ] |> Matrix{Float64}
      # TODO: add a zero column and row to weights
      weights[n_periods, n_rp] =
        (total_time_steps - (n_periods - 1) * period_duration) / period_duration
    else
      w = total_time_steps / ((n_periods - 1) * period_duration)
      weights =
        [kmeans_result.assignments[t] == r ? w : 0.0 for t = 1:(n_periods - 1), r = 1:n_rp] |> Matrix{Float64}
    end

    return ClusteringResult(demand, generation, weights)
  elseif method ≡ :k_medoids
  elseif method ≡ :hull
  else
  end
end
