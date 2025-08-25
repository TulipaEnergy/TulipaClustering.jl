export find_representative_periods,
  split_into_periods!, find_auxiliary_data, validate_initial_representatives

"""
    combine_periods!(df; layout = ProfilesTableLayout())

Combine per-period time steps into a single global `timestep` column in-place.

Given a long-format dataframe `df` with (at least) a per-period `timestep`
column and, optionally, a `period` column (names provided by `layout`), this
function rewrites the `timestep` column so that time becomes a single global,
monotonically increasing index across all periods, then removes the original
`period` column.

Period length inference:
  * The (nominal) period duration `L` is inferred as the maximum value found in
    the per-period time-step column across the whole dataframe (NOT per period).
  * Each row's global timestep is computed as `(period - 1) * L + timestep`.
  * If the final period is shorter than `L`, the resulting global time index will
    simply end earlier; missing intermediate global timesteps are not created.

Arguments:
  * `df::AbstractDataFrame` (mutated): Source data in long format.
  * `layout::ProfilesTableLayout`: Describes the column names for `period` and
    `timestep` (defaults to standard names). Pass a custom layout if your
    dataframe uses different symbols.

Behavior & edge cases:
  * If the `timestep` column (as specified by `layout`) is missing, a `DomainError` is thrown.
  * If the `period` column is absent, the function is a no-op (returns immediately).
  * Non-1-based or non-consecutive per-period timesteps are not validated; unusual
    values may result in non-contiguous or non-strictly increasing global indices.
  * Works in-place; the modified dataframe (without `period`) is also returned for convenience.

## Examples

Basic usage with default layout:
```
julia> df = DataFrame([:period => [1, 1, 2], :timestep => [1, 2, 1], :value => 1:3])
3×3 DataFrame
 Row │ period  timestep  value
     │ Int64   Int64      Int64
─────┼──────────────────────────
   1 │      1          1      1
   2 │      1          2      2
   3 │      2          1      3

julia> TulipaClustering.combine_periods!(df)
3×2 DataFrame
 Row │ timestep  value
     │ Int64      Int64
─────┼──────────────────
   1 │         1      1
   2 │         2      2
   3 │         3      3
```

Custom column names via a layout:
```
julia> layout = ProfilesTableLayout(; period = :p, timestep = :ts)
julia> df = DataFrame([:p => [1,1,2], :ts => [1,2,1], :value => 10:12])
3×3 DataFrame
 Row │ p      ts   value
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1     1     10
   2 │     1     2     11
   3 │     2     1     12

julia> TulipaClustering.combine_periods!(df; layout)
3×2 DataFrame
 Row │ ts    value
     │ Int64  Int64
─────┼──────────────
   1 │    1     10
   2 │    2     11
   3 │    3     12
```

No `period` column (no-op):
```
julia> df = DataFrame([:timestep => 1:3, :value => 4:6])
julia> TulipaClustering.combine_periods!(df)
3×2 DataFrame
 Row │ timestep  value
     │ Int64      Int64
─────┼──────────────────
   1 │         1      4
   2 │         2      5
   3 │         3      6
```
"""
function combine_periods!(df::AbstractDataFrame; layout = ProfilesTableLayout())
  # Unpack layout
  timestep_col = layout.timestep
  period_col = layout.period

  # First check that df contains a timestep column
  if columnindex(df, timestep_col) == 0
    throw(DomainError(df, "DataFrame does not contain a column $timestep_col"))
  end
  if columnindex(df, period_col) == 0
    return  # if there is no period_col column in the df, leave df as is
  end
  max_t = maximum(df[!, timestep_col])
  df[!, timestep_col] .= (df[!, period_col] .- 1) .* max_t .+ df[!, timestep_col]
  select!(df, Not(period_col))
end

"""
    split_into_periods!(df; period_duration=nothing, layout=ProfilesTableLayout())

Modifies a dataframe `df` by separating the time column into periods of length
`period_duration`, respecting custom column names provided by `layout`.

The new data is written into two columns defined by the layout:
  - `layout.period`: the period ID
  - `layout.timestep`: the time step within the current period

If `period_duration` is `nothing`, then all time steps are in a single period (ID 1).

# Examples

```
julia> df = DataFrame([:timestep => 1:4, :value => 5:8])
4×2 DataFrame
 Row │ timestep  value
     │ Int64      Int64
─────┼──────────────────
   1 │         1      5
   2 │         2      6
   3 │         3      7
   4 │         4      8

julia> TulipaClustering.split_into_periods!(df; period_duration=2)
4×3 DataFrame
 Row │ period  timestep  value
     │ Int64   Int64      Int64
─────┼──────────────────────────
   1 │      1          1      5
   2 │      1          2      6
   3 │      2          1      7
   4 │      2          2      8

julia> df = DataFrame([:period => [1, 1, 2], :timestep => [1, 2, 1], :value => 1:3])
3×3 DataFrame
 Row │ period  timestep  value
     │ Int64   Int64      Int64
─────┼──────────────────────────
   1 │      1          1      1
   2 │      1          2      2
   3 │      2          1      3

julia> TulipaClustering.split_into_periods!(df; period_duration=1)
3×3 DataFrame
 Row │ period  timestep  value
     │ Int64   Int64      Int64
─────┼──────────────────────────
   1 │      1          1      1
   2 │      2          1      2
   3 │      3          1      3

julia> TulipaClustering.split_into_periods!(df)
3×3 DataFrame
 Row │ period  timestep  value
     │ Int64   Int64      Int64
─────┼──────────────────────────
   1 │      1          1      1
   2 │      1          2      2
   3 │      1          3      3
```

Custom column names via a layout:
```
julia> layout = ProfilesTableLayout(; timestep = :time_step, period = :periods)
julia> df = DataFrame([:time_step => 1:4, :value => 5:8])
4×2 DataFrame
 Row │ time_step  value
    │ Int64      Int64
─────┼──────────────────
  1 │         1      5
  2 │         2      6
  3 │         3      7
  4 │         4      8

julia> TulipaClustering.split_into_periods!(df; period_duration=2, layout)
4×3 DataFrame
 Row │ periods  time_step  value
    │ Int64    Int64      Int64
─────┼───────────────────────────
  1 │       1          1      5
  2 │       1          2      6
  3 │       2          1      7
  4 │       2          2      8
```
"""
function split_into_periods!(
  df::AbstractDataFrame;
  period_duration::Union{Int, Nothing} = nothing,
  layout = ProfilesTableLayout(),
)
  # Unpack layout
  timestep_col = layout.timestep
  period_col = layout.period

  # If the periods already exist, combine them into the time steps if necessary
  combine_periods!(df; layout)

  if isnothing(period_duration)
    # If period_duration is nothing, then leave the time steps as is and
    # everything is just the same period with index 1.
    insertcols!(df, period_col => 1)
  else
    # Otherwise, split the time step index using 1-based modular arithmetic
    indices = fldmod1.(df[!, timestep_col], period_duration)  # find the new indices
    indices = reinterpret(reshape, Int, indices)              # change to an array for slicing

    # first row is the floor quotients, i.e., the period indices
    df[!, period_col] = indices[1, :]
    # second row is the remainders, i.e., the new time steps
    df[!, timestep_col] = indices[2, :]
  end
  # move the time-related columns to the front
  select!(df, period_col, timestep_col, :)
end

"""
  validate_df_and_find_key_columns(df; layout = ProfilesTableLayout())

Checks that dataframe `df` contains the necessary columns (as described by
`layout`) and returns a list of columns that act as keys (i.e., unique data
identifiers within different periods). Keys are all columns except
`layout.period` and `layout.value`.

# Examples

Default column names:
```
julia> df = DataFrame([:period => [1, 1, 2], :timestep => [1, 2, 1], :a .=> "a", :value => 1:3])
3×4 DataFrame
 Row │ period  timestep  a       value
     │ Int64   Int64      String  Int64
─────┼──────────────────────────────────
   1 │      1          1  a           1
   2 │      1          2  a           2
   3 │      2          1  a           3

julia> TulipaClustering.validate_df_and_find_key_columns(df)
2-element Vector{Symbol}:
 :timestep
 :a
```

Custom column names via a layout:
```
julia> layout = ProfilesTableLayout(; period = :p, timestep = :ts, value = :val)
julia> df = DataFrame(p = [1, 1, 2], ts = [1, 2, 1], a = "a", val = 1:3)
3×4 DataFrame
 Row │ p      ts   a       val
     │ Int64  Int64  String  Int64
─────┼─────────────────────────────
   1 │     1     1  a           1
   2 │     1     2  a           2
   3 │     2     1  a           3

julia> TulipaClustering.validate_df_and_find_key_columns(df; layout)
2-element Vector{Symbol}:
 :ts
 :a
```

Missing columns error references layout-provided names:
```
julia> df = DataFrame([:value => 1])
julia> TulipaClustering.validate_df_and_find_key_columns(df)
ERROR: DomainError: DataFrame must contain columns `timestep` and `value`
```
"""
function validate_df_and_find_key_columns(
  df::AbstractDataFrame;
  layout::ProfilesTableLayout = ProfilesTableLayout(),
)::Vector{Symbol}
  columns = propertynames(df)
  if layout.timestep ∉ columns || layout.value ∉ columns
    throw(
      DomainError(
        df,
        "DataFrame must contain columns `$(layout.timestep)` and `$(layout.value)`",
      ),
    )
  end
  if layout.period ∉ columns
    throw(
      DomainError(
        df,
        "DataFrame must contain column `$(layout.period)`; call split_into_periods! to split it into periods.",
      ),
    )
  end
  non_key_columns = [layout.period, layout.value]
  key_columns = filter!(col -> col ∉ non_key_columns, columns)
  return key_columns
end

"""
    find_auxiliary_data(clustering_data; layout = ProfilesTableLayout())

Calculates auxiliary data associated with `clustering_data`, considering custom
column names via `layout`.

Returns `AuxiliaryClusteringData` with:
  - `key_columns`: key columns in the dataframe
  - `period_duration`: nominal duration of periods (max timestep across data)
  - `last_period_duration`: duration of the last period
  - `n_periods`: total number of periods

# Example

```
julia> df = DataFrame([:period => [1,1,2,2], :timestep => [1,2,1,2], :a => "x", :value => 10:13])
julia> aux = TulipaClustering.find_auxiliary_data(df)
AuxiliaryClusteringData([:timestep, :a], 2, 2, 2, nothing)

julia> layout = ProfilesTableLayout(; period=:p, timestep=:ts, value=:val)
julia> df2 = DataFrame([:p => [1,1,2,2], :ts => [1,2,1,1], :a => "x", :val => 10:13])
julia> TulipaClustering.find_auxiliary_data(df2; layout)
AuxiliaryClusteringData([:ts, :a], 2, 1, 2, nothing)
```
"""
function find_auxiliary_data(
  clustering_data::AbstractDataFrame;
  layout::ProfilesTableLayout = ProfilesTableLayout(),
)
  key_columns = validate_df_and_find_key_columns(clustering_data; layout)
  period_col = layout.period
  timestep_col = layout.timestep
  n_periods = maximum(clustering_data[!, period_col])
  period_duration = maximum(clustering_data[!, timestep_col])
  last_period_duration =
    maximum(clustering_data[clustering_data[!, period_col] .== n_periods, timestep_col])
  medoids = nothing

  return AuxiliaryClusteringData(
    key_columns,
    period_duration,
    last_period_duration,
    n_periods,
    medoids,
  )
end

"""
    find_period_weights(period_duration, last_period_duration, n_periods, drop_incomplete_periods)

Finds weights of two different types of periods in the clustering data:

  - complete periods: these are all of the periods with length equal to `period_duration`.
  - incomplete last period: if last period duration is less than `period_duration`, it is incomplete.
"""
function find_period_weights(
  period_duration::Int,
  last_period_duration::Int,
  n_periods::Int,
  drop_incomplete_periods::Bool,
)::Tuple{Float64, Union{Float64, Nothing}}
  if last_period_duration == period_duration
    complete_period_weight = 1.0
    incomplete_period_weight = nothing
  elseif drop_incomplete_periods
    full_period_timesteps = period_duration * (n_periods - 1)
    total_timesteps = full_period_timesteps + last_period_duration
    complete_period_weight = total_timesteps / full_period_timesteps
    incomplete_period_weight = nothing
  else
    complete_period_weight = 1.0
    incomplete_period_weight = 1.0
  end
  return complete_period_weight, incomplete_period_weight
end

"""
  df_to_matrix_and_keys(df, key_columns; layout = ProfilesTableLayout())

Converts a long-format dataframe `df` to a matrix, using the value/period
columns from `layout`. Columns listed in `key_columns` are kept as keys.

Returns `(matrix::Matrix{Float64}, keys::DataFrame)`.

# Examples

Default layout:
```
julia> df = DataFrame([:period => [1, 1, 2, 2], :timestep => [1, 2, 1, 2], :a .=> "a", :value => 1:4])
4×4 DataFrame
 Row │ period  timestep  a       value
     │ Int64   Int64      String  Int64
─────┼──────────────────────────────────
   1 │      1          1  a           1
   2 │      1          2  a           2
   3 │      2          1  a           3
   4 │      2          2  a           4

julia> m, k = TulipaClustering.df_to_matrix_and_keys(df, [:timestep, :a]); m
2×2 Matrix{Float64}:
 1.0  3.0
 2.0  4.0

julia> k
2×2 DataFrame
 Row │ timestep  a
     │ Int64      String
─────┼───────────────────
   1 │         1  a
   2 │         2  a
```

Custom layout:
```
julia> layout = ProfilesTableLayout(; period=:p, timestep=:ts, value=:val)
julia> df = DataFrame([:p => [1,1,2,2], :ts => [1,2,1,2], :a .=> "a", :val => 1:4])
julia> m, k = TulipaClustering.df_to_matrix_and_keys(df, [:ts, :a]; layout); m
2×2 Matrix{Float64}:
 1.0  3.0
 2.0  4.0

julia> k
2×2 DataFrame
 Row │ ts    a
     │ Int64  String
─────┼────────────────
   1 │    1  a
   2 │    2  a
```
"""
function df_to_matrix_and_keys(
  df::AbstractDataFrame,
  key_columns::Vector{Symbol};
  layout::ProfilesTableLayout = ProfilesTableLayout(),
)
  wide_df = unstack(df, key_columns, layout.period, layout.value)
  matrix = select(wide_df, Not(key_columns)) |> dropmissing |> Matrix{Float64}
  keys = select(wide_df, key_columns)
  return matrix, keys
end

"""
    matrix_and_keys_to_df(matrix, keys; layout = ProfilesTableLayout())

Converts a matrix `matrix` to a long-format dataframe with columns
`(:rep_period, layout.timestep, keys..., layout.value)`.

# Examples

Default layout:
```
julia> m = [1.0 3.0; 2.0 4.0]
2×2 Matrix{Float64}:
 1.0  3.0
 2.0  4.0

julia> k = DataFrame([:timestep => 1:2, :a .=> "a"])
2×2 DataFrame
 Row │ timestep  a
     │ Int64      String
─────┼───────────────────
   1 │         1  a
   2 │         2  a

julia> TulipaClustering.matrix_and_keys_to_df(m, k)
4×4 DataFrame
 Row │ rep_period  timestep  a       value
     │ Int64       Int64      String  Float64
─────┼────────────────────────────────────────
   1 │          1          1  a           1.0
   2 │          1          2  a           2.0
   3 │          2          1  a           3.0
   4 │          2          2  a           4.0
```

Custom layout:
```
julia> layout = ProfilesTableLayout(; timestep=:ts, value=:val)
julia> k = DataFrame([:ts => 1:2, :a .=> "a"])
julia> TulipaClustering.matrix_and_keys_to_df(m, k; layout)
4×4 DataFrame
 Row │ rep_period  ts    a       val
   │ Int64       Int64  String  Float64
─────┼────────────────────────────────────
   1 │          1     1  a           1.0
   2 │          1     2  a           2.0
   3 │          2     1  a           3.0
   4 │          2     2  a           4.0
```
"""
function matrix_and_keys_to_df(
  matrix::Matrix{Float64},
  keys::AbstractDataFrame;
  layout::ProfilesTableLayout = ProfilesTableLayout(),
)
  n_columns = size(matrix, 2)
  result = DataFrame(matrix, string.(1:n_columns))
  result = hcat(keys, result)            # prepend the previously deleted columns
  result = stack(result; variable_name = :rep_period) |> dropmissing # convert from wide to long format
  # Rename value column to match layout, if needed
  if layout.value ≠ :value && hasproperty(result, :value)
    rename!(result, :value => layout.value)
  end
  result.rep_period = parse.(Int, result.rep_period)  # change the type of rep_period column to Int
  select!(result, :rep_period, layout.timestep, :)         # move the rep_period column to the front

  return result
end

"""
  append_period_from_source_df_as_rp!(df; source_df, period, rp, key_columns, layout = ProfilesTableLayout())

Extracts a period with index `period` from `source_df` and appends it as a
representative period with index `rp` to `df`, using `key_columns` as keys.
Respects custom column names via `layout`.

# Examples

Default layout:
```
julia> source_df = DataFrame([:period => [1, 1, 2, 2], :timestep => [1, 2, 1, 2], :a .=> "b", :value => 5:8])
4×4 DataFrame
 Row │ period  timestep  a       value
     │ Int64   Int64      String  Int64
─────┼──────────────────────────────────
   1 │      1          1  b           5
   2 │      1          2  b           6
   3 │      2          1  b           7
   4 │      2          2  b           8

julia> df = DataFrame([:rep_period => [1, 1, 2, 2], :timestep => [1, 2, 1, 2], :a .=> "a", :value => 1:4])
4×4 DataFrame
 Row │ rep_period  timestep  a       value
     │ Int64       Int64      String  Int64
─────┼──────────────────────────────────────
   1 │          1          1  a           1
   2 │          1          2  a           2
   3 │          2          1  a           3
   4 │          2          2  a           4

julia> TulipaClustering.append_period_from_source_df_as_rp!(df; source_df, period = 2, rp = 3, key_columns = [:timestep, :a])
6×4 DataFrame
 Row │ rep_period  timestep  a       value
     │ Int64       Int64      String  Int64
─────┼──────────────────────────────────────
   1 │          1          1  a           1
   2 │          1          2  a           2
   3 │          2          1  a           3
   4 │          2          2  a           4
   5 │          3          1  b           7
   6 │          3          2  b           8
```

Custom layout:
```
julia> layout = ProfilesTableLayout(; period=:p, timestep=:ts, value=:val)
julia> src = DataFrame([:p => [1,1,2,2], :ts => [1,2,1,2], :a .=> "b", :val => 5:8])
julia> df = DataFrame([:rep_period => [1,1], :ts => [1,2], :a .=> "a", :val => [1,2]])
julia> TulipaClustering.append_period_from_source_df_as_rp!(df; source_df = src, period = 2, rp = 3, key_columns = [:ts, :a], layout)
4×4 DataFrame
 Row │ rep_period  ts    a       val
   │ Int64       Int64  String  Int64
─────┼──────────────────────────────────
   1 │          1     1  a           1
   2 │          1     2  a           2
   3 │          3     1  b           7
   4 │          3     2  b           8
```
"""
function append_period_from_source_df_as_rp!(
  df::AbstractDataFrame;
  source_df::AbstractDataFrame,
  period::Int,
  rp::Int,
  key_columns::Vector{Symbol},
  layout::ProfilesTableLayout = ProfilesTableLayout(),
)
  period_col = layout.period
  value_col = layout.value
  period_df = source_df[source_df[!, period_col] .== period, :]
  period_df[!, period_col] .= rp
  select!(period_df, period_col => :rep_period, key_columns..., value_col)
  append!(df, period_df)
end

"""
    greedy_convex_hull(matrix; n_points, distance, initial_indices, mean_vector)

  Greedy method for finding `n_points` points in a hull of the dataset. The points
  are added iteratively, at each step the point that is the furthest away from the
  hull of the current set of points is found and added to the hull.

  - `matrix`: the clustering matrix
  - `n_points`: number of hull points to find
  - `distance`: distance semimetric
  - `initial_indices`: initial points which must be added to the hull, can be nothing
  - `mean_vector`: when adding the first point (if `initial_indices` is not given),
      it will be chosen as the point furthest away from the `mean_vector`; this can be
      nothing, in which case the first step will add a point furtherst away from
      the centroid (the mean) of the dataset
"""
function greedy_convex_hull(
  matrix::AbstractMatrix{Float64};
  n_points::Int,
  distance::SemiMetric,
  initial_indices::Union{Vector{Int}, Nothing} = nothing,
  mean_vector::Union{Vector{Float64}, Nothing} = nothing,
)
  # First resolve the points that are already in the hull given via `initial_indices`
  if initial_indices ≡ nothing
    if mean_vector ≡ nothing
      mean_vector = vec(mean(matrix; dims = 2))
    end
    distances_from_mean = [distance(mean_vector, matrix[:, j]) for j in axes(matrix, 2)]
    initial_indices = [argmax(distances_from_mean)]
  end

  # If there are more initial points than `n_points`, return the first `n_points`
  if length(initial_indices) ≥ n_points
    return initial_indices[1:n_points]
  end

  # Start filling in the remaining points
  hull_indices = initial_indices
  distances_cache = Dict{Int, Float64}()  # store previously computed distances
  starting_index = length(initial_indices) + 1

  for _ in starting_index:n_points
    # Find the point that is the furthest away from the current hull
    max_distance = -Inf
    furthest_vector_index = nothing
    hull_matrix = matrix[:, hull_indices]
    projection_matrix = pinv(hull_matrix)
    for column_index in axes(matrix, 2)
      if column_index in hull_indices
        continue
      end
      last_added_vector = matrix[:, last(hull_indices)]
      target_vector = matrix[:, column_index]

      # Check whether the distance was previosly computed
      cached_distance = get(distances_cache, column_index, Inf)
      d_temp = distance(target_vector, last_added_vector)
      if d_temp ≥ cached_distance
        d_min = cached_distance
      else
        subgradient = x -> hull_matrix' * (hull_matrix * x - target_vector)
        x = projection_matrix * target_vector
        x =
          projected_subgradient_descent!(x; subgradient, projection = project_onto_simplex)
        projected_target = hull_matrix * x
        d = distance(projected_target, target_vector)
        d_min = min(d, d_temp)
        distances_cache[column_index] = d_min
      end

      if d_min > max_distance
        max_distance = d_min
        furthest_vector_index = column_index
      end
    end

    # If no point is found for some reason, throw an error
    if furthest_vector_index ≡ nothing
      throw(ArgumentError("Point not found"))
    end

    # Add the found point to the hull
    push!(hull_indices, furthest_vector_index)
  end
  return hull_indices
end

"""
    validate_initial_representatives(
      initial_representatives,
      clustering_data,
      aux_clustering,
      last_period_excluded,
      n_rp;
      layout = ProfilesTableLayout()
    )

Validates that `initial_representatives` is compatible with `clustering_data` for
use in `find_representative_periods`, considering custom column names via `layout`.
Checks include:
  1. Key columns match between initial representatives and clustering data.
  2. Initial representatives do not contain an incomplete last period.
  3. Both dataframes have the same set of keys (no extra/missing keys).
  4. The number of periods in `initial_representatives` does not exceed `n_rp`
     (adjusted for `last_period_excluded`).

# Examples

```
julia> df = DataFrame([:period => [1,1,2,2], :timestep => [1,2,1,2], :zone .=> "A", :value => 10:13])
julia> aux = TulipaClustering.find_auxiliary_data(df)
julia> init = DataFrame([:period => [1,1], :timestep => [1,2], :zone .=> "A", :value => [10, 11]])
julia> TulipaClustering.validate_initial_representatives(init, df, aux, false, 2)
```

Custom layout:
```
julia> layout = ProfilesTableLayout(; period=:p, timestep=:ts, value=:val)
julia> df2 = DataFrame([:p => [1,1,2,2], :ts => [1,2,1,2], :zone .=> "A", :val => 10:13])
julia> aux2 = TulipaClustering.find_auxiliary_data(df2; layout)
julia> init2 = DataFrame([:p => [1,1], :ts => [1,2], :zone .=> "A", :val => [10, 11]])
julia> TulipaClustering.validate_initial_representatives(init2, df2, aux2, false, 2; layout)
```
"""
function validate_initial_representatives(
  initial_representatives::AbstractDataFrame,
  clustering_data::AbstractDataFrame,
  aux_clustering::AuxiliaryClusteringData,
  last_period_excluded::Bool,
  n_rp::Int,
  layout::ProfilesTableLayout = ProfilesTableLayout(),
)

  # Calling find_auxiliary_data on the initial representatives already checks whether the dataframes satisfies some of the base requirements (:period, :value, :timestep)
  aux_initial = find_auxiliary_data(initial_representatives; layout)

  # 1. Check that the column names for initial representatives are the same as for clustering data
  if aux_clustering.key_columns ≠ aux_initial.key_columns
    throw(
      ArgumentError(
        "Key columns of initial represenatives do not match clustering data\nExpected was: $(aux_clustering.key_columns) \nFound was: $(aux_initial.key_columns)",
      ),
    )
  end

  # 2. Check that initial representatives do not contain a incomplete period
  if aux_initial.last_period_duration ≠ aux_clustering.period_duration
    throw(
      ArgumentError(
        "Initial representatives have an incomplete last period, which is not allowed",
      ),
    )
  end

  # 3. Check that the initial representatives and clustering data have the same keys
  more_keys_initial = size(
    antijoin(initial_representatives, clustering_data; on = aux_clustering.key_columns),
    1,
  )
  more_keys_clustering = size(
    antijoin(clustering_data, initial_representatives; on = aux_clustering.key_columns),
    1,
  )
  if more_keys_initial > 0 || more_keys_clustering > 0
    throw(
      ArgumentError(
        "Initial representatives and clustering data do not have the same keys\n" *
        "There are $(more_keys_initial) extra keys in initial representatives\n" *
        "and $(more_keys_clustering) extra keys in clustering data.",
      ),
    )
  end

  # 4. Make sure that initial representatives does not contain more periods than asked
  if !last_period_excluded && n_rp < aux_initial.n_periods
    throw(
      ArgumentError(
        "The number of representative periods is $n_rp but has to be at least $(aux_initial.n_periods).",
      ),
    )
  end

  if last_period_excluded && n_rp < aux_initial.n_periods + 1
    throw(
      ArgumentError(
        "The number of representative periods is $n_rp but has to be at least $(aux_initial.n_periods + 1).",
      ),
    )
  end
end
