export find_representative_periods,
  split_into_periods!, find_auxiliary_data, validate_initial_representatives

"""
    combine_periods!(df; layout = DataFrameLayout())

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
  * `layout::DataFrameLayout`: Describes the column names for `period` and
    `timestep` (defaults to standard names). Pass a custom layout if your
    dataframe uses different symbols.

Behavior & edge cases:
  * If the `timestep` column (as specified by `layout`) is missing, a `DomainError` is thrown.
  * If the `period` column is absent, the function is a no-op (returns immediately).
  * Non-1-based or non-consecutive per-period timesteps are not validated; unusual
    values may result in non-contiguous or non-strictly increasing global indices.
  * Works in-place; the modified dataframe (without `period`) is also returned for convenience.

Complexity: O(n) over the number of rows (simple vectorised arithmetic + column drop).

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
julia> layout = DataFrameLayout(; period = :p, timestep = :ts)
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
function combine_periods!(df::AbstractDataFrame; layout = DataFrameLayout())
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
    split_into_periods!(df; period_duration=nothing, layout=DataFrameLayout())

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
julia> layout = DataFrameLayout(; timestep = :time_step, period = :periods)
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
  layout = DataFrameLayout(),
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
  validate_df_and_find_key_columns(df; layout = DataFrameLayout())

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
julia> layout = DataFrameLayout(; period = :p, timestep = :ts, value = :val)
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
  layout::DataFrameLayout = DataFrameLayout(),
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
    find_auxiliary_data(clustering_data; layout = DataFrameLayout())

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

julia> layout = DataFrameLayout(; period=:p, timestep=:ts, value=:val)
julia> df2 = DataFrame([:p => [1,1,2,2], :ts => [1,2,1,1], :a => "x", :val => 10:13])
julia> TulipaClustering.find_auxiliary_data(df2; layout)
AuxiliaryClusteringData([:ts, :a], 2, 1, 2, nothing)
```
"""
function find_auxiliary_data(
  clustering_data::AbstractDataFrame;
  layout::DataFrameLayout = DataFrameLayout(),
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
  df_to_matrix_and_keys(df, key_columns; layout = DataFrameLayout())

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
julia> layout = DataFrameLayout(; period=:p, timestep=:ts, value=:val)
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
  layout::DataFrameLayout = DataFrameLayout(),
)
  wide_df = unstack(df, key_columns, layout.period, layout.value)
  matrix = select(wide_df, Not(key_columns)) |> dropmissing |> Matrix{Float64}
  keys = select(wide_df, key_columns)
  return matrix, keys
end

"""
    matrix_and_keys_to_df(matrix, keys; layout = DataFrameLayout())

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
julia> layout = DataFrameLayout(; timestep=:ts, value=:val)
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
  layout::DataFrameLayout = DataFrameLayout(),
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
  append_period_from_source_df_as_rp!(df; source_df, period, rp, key_columns, layout = DataFrameLayout())

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
julia> layout = DataFrameLayout(; period=:p, timestep=:ts, value=:val)
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
  layout::DataFrameLayout = DataFrameLayout(),
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
  find_representative_periods(
    clustering_data,
    n_rp;
    drop_incomplete_last_period = false,
    method = :k_means,
    distance = SqEuclidean(),
    initial_representatives = DataFrame(),
    layout = DataFrameLayout(),
    args...,
  )

Finds representative periods via data clustering. Honors custom column names via
`layout` (defaults to `(:period, :timestep, :value)`).

Arguments
  - `clustering_data`: long-format data to cluster.
  - `n_rp`: number of representative periods to find.
  - `drop_incomplete_last_period`: controls how the last period is treated if it
    is not complete: if this parameter is set to `true`, the incomplete period
    is dropped and the weights are rescaled accordingly; otherwise, clustering
    is done for `n_rp - 1` periods, and the last period is added as a special
    shorter representative period.
  - `method`: clustering method to use `:k_means`, `:k_medoids`, `:convex_hull`, `:convex_hull_with_null`, or `:conical_hull`.
  - `distance`: semimetric used to measure distance between data points.
  - `initial_representatives`: dataframe of initial RPs. It must use the same key
    columns and follow the same `layout` as `clustering_data`. For hull methods the
    RPs are prepended before clustering; for `:k_means`/`:k_medoids` they are appended
    after clustering.
  - `layout`: `DataFrameLayout` describing the column names.
  - other named arguments are forwarded to the clustering method.

# Returns

Returns a `ClusteringResult` with:
  - `profiles::DataFrame`: Long-format representative profiles with columns
    `:rep_period`, `layout.timestep`, all key columns (`auxiliary_data.key_columns`),
    and `layout.value`.
  - `weight_matrix::SparseMatrixCSC{Float64,Int}` (or dense `Matrix{Float64}`):
    rows correspond to source periods and columns to representative periods; entry
    `(p, r)` is the weight of period `p` assigned to representative `r`.
    If the last period is incomplete and `drop_incomplete_last_period` is false,
    it maps to its own representative column with its specific weight; if dropped,
    it is excluded from the rows.
  - `clustering_matrix::Matrix{Float64}`: The feature-by-period matrix used for
    clustering (features are derived from `layout.timestep` crossed with key columns).
  - `rp_matrix::Matrix{Float64}`: The representative profiles in matrix form
    (same feature layout as `clustering_matrix`).
  - `auxiliary_data::AuxiliaryClusteringData`: Auxiliary metadata such as
    `key_columns`, `period_duration`, `last_period_duration`, `n_periods`, and
    (for applicable methods) `medoids` indices.

# Examples

Finding two representatives using default values:
```
julia> df = DataFrame(
           period = kron(1:4, ones(Int, 2)),
           timestep = repeat(1:2, 4),
           profile = "A",
           value = 1:8,
         )

julia> res = TulipaClustering.find_representative_periods(df, 2)
```

Finding two representatives using k-medoids and a custom layout:
```
julia> layout = DataFrameLayout(; period = :p, timestep = :ts, value = :val)

julia> df = DataFrame(
           p = kron(1:4, ones(Int, 2)),
           ts = repeat(1:2, 4),
           profile = "A",
           val = 1:8,
         )

julia> res = TulipaClustering.find_representative_periods(df, 2; method = :k_medoids, layout)
```
"""
function find_representative_periods(
  clustering_data::AbstractDataFrame,
  n_rp::Int;
  drop_incomplete_last_period::Bool = false,
  method::Symbol = :k_means,
  distance::SemiMetric = SqEuclidean(),
  initial_representatives::AbstractDataFrame = DataFrame(),
  layout::DataFrameLayout = DataFrameLayout(),
  args...,
)
  # 1. Check that the number of RPs makes sense. The first check can be done immediately,
  # The second check is done after we compute the auxiliary data
  if n_rp < 1
    throw(
      ArgumentError(
        "The number of representative periods is $n_rp but has to be at least 1.",
      ),
    )
  end

  # Find auxiliary data and pre-compute additional constants that are used multiple times alter
  aux = find_auxiliary_data(clustering_data; layout)
  n_periods = aux.n_periods

  if n_rp > n_periods
    throw(
      ArgumentError(
        "The number of representative periods exceeds the total number of periods, $n_rp > $n_periods.",
      ),
    )
  end

  has_incomplete_last_period = aux.last_period_duration ≠ aux.period_duration
  is_last_period_excluded = has_incomplete_last_period && !drop_incomplete_last_period
  n_complete_periods = has_incomplete_last_period ? n_periods - 1 : n_periods

  # Check that the initial representatives are compatible with the clustering data
  if !isempty(initial_representatives)
    validate_initial_representatives(
      initial_representatives,
      clustering_data,
      aux,
      is_last_period_excluded,
      n_rp,
      layout,
    )
    i_rp = maximum(initial_representatives.period) # number of provided representative periods
  else
    i_rp = 0
  end

  # 2. Find the weights of the two types of periods and pre-build the weight matrix.
  # We assume that the only period that can be incomplete (i.e., has a duration
  # that is less than aux.period_duration) is the very last one. All other periods
  # are complete periods.
  complete_period_weight, incomplete_period_weight = find_period_weights(
    aux.period_duration,
    aux.last_period_duration,
    n_periods,
    drop_incomplete_last_period,
  )

  # In both cases, the weights of the complete periods will be found after clustering.
  if is_last_period_excluded
    weight_matrix = sparse([n_periods], [n_rp], [incomplete_period_weight])
    n_rp -= 1  # incomplete last period becomes its own representative, exclude it from clustering
  else
    weight_matrix = spzeros(n_complete_periods, n_rp)
  end

  # 3. Build the clustering matrix

  period_col = layout.period
  if method in [:k_means, :k_medoids] && !isempty(initial_representatives)
    # If clustering is k-means or k-medoids we remove amount of initial representatives from n_rp
    n_rp -= i_rp
    clustering_matrix, keys = df_to_matrix_and_keys(
      clustering_data[clustering_data[!, period_col] .≤ n_complete_periods, :],
      aux.key_columns;
      layout,
    )

  elseif method in [:convex_hull, :convex_hull_with_null, :conical_hull] &&
         !isempty(initial_representatives)
    # If clustering is one of the hull methods, we add initial representatives to the clustering matrix in front
    updated_clustering_data = deepcopy(clustering_data)
    updated_clustering_data[!, period_col] = updated_clustering_data[!, period_col] .+ i_rp
    clustering_data = vcat(initial_representatives, updated_clustering_data)

    clustering_matrix, keys = df_to_matrix_and_keys(
      clustering_data[
        clustering_data[
          !,
          period_col,
        ] .≤ (n_complete_periods + maximum(initial_representatives[!, period_col])),
        :,
      ],
      aux.key_columns;
      layout,
    )
  else
    clustering_matrix, keys = df_to_matrix_and_keys(
      clustering_data[clustering_data[!, period_col] .≤ n_complete_periods, :],
      aux.key_columns;
      layout,
    )
  end

  # 4. Do the clustering, now that the data is transformed into a matrix
  if n_rp == 0 # If due to the additional representatives we have no clustering, create an empty placeholder
    rp_matrix = nothing
    assignments = Int[]
  elseif method ≡ :k_means
    # Do the clustering
    kmeans_result = kmeans(clustering_matrix, n_rp; distance, args...)

    # Reinterpret the results
    rp_matrix = kmeans_result.centers
    assignments = kmeans_result.assignments
  elseif method ≡ :k_medoids
    # Do the clustering
    # k-medoids uses distance matrix instead of clustering matrix
    distance_matrix = pairwise(distance, clustering_matrix; dims = 2)
    kmedoids_result = kmedoids(distance_matrix, n_rp; args...)

    # Reinterpret the results
    rp_matrix = clustering_matrix[:, kmedoids_result.medoids]
    assignments = kmedoids_result.assignments
    aux.medoids = kmedoids_result.medoids
  elseif method ≡ :convex_hull
    # Do the clustering, with initial indices if provided
    initial_indices = if !isempty(initial_representatives)
      collect(1:i_rp)
    else
      nothing
    end
    hull_indices = greedy_convex_hull(
      clustering_matrix;
      initial_indices = initial_indices,
      n_points = n_rp,
      distance,
    )

    # Reinterpret the results
    rp_matrix = clustering_matrix[:, hull_indices]
    assignments = [
      argmin([
        distance(clustering_matrix[:, h], clustering_matrix[:, p + i_rp]) for
        h in hull_indices
      ]) for p in 1:n_complete_periods
    ]
    clustering_matrix = clustering_matrix[:, (i_rp + 1):end]
    aux.medoids = hull_indices
  elseif method ≡ :convex_hull_with_null
    # Check if we can add null to the clustering matrix. The distance to null can
    # be undefined, e.g., for the cosine distance.
    is_distance_to_zero_undefined =
      isnan(distance(zeros(size(clustering_matrix, 1), 1), clustering_matrix[:, 1]))

    if is_distance_to_zero_undefined
      throw(
        ArgumentError(
          "cannot add null to the clustering data because distance to it is undefined",
        ),
      )
    end

    # Add null to the clustering matrix
    matrix = [zeros(size(clustering_matrix, 1), 1) clustering_matrix]

    # Do the clustering
    hull_indices = greedy_convex_hull(
      matrix;
      n_points = n_rp + 1,
      distance,
      initial_indices = collect(1:(i_rp + 1)),
    )

    # Remove null from the beginning and shift all indices by one
    popfirst!(hull_indices)
    hull_indices .-= 1

    # Reinterpret the results
    rp_matrix = clustering_matrix[:, hull_indices]
    assignments = [
      argmin([
        distance(clustering_matrix[:, h], clustering_matrix[:, p + i_rp]) for
        h in hull_indices
      ]) for p in 1:n_complete_periods
    ]
    clustering_matrix = clustering_matrix[:, (i_rp + 1):end]

    aux.medoids = hull_indices
  elseif method ≡ :conical_hull
    # Do a gnomonic projection (normalization) of the data
    normal_vector = vec(mean(clustering_matrix; dims = 2))
    normalize!(normal_vector)
    projection_coefficients = [
      1.0 / dot(normal_vector, clustering_matrix[:, j]) for j in axes(clustering_matrix, 2)
    ]
    projected_matrix = [
      clustering_matrix[i, j] * projection_coefficients[j] for
      i in axes(clustering_matrix, 1), j in axes(clustering_matrix, 2)
    ]

    initial_indices = if !isempty(initial_representatives)
      collect(1:i_rp)
    else
      nothing
    end

    hull_indices = greedy_convex_hull(
      projected_matrix;
      n_points = n_rp,
      distance,
      mean_vector = normal_vector,
      initial_indices = initial_indices,
    )

    # Reinterpret the results
    rp_matrix = clustering_matrix[:, hull_indices]

    assignments = [
      argmin([
        distance(clustering_matrix[:, h], clustering_matrix[:, p + i_rp]) for
        h in hull_indices
      ]) for p in 1:n_complete_periods
    ]
    clustering_matrix = clustering_matrix[:, (i_rp + 1):end]
  else
    throw(ArgumentError("Clustering method is not supported"))
  end

  # 5. Reinterpret the clustering results into a format we need

  # First, convert the matrix data back to dataframes using the previously saved key columns
  rp_df = if rp_matrix ≡ nothing
    nothing
  else
    matrix_and_keys_to_df(rp_matrix, keys; layout)
  end

  # In case of initial representatives and a non hull method, we add them now
  if !isempty(initial_representatives) && method in [:k_means, :k_medoids]
    representatives_to_add = select!(
      initial_representatives,
      period_col => :rep_period,
      aux.key_columns...,
      layout.value,
    )
    representatives_to_add.rep_period .= representatives_to_add.rep_period .+ n_rp
    rp_df = if rp_df === nothing
      representatives_to_add
    else
      vcat(rp_df, representatives_to_add)
    end
    rename!(rp_df, :rep_period => period_col)
    rp_matrix, keys = df_to_matrix_and_keys(rp_df, aux.key_columns; layout)
    rename!(rp_df, period_col => :rep_period)
    rp_matrix
    n_rp += i_rp
  end

  assignments = [
    argmin([
      distance(clustering_matrix[:, p], rp_matrix[:, r]) for r in axes(rp_matrix, 2)
    ]) for p in 1:n_complete_periods
  ]

  for (p, rp) in enumerate(assignments)
    weight_matrix[p, rp] = complete_period_weight
  end

  # Next, re-append the last period if it was excluded from clustering
  if is_last_period_excluded
    n_rp += 1
    append_period_from_source_df_as_rp!(
      rp_df;
      source_df = clustering_data,
      period = n_periods,
      rp = n_rp,
      key_columns = aux.key_columns,
      layout = layout,
    )
    if method ≡ :k_medoids
      append!(aux.medoids, n_complete_periods + 1)
    end
  end

  return ClusteringResult(rp_df, weight_matrix, clustering_matrix, rp_matrix, aux)
end

"""
    validate_initial_representatives(
      initial_representatives,
      clustering_data,
      aux_clustering,
      last_period_excluded,
      n_rp;
      layout = DataFrameLayout()
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
julia> layout = DataFrameLayout(; period=:p, timestep=:ts, value=:val)
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
  layout::DataFrameLayout = DataFrameLayout(),
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
