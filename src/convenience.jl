export cluster!, dummy_cluster!, transform_wide_to_long!

"""
    cluster!(
        connection,
        period_duration,
        num_rps;
        input_database_schema = "",
        input_profile_table_name = "profiles",
        database_schema = "",
        drop_incomplete_last_period::Bool = false,
        method::Symbol = :k_means,
        distance::SemiMetric = SqEuclidean(),
        initial_representatives::AbstractDataFrame = DataFrame(),
        weight_type::Symbol = :convex,
        tol::Float64 = 1e-2,
        clustering_kwargs = Dict(),
        weight_fitting_kwargs = Dict(),
        niters::Int = 100,
        learning_rate::Float64 = 0.001,
        adaptive_grad::Bool = false,
    )

Convenience function to cluster the table named in `input_profile_table_name`
using `period_duration` and `num_rps`. The resulting tables
`profiles_rep_periods`, `rep_periods_mapping`, and
`rep_periods_data` are loaded into `connection` in the `database_schema`, if
given, and enriched with `year` information.

This function extract the table, then calls [`split_into_periods!`](@ref),
[`find_representative_periods`](@ref), [`fit_rep_period_weights!`](@ref), and
finally `write_clustering_result_to_tables`.

## Arguments

**Required**

- `connection`: DuckDB connection
- `period_duration`: Duration of each period, i.e., number of `timestep`s.
- `num_rps`: Number of find_representative_periods

**Keyword arguments**

- `input_database_schema` (default `""`): Schema of the input tables
- `input_profile_table_name` (default `"profiles"`): Default name of the `profiles` table inside the above schemaa
- `database_schema` (default `""`): Schema of the output tables
- `drop_incomplete_last_period` (default `false`): controls how the last period is treated if it
  is not complete: if this parameter is set to `true`, the incomplete period
  is dropped and the weights are rescaled accordingly; otherwise, clustering
  is done for `n_rp - 1` periods, and the last period is added as a special
  shorter representative period
- `method` (default `:k_means``): clustering method to use, either `:k_means` and `:k_medoids`
- `distance` (default `Distances.SqEuclidean()`): semimetric used to measure distance between data points.
- `initial_representatives` initial representatives that should be
    included in the clustering. The period column in the initial representatives
    should be 1-indexed and the key columns should be the same as in the clustering data.
    For the hull methods it will be added before clustering, for :k_means and :k_medoids
    it will be added after clustering.
- `weight_type` (default `:convex`): the type of weights to find; possible values are:
    - `:convex`: each period is represented as a convex sum of the
      representative periods (a sum with nonnegative weights adding into one)
    - `:conical`: each period is represented as a conical sum of the
      representative periods (a sum with nonnegative weights)
    - `:conical_bounded`: each period is represented as a conical sum of the
      representative periods (a sum with nonnegative weights) with the total
      weight bounded from above by one.
- `tol` (default `1e-2`): algorithm's tolerance; when the weights are adjusted by a value less
  then or equal to `tol`, they stop being fitted further.
- `clustering_kwargs` (default `Dict()`): Extra keyword arguments passed to [`find_representative_periods`](@ref)
- `weight_fitting_kwargs` (default `Dict()`): Extra keyword arguments passed to [`fit_rep_period_weights!`](@ref)
"""
function cluster!(
  connection,
  period_duration,
  num_rps;
  input_database_schema = "",
  input_profile_table_name = "profiles",
  database_schema = "",
  drop_incomplete_last_period::Bool = false,
  method::Symbol = :k_means,
  distance::SemiMetric = SqEuclidean(),
  initial_representatives::AbstractDataFrame = DataFrame(),
  weight_type::Symbol = :convex,
  tol::Float64 = 1e-2,
  clustering_kwargs = Dict(),
  weight_fitting_kwargs = Dict(),
)
  prefix = ""
  if database_schema != ""
    DBInterface.execute(connection, "CREATE SCHEMA IF NOT EXISTS $database_schema")
    prefix = "$database_schema."
  end
  validate_data!(
    connection;
    input_database_schema,
    table_names = Dict("profiles" => input_profile_table_name),
  )

  if input_database_schema != ""
    input_profile_table_name = "$input_database_schema.$input_profile_table_name"
  end
  df = DuckDB.query(
    connection,
    "SELECT * FROM $input_profile_table_name
    ",
  ) |> DataFrame
  combine_periods!(df)
  split_into_periods!(df; period_duration)
  clusters = find_representative_periods(
    df,
    num_rps;
    drop_incomplete_last_period,
    method,
    distance,
    initial_representatives,
    clustering_kwargs...,
  )
  fit_rep_period_weights!(clusters; weight_type, tol, weight_fitting_kwargs...)

  for table_name in
      ("rep_periods_data", "rep_periods_mapping", "profiles_rep_periods", "timeframe_data")
    DuckDB.query(connection, "DROP TABLE IF EXISTS $prefix$table_name")
  end
  write_clustering_result_to_tables(connection, clusters; database_schema)

  return clusters
end

"""
    dummy_cluster!(connection)

Convenience function to create the necessary columns and tables when clustering
is not required.

This is essentially creating a single representative period with the size of
the whole profile.
See [`cluster!`](@ref) for more details of what is created.
"""
function dummy_cluster!(
  connection;
  input_database_schema = "",
  input_profile_table_name = "profiles",
  kwargs...,
)
  table_name = if input_database_schema != ""
    "$input_database_schema.$input_profile_table_name"
  else
    input_profile_table_name
  end
  period_duration = only([
    row.max_timestep for row in
    DuckDB.query(connection, "SELECT MAX(timestep) AS max_timestep FROM $table_name")
  ])
  cluster!(connection, period_duration, 1; kwargs...)
end

"""
    transform_wide_to_long!(
        connection,
        wide_table_name,
        long_table_name;
    )

Convenience function to convert a table in wide format to long format using DuckDB.
Originally aimed at converting a profile table like the following:

| year | timestep | name1 | name2 | ⋯  | name2 |
| ---- | -------- | ----- | ----- | -- | ----- |
| 2030 |        1 |   1.0 |   2.5 | ⋯  |   0.0 |
| 2030 |        2 |   1.5 |   2.6 | ⋯  |   0.0 |
| 2030 |        3 |   2.0 |   2.6 | ⋯  |   0.0 |

To a table like the following:

| year | timestep | profile_name | value |
| ---- | -------- | ------------ | ----- |
| 2030 |        1 |        name1 |   1.0 |
| 2030 |        2 |        name1 |   1.5 |
| 2030 |        3 |        name1 |   2.0 |
| 2030 |        1 |        name2 |   2.5 |
| 2030 |        2 |        name2 |   2.6 |
| 2030 |        3 |        name2 |   2.6 |
|    ⋮ |        ⋮ |            ⋮ |     ⋮ |
| 2030 |        1 |        name3 |   0.0 |
| 2030 |        2 |        name3 |   0.0 |
| 2030 |        3 |        name3 |   0.0 |

This conversion is done using the `UNPIVOT` SQL command from DuckDB.

## Keyword arguments

- `exclude_columns = ["year", "timestep"]`: Which tables to exclude from the conversion
- `name_column = "profile_name"`: Name of the new column that contains the names of the old columns
- `value_column = "value"`: Name of the new column that holds the values from the old columns
"""
function transform_wide_to_long!(
  connection,
  wide_table_name,
  long_table_name;
  exclude_columns = ["year", "timestep"],
  name_column = "profile_name",
  value_column = "value",
)
  @assert length(exclude_columns) > 0
  exclude_str = join(exclude_columns, ", ")
  DuckDB.query(
    connection,
    "CREATE OR REPLACE TABLE $long_table_name AS
    UNPIVOT $wide_table_name
    ON COLUMNS(* EXCLUDE ($exclude_str))
    INTO
        NAME $name_column
        VALUE $value_column
    ORDER BY $name_column, $exclude_str
    ",
  )

  return
end
