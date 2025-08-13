export write_clustering_result_to_tables

"""
    weight_matrix_to_df(weights)

Converts a weight matrix from a (sparse) matrix, which is more convenient for
internal computations, to a dataframe, which is better for saving into a file.
Zero weights are dropped to avoid cluttering the dataframe.
"""
function weight_matrix_to_df(
  weights::Union{SparseMatrixCSC{Float64, Int64}, Matrix{Float64}},
)
  weights = sparse(weights)
  periods, rep_periods, values = weights |> findnz
  result = DataFrame(; period = periods, rep_period = rep_periods, weight = values)
  sort!(result, [:period, :rep_period])
  return result
end

"""
    write_clustering_result_to_tables(connection, clustering_result; database_schema="", layout=DataFrameLayout())

Writes a [`TulipaClustering.ClusteringResult`](@ref) into DuckDB tables in `connection`.

Notes
- The `profiles_rep_periods` table is standardized to the default output schema regardless of the
  layout used during in-memory processing: columns are `profile_name`, `rep_period`, `timestep`, and `value`.
- Pass `layout` if the in-memory profiles DataFrame uses custom names for `timestep` and `value` so they can be
  renamed to the default output schema.
"""
function write_clustering_result_to_tables(
  connection,
  clustering_result::TulipaClustering.ClusteringResult;
  database_schema = "",
  layout::DataFrameLayout = DataFrameLayout(),
)
  mapping_df = weight_matrix_to_df(clustering_result.weight_matrix)

  # Below we register the dataframes as `t_<name>` in the general schema,
  # because we can't create them directly into the correct schema.
  # Then, we create tables in the correct schema copying these tables.
  # Finally, we drop them.
  prefix = ""
  if database_schema != ""
    DBInterface.execute(connection, "CREATE SCHEMA IF NOT EXISTS $database_schema")
    prefix = "$database_schema."
  end

  # Standardize profiles columns to default output schema irrespective of in-memory layout
  df_profiles = copy(clustering_result.profiles)
  if layout.timestep != :timestep && hasproperty(df_profiles, layout.timestep)
    rename!(df_profiles, layout.timestep => :timestep)
  end
  if layout.value != :value && hasproperty(df_profiles, layout.value)
    rename!(df_profiles, layout.value => :value)
  end
  DuckDB.register_data_frame(connection, df_profiles, "t_profiles_rep_periods")
  DuckDB.register_data_frame(connection, mapping_df, "t_rep_periods_mapping")

  aux = clustering_result.auxiliary_data
  num_rep_periods = size(clustering_result.weight_matrix, 2)
  rep_period_duration = fill(aux.period_duration, num_rep_periods)
  rep_period_duration[end] = aux.last_period_duration
  rp_data_df = DataFrame(;
    rep_period = 1:num_rep_periods,
    num_timesteps = rep_period_duration,
    resolution = 1.0,
  )
  DuckDB.register_data_frame(connection, rp_data_df, "t_rep_periods_data")

  num_periods = size(clustering_result.weight_matrix, 1)
  period_duration = fill(aux.period_duration, num_periods)
  period_duration[end] = aux.last_period_duration
  period_data_df = DataFrame(; period = 1:num_periods, num_timesteps = period_duration)
  DuckDB.register_data_frame(connection, period_data_df, "t_timeframe_data")

  for table_name in
      ("profiles_rep_periods", "rep_periods_data", "rep_periods_mapping", "timeframe_data")
    DuckDB.query(
      connection,
      "CREATE OR REPLACE TABLE $(prefix)$table_name AS FROM t_$table_name",
    )
    DuckDB.query(connection, "DROP VIEW t_$table_name")
  end

  return nothing
end
