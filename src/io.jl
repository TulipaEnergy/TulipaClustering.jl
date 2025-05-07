export read_clustering_data_from_csv_folder, write_clustering_result_to_csv_folder

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
    write_clustering_result_to_table(connection, clustering_result)

Writes a [`TulipaClustering.ClusteringResult`](@ref) to CSV files in the
`output_folder`.
"""
function write_clustering_result_to_tables(
  connection,
  clustering_result::TulipaClustering.ClusteringResult,
)
  years = DataFrame(; :year => clustering_result.profiles.year |> unique)

  DuckDB.register_data_frame(connection, clustering_result.profiles, "profiles_rep_periods")
  mapping_df = weight_matrix_to_df(clustering_result.weight_matrix)

  DuckDB.register_data_frame(
    connection,
    crossjoin(years, mapping_df),
    "rep_periods_mapping",
  )

  aux = clustering_result.auxiliary_data
  num_rep_periods = size(clustering_result.weight_matrix, 2)
  rep_period_duration = fill(aux.period_duration, num_rep_periods)
  rep_period_duration[end] = aux.last_period_duration
  rp_data_df = DataFrame(;
    rep_period = 1:num_rep_periods,
    num_timesteps = rep_period_duration,
    resolution = 1.0,
  )
  DuckDB.register_data_frame(connection, crossjoin(years, rp_data_df), "rep_periods_data")

  num_periods = size(clustering_result.weight_matrix, 1)
  period_duration = fill(aux.period_duration, num_periods)
  period_duration[end] = aux.last_period_duration
  period_data_df = DataFrame(; period = 1:num_periods, num_timesteps = period_duration)
  DuckDB.register_data_frame(connection, crossjoin(years, period_data_df), "timeframe_data")
  return nothing
end
