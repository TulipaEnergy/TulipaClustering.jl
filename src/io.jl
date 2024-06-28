export read_clustering_data_from_csv_folder, write_clustering_result_to_csv_folder

"""
    weight_matrix_to_df(weights)

Converts a weight matrix from a (sparse) matrix, which is more convenient for
internal computations, to a dataframe, which is better for saving into a file.
Zero weights are dropped to avoid cluttering the dataframe.
"""
function weight_matrix_to_df(weights::Union{SparseMatrixCSC{Float64, Int64}, Matrix{Float64}})
  weights = sparse(weights)
  periods, rep_periods, values = weights |> findnz
  result = DataFrame(period = periods, rep_period = rep_periods, weight = values)
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
  DuckDB.register_data_frame(connection, clustering_result.profiles, "profiles_rep_periods")

  DuckDB.register_data_frame(
    connection,
    weight_matrix_to_df(clustering_result.weight_matrix),
    "rep_periods_mapping",
  )

  return nothing
end
