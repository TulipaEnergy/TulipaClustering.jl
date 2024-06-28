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
    write_clustering_result_to_csv_folder(output_folder, clustering_result)

Writes a [`TulipaClustering.ClusteringResult`](@ref) to CSV files in the
`output_folder`.
"""
function write_clustering_result_to_csv_folder(
  output_folder::AbstractString,
  clustering_result::TulipaClustering.ClusteringResult,
)
  mkpath(output_folder)

  fillpath(filename) = joinpath(output_folder, filename)

  write_csv_with_prefixes(
    fillpath("assets-profiles.csv"),
    clustering_result.profiles,
    prefixes = [missing, missing, missing, "MW"],
  )

  write_csv_with_prefixes(
    fillpath("rp-weights.csv"),
    weight_matrix_to_df(clustering_result.weight_matrix),
    prefixes = [missing, missing, missing],
  )

  return nothing
end

"""
    write_csv_with_prefixes(file_path, df; prefixes)

Writes the dataframe `df` into a csv file at `file_path`. If `prefixes` are
provided, they are written above the column names. For example, these prefixes
can contain metadata describing the columns.
"""
function write_csv_with_prefixes(file_path, df; prefixes = nothing, csvargs...)
  if isnothing(prefixes) || length(prefixes) == 0
    # If there are no prefixes, just write the data frame into the file
    CSV.write(file_path, df; strict = true, csvargs...)
  else
    # Convert the prefixes to a one-row table for `CSV.write` to use
    prefixes = reshape(prefixes, (1, length(prefixes))) |> Tables.table
    # First we write the prefixes, then we append the data drom the dataframe
    CSV.write(file_path, prefixes; header = false, strict = true, csvargs...)
    CSV.write(file_path, df; header = true, append = true, strict = true, csvargs...)
  end
  return nothing
end
