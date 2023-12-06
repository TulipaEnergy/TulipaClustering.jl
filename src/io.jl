export read_clustering_data_from_csv_folder, write_clustering_result_to_csv_folder

"""
    read_clustering_data_from_csv_folder(input_folder)

Returns the data frame with all of the needed data from the `input_folder`.

`assets-profiles.csv` should exist in the directory, following the [`TulipaClustering.AssetProfiles`](@ref) specification.
"""
function read_clustering_data_from_csv_folder(input_folder::AbstractString)::DataFrame
  # Read the data
  fillpath(filename) = joinpath(input_folder, filename)
  df = read_csv_with_schema(fillpath("assets-profiles.csv"), AssetProfiles)
  return df
end

"""
    read_csv_with_schema(file_path, schema)

Reads the csv with file_name at location path validating the data using the schema.
It is assumes that the file's header is at the second row.
The first row of the file contains some metadata information that is not used.
"""
function read_csv_with_schema(file_path, schema; csvargs...)
  # Get the schema names and types in the form of Dictionaries
  # TODO: This is copied from TulipaEnergyModel.jl; this should probably be in
  # a separate module (TulipaIO.jl?) so that different modules can access the
  # API methods to read and write the data.
  col_types = zip(fieldnames(schema), fieldtypes(schema)) |> Dict
  df = CSV.read(
    file_path,
    DataFrames.DataFrame;
    header = 2,
    types = col_types,
    strict = true,
    csvargs...,
  )

  return df
end

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
