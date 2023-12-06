export read_clustering_data_from_csv_folder, write_clustering_result_to_csv_folder

"""
    read_clustering_data_from_csv_folder(input_folder)

Returns the [`TulipaClustering.ClusteringData`](@ref) reading all data from CSV files
in the `input_folder`.

The following files are expected to exist in the input folder:

  - `demand.csv`: Following the [`TulipaClustering.DemandData`](@ref) specification.
  - `generation-availability.csv`: Following the [`TulipaClustering.GenerationAvailabilityData`](@ref) specification.

The output contains:

  - `demand`: a DataFrame of demand values at different nodes per time step
  - `generation_availability`: a DataFrame of availability coefficients for different generation technologies located at different nodes per time step
"""
function read_clustering_data_from_csv_folder(input_folder::AbstractString)
  # Read data
  fillpath(filename) = joinpath(input_folder, filename)

  demand_df = read_csv_with_schema(fillpath("demand.csv"), DemandData)
  generation_availability_df =
    read_csv_with_schema(fillpath("generation-availability.csv"), GenerationAvailabilityData)

  return ClusteringData(demand_df, generation_availability_df)
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
    fillpath("demand.csv"),
    clustering_result.demand,
    prefixes = [missing, missing, missing, "MW"],
  )

  write_csv_with_prefixes(
    fillpath("generation-availability.csv"),
    clustering_result.generation_availability,
    prefixes = [missing, missing, missing, missing, "p.u."],
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
