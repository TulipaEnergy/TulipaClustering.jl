export read_clustering_data_from_csv_folder

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
