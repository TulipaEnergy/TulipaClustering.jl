export ProfilesTableLayout

"""
    ProfilesTableLayout(;key = value, ...)
    ProfilesTableLayout(path; ...)

Structure to hold the profiles input data table layout.
Column names in the layout are defined by default.

If `path` is passed, it is expected to be a string pointing to a TOML file with
a `key = value` list of parameters. Explicit keyword arguments take precedence.

## Parameters

- `value::Symbol = :value`: The column name with the profile values. This is a required column.
- `timestep::Symbol = :timestep`: The column name with the time steps in the profile. This is a required column.
- `period::Symbol = :period`: The column name with the period number in the profile. This is a required column.
- `year::Symbol = :year`: The column name with the year of the profile. This is an optional column.
  If not present, all profiles are assumed to be from `default_year`.
- `default_year::Int = 2000`: The default year to use if the year column is not present
  in the profiles.
- `scenario::Symbol = :scenario`: The column name with the scenario of the profile. This is an optional column.
  If not present, all profiles are assumed to be from `default_scenario`.
- `default_scenario::Int = 1`: The default scenario to use if the scenario column is
  not present in the profiles.
- `cols_to_groupby::Vector{Symbol} = [:year]`: The column names to group by when
  performing clustering on groups of profiles separately. If empty, no grouping is done. Default is to group by `year`.

"""
Base.@kwdef struct ProfilesTableLayout
  value::Symbol = :value
  timestep::Symbol = :timestep
  period::Symbol = :period
  profile_name::Symbol = :profile_name
  year::Symbol = :year
  default_year::Int = 2000
  scenario::Symbol = :scenario
  default_scenario::Int = 1
  cols_to_groupby::Vector{Symbol} = [:year]
end

# Using `@kwdef` defines a default constructor based on keywords

function _read_model_parameters(path)
  if length(path) == 0
    throw(ArgumentError("Argument cannot be an empty string"))
  elseif !isfile(path)
    throw(ArgumentError("Path '$path' does not contain a file"))
  end

  file_data = TOML.parsefile(path)
  file_parameters = Dict(Symbol(k) => Symbol(v) for (k, v) in file_data)

  return file_parameters
end

function ProfilesTableLayout(path::String; kwargs...)
  file_parameters = _read_model_parameters(path)

  return ProfilesTableLayout(; file_parameters..., kwargs...)
end
