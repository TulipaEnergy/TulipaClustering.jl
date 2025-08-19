export ProfilesTableLayout

"""
    ProfilesTableLayout(;key = value, ...)
    ProfilesTableLayout(path; ...)

Structure to hold the profiles input data table layout.
Column names in the layout are defined by default.

If `path` is passed, it is expected to be a string pointing to a TOML file with
a `key = value` list of parameters. Explicit keyword arguments take precedence.

## Parameters

- `value::Symbol = :value`: The column name with the profile values.
- `timestep::Symbol = :timestep`: The column name with the time steps in the profile.
- `period::Symbol = :period`: The column name with the period number in the profile.
"""
Base.@kwdef struct ProfilesTableLayout
  value::Symbol = :value
  timestep::Symbol = :timestep
  period::Symbol = :period
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
