function _new_connection(;
    year::Int = 1982,
    profile_names::Vector{String} = ["name001", "name002"],
    num_timesteps::Int = 24,
    database_schema = "",
    layout::TulipaClustering.ProfilesTableLayout = TulipaClustering.ProfilesTableLayout(),
)
    @assert length(profile_names) > 0
    @assert num_timesteps ≥ 1
    connection = DBInterface.connect(DuckDB.DB)
    profile_names_str = join(["'$x'" for x in profile_names], ", ")
    prefix = ""
    if database_schema != ""
        DuckDB.query(connection, "CREATE SCHEMA $database_schema")
        prefix = "$database_schema."
    end
    DuckDB.query(
        connection,
        "CREATE TABLE $(prefix)profiles AS
        SELECT
          $year AS $(layout.year),
          $(layout.profile_name) AS $(layout.profile_name),
          i AS $(layout.timestep),
          i * 3.14 AS $(layout.value),
        FROM generate_series(1, $num_timesteps) AS s(i)
        CROSS JOIN (
          SELECT unnest([$profile_names_str]) AS $(layout.profile_name),
        )
        ",
    )

    return connection
end

function _new_connection_multi_scenario_year(;
    profile_names::Vector{String} = ["name001", "name002"],
    num_timesteps::Int = 24,
    years::Vector{Int} = [2020, 2021],
    scenarios::Vector{Int} = [1, 2],
    database_schema = "",
    layout::TulipaClustering.ProfilesTableLayout = TulipaClustering.ProfilesTableLayout(),
)
    @assert length(profile_names) > 0
    @assert num_timesteps ≥ 1
    @assert length(years) > 0
    @assert length(scenarios) > 0

    connection = DBInterface.connect(DuckDB.DB)
    profile_names_str = join(["'$x'" for x in profile_names], ", ")
    years_str = join(years, ", ")
    scenarios_str = join(scenarios, ", ")

    prefix = ""
    if database_schema != ""
        DuckDB.query(connection, "CREATE SCHEMA $database_schema")
        prefix = "$database_schema."
    end

    DuckDB.query(
        connection,
        "CREATE TABLE $(prefix)profiles AS
        SELECT
          $(layout.year) AS $(layout.year),
          $(layout.scenario) AS $(layout.scenario),
          $(layout.profile_name) AS $(layout.profile_name),
          i AS $(layout.timestep),
          (i * 3.14 + $(layout.year) * 0.1 + $(layout.scenario) * 0.01) AS $(layout.value),
        FROM generate_series(1, $num_timesteps) AS s(i)
        CROSS JOIN (
          SELECT unnest([$profile_names_str]) AS $(layout.profile_name),
        )
        CROSS JOIN (
          SELECT unnest([$years_str]) AS $(layout.year),
        )
        CROSS JOIN (
          SELECT unnest([$scenarios_str]) AS $(layout.scenario),
        )
        ",
    )

    return connection
end
