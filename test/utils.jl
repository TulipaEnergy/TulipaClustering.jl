function _new_connection(;
  profile_names = ["name001", "name002"],
  num_timesteps::Int = 24,
  database_schema = "",
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
      profile_name,
      i AS timestep,
      i * 3.14 AS value,
    FROM generate_series(1, $num_timesteps) AS s(i)
    CROSS JOIN (
      SELECT unnest([$profile_names_str]) AS profile_name,
    )
    ",
  )

  return connection
end
