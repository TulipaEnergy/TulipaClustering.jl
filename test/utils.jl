function _new_connection(;
  years = [2030, 2050],
  profile_names = ["name001", "name002"],
  num_timesteps::Int = 24,
)
  @assert length(years) > 0
  @assert length(profile_names) > 0
  @assert num_timesteps ≥ 1
  connection = DBInterface.connect(DuckDB.DB)
  profile_names_str = join(["'$x'" for x in profile_names], ", ")
  DuckDB.query(connection, "CREATE SCHEMA input")
  DuckDB.query(
    connection,
    "CREATE TABLE input.profiles AS
    SELECT
      profile_name,
      unnest($years) AS year,
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
