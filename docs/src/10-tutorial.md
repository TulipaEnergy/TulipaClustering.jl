# Tutorial

## Explanation

To simplify, let's consider a single profile, for a single year.
Let's denote it as $p_i$, where $i = 1,\dots,N$.
The clustering process consists of:

1. Split `N` into (let's assume equal) _periods_ of size `m = period_duration`.
   We can rename $p_i$ as

   $$p_{j,k}, \qquad \text{where} \qquad j = 1,\dots,m, \quad k = 1,\dots,N/m.$$
2. Compute `num_rps` representative periods

   $$r_{j,\ell}, \qquad \text{where} \qquad j = 1,\dots,m, \qquad \ell = 1,\dots,\text{num\_rps}.$$
3. During computation of the representative periods, we obtained weight
   $w_{k,\ell}$ between the period $k$ and the representative period $\ell$,
   such that

   $$p_{j,k} = \sum_{\ell = 1}^{\text{num\_rps}} r_{j,\ell} \ w_{k,\ell}, \qquad \forall j = 1,\dots,m, \quad k = 1,\dots,N/m$$

## High level API/DuckDB API

!!! note "High level API"
    This tutorial focuses on the highest level of the API, which requires the
    use of a DuckDB connection.

The high-level API of TulipaClustering focuses on using TulipaClustering as part of the [Tulipa workflow](https://tulipaenergy.github.io/TulipaEnergyModel.jl/stable/).
This API consists of three main functions: [`transform_wide_to_long!`](@ref), [`cluster!`](@ref), and [`dummy_cluster!`](@ref).
In this tutorial we'll use all three.

Normally, you will have the DuckDB connection from the larger Tulipa workflow,
so here we will create a temporary connection with fake data to show an example
of the workflow. You can look into the source code of this documentation to see
how to create this fake data.

```@setup duckdb_example
using DuckDB

connection = DBInterface.connect(DuckDB.DB)
DuckDB.query(
  connection,
  "CREATE TABLE profiles_wide AS
  SELECT
      2030 AS year,
          i + 24 * (p - 1) AS timestep,
      4 + 0.3 * cos(4 * 3.14 * i / 24) + random() * 0.2 AS avail,
      solar_rand * greatest(0, (5 + random()) * cos(2 * 3.14 * (i - 12.5) / 24)) AS solar,
      3.6 + 3.6 * sin(3.14 * i / 24) ^ 2 * (1 + 0.3 * random()) AS demand,
  FROM
    generate_series(1, 24) AS _timestep(i)
  CROSS JOIN (
    SELECT p, RANDOM() AS solar_rand
    FROM generate_series(1, 7 * 4) AS _period(p)
  )
  ORDER BY timestep
  ",
)
```

Here is the content of that connection:

```@example duckdb_example
using DataFrames, DuckDB

nice_query(str) = DataFrame(DuckDB.query(connection, str))
nice_query("show tables")
```

And here is the first rows of `profiles_wide`:

```@example duckdb_example
nice_query("from profiles_wide limit 10")
```

And finally, this is the plot of the data:

```@example duckdb_example
using Plots

table = DuckDB.query(connection, "from profiles_wide")
plot(size=(800, 400))
timestep = [row.timestep for row in table]
for profile_name in (:avail, :solar, :demand)
    value = [row[profile_name] for row in table]
    plot!(timestep, value, lab=string(profile_name))
end
plot!()
```

## Transform a wide profiles table into a long table

!!! warning "Required"
    The long table format is a requirement of TulipaClustering, even for the dummy clustering example.

In this context, a wide table is a table where each new profile occupies a new column. A long table is a table where the profile names are stacked in a column with the corresponding values in a separate column.
Given the name of the source table (in this case, `profiles_wide`), we can create a long table with the following call:

```@example duckdb_example
using TulipaClustering

transform_wide_to_long!(connection, "profiles_wide", "profiles")

nice_query("FROM profiles LIMIT 10")
```

Here, we decided to save the long profiles table with the name `profiles` to use in the clustering below.

## Dummy Clustering

A dummy cluster will essentially ignore the clustering and create the necessary tables for the next steps in the Tulipa workflow.

```@example duckdb_example
for table_name in (
    "rep_periods_data",
    "rep_periods_mapping",
    "profiles_rep_periods",
    "timeframe_data",
)
    DuckDB.query(connection, "DROP TABLE IF EXISTS $table_name")
end

clusters = dummy_cluster!(connection)

nice_query("FROM rep_periods_data LIMIT 5")
```

```@example duckdb_example
nice_query("FROM rep_periods_mapping LIMIT 5")
```

```@example duckdb_example
nice_query("FROM profiles_rep_periods LIMIT 5")
```

```@example duckdb_example
nice_query("FROM timeframe_data LIMIT 5")
```

## Clustering

We can perform a real clustering by using the [`cluster!`](@ref) function with two extra arguments (see [Explanation](@ref) for their deeped meaning):

- `period_duration`: How long are the split periods;
- `num_rps`: How many representative periods.

```@example duckdb_example
period_duration = 24
num_rps = 3

for table_name in (
    "rep_periods_data",
    "rep_periods_mapping",
    "profiles_rep_periods",
    "timeframe_data",
)
    DuckDB.query(connection, "DROP TABLE IF EXISTS $table_name")
end

clusters = cluster!(connection, period_duration, num_rps)

nice_query("FROM rep_periods_data LIMIT 5")
```

```@example duckdb_example
nice_query("FROM rep_periods_mapping LIMIT 5")
```

```@example duckdb_example
nice_query("FROM profiles_rep_periods LIMIT 5")
```

```@example duckdb_example
nice_query("FROM timeframe_data LIMIT 5")
```
