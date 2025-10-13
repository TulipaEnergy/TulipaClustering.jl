# Tutorial

```@contents
Pages = ["10-tutorial.md"]
Depth = [2, 3]
```

## Getting Started

### Input files

We use [DuckDB](https://duckdb.org/) as the database backend for the input data of the profiles. The input data must be in a long table format with at least the columns `year`, `profile_name`, `timestep`, and `value`. Extra columns such as `scenario` are allowed. These defaults values are configurable by passing a [`ProfilesTableLayout`](@ref) to [`cluster!`](@ref). See the [Using a Custom Layout](@ref custom_layout) section for an example of how to use a custom layout.

For this tutorial we will use the [profiles file](https://github.com/TulipaEnergy/TulipaClustering.jl/blob/main/test/inputs/EU/profiles.csv) available in the TulipaClustering repository.

In this example the profiles are in a CSV file, but you can also load them from other sources (e.g., Parquet, Excel, etc.) using DuckDB's [readers](https://duckdb.org/docs/stable/data/data_sources).

```@example tutorial
using DataFrames, DuckDB

# change the following path to your file location
profiles_file = joinpath(@__DIR__,"../../test/inputs/EU/profiles.csv")

connection = DBInterface.connect(DuckDB.DB)
DuckDB.query(
  connection,
  """
  CREATE TABLE profiles AS
  SELECT * FROM read_csv('$profiles_file');
  """,
)

# helper function to query the DuckDB tables in the connection
nice_query(str) = DuckDB.query(connection, str) |> DataFrame

# show the tables in the connection using the helper function
nice_query("SHOW tables")
```

And here we can have a look at the first rows of `profiles`:

```@example tutorial
nice_query("FROM profiles LIMIT 10")
```

Let's explore the first 10 unique profile names in the `profiles` table:

```@example tutorial
nice_query("""
    SELECT DISTINCT profile_name
    FROM profiles
    ORDER BY profile_name
    LIMIT 10
""")
```

And finally, we can use `nice_query` to filter the profiles and plot them. For example, here we filter and plot the profiles in the Netherlands (i.e., those starting with `NED_`) and plot only a sample with the profiles for the first week of the year:

```@example tutorial
using Plots

df = nice_query("""
    SELECT *
    FROM profiles
    WHERE profile_name LIKE 'NED_%'
    ORDER BY profile_name, timestep
""")

sample = 1:168
plot(size=(800, 400))
for group in groupby(df, :profile_name)
    name = group.profile_name[1]
    plot!(group.timestep[sample], group.value[sample], label=name)
end
plot!(xlabel="Timestep", ylabel="Value", title="Profiles in the Netherlands")
```

### Clustering

We can perform the clustering by using the [`cluster!`](@ref) function by passing the connection with the profiles table and two extra arguments (see [Concepts](@ref concepts) for their deeped meaning):

- `period_duration`: How long are the periods (e.g., 24 for daily periods if the timestep is hourly);
- `num_rps`: How many representative periods.

In this example we use the function [`cluster!`](@ref) with its default parameters. Section [Hull Clustering with Blended Representative Periods](@ref hull_clustering) explains the extra parameters that can be passed to the clustering function.

Finally, it will create new output tables in the DuckDB connection that we will explore in the next section.

After the clustering, four tables will be created in the DuckDB connection:

```@example tutorial
using TulipaClustering

period_duration = 24
num_rps = 4

for table_name in (          # hide
    "rep_periods_data",      # hide
    "rep_periods_mapping",   # hide
    "profiles_rep_periods",  # hide
    "timeframe_data",        # hide
)                            # hide
    DuckDB.query(connection, "DROP TABLE IF EXISTS $table_name") # hide
end                          # hide

clusters = cluster!(connection, period_duration, num_rps)

nice_query("SHOW tables")
```

### Output Tables

The output tables are:

- `profiles_rep_periods` contains the profiles for each RP,

```@example tutorial
nice_query("FROM profiles_rep_periods LIMIT 5")
```

- `rep_periods_data` contains the general informations of the RPs,

```@example tutorial
nice_query("FROM rep_periods_data")
```

- `rep_periods_mapping` containts the weights that are use to map the RPs to the original (or base) periods,

```@example tutorial
nice_query("FROM rep_periods_mapping LIMIT 5")
```

- `timeframe_data` contains information about the original (or base) periods

```@example tutorial
nice_query("FROM timeframe_data LIMIT 5")
```

You can use DuckDB to explore the results using SQL queries or export them to CSV or Parquet files using DuckDB's [writers](https://duckdb.org/docs/stable/guides/file_formats/overview).

For example, we can plot again the profiles in the Netherlands, but this time using the clustered profiles:

```@example tutorial
df = nice_query("""
    SELECT *
    FROM profiles_rep_periods
    WHERE profile_name LIKE 'NED_%'
    ORDER BY profile_name, timestep
""")
rep_periods = unique(df.rep_period)
plots = []

for rp in rep_periods
    df_rp = filter(row -> row.rep_period == rp, df)
    p = plot(size=(400, 300), title="Representative Period $rp")

    for group in groupby(df_rp, :profile_name)
        name = group.profile_name[1]
        plot!(p, group.timestep, group.value, label=name)
    end

    show_legend = (rp == rep_periods[1])
    plot!(p,
          xlabel="Timestep",
          ylabel="Value",
          xticks=0:2:period_duration,
          xlim=(1, period_duration),
          ylim=(0, 1),
          legend=show_legend ? :bottomleft : false,
          legendfontsize=6
         )
    push!(plots, p)
end

plot(plots..., layout=(2, 2), size=(800, 600))
```

🎉 Congratulations! You have successfully finished the first part of the tutorial. Now you can continue with more concepts and options in the following sections 😉

## [Hull Clustering with Blended Representative Periods](@id hull_clustering)

The function [`cluster!`](@ref) has several keyword arguments that can be used to customize the clustering process. Alternatively, you can use the help mode in Julia REPL by typing `?cluster!` to see all the available keyword arguments and their descriptions. Here is a summary of the most important keyword arguments for this tutorial:

- `method` (default `:k_medoids`): clustering method to use `:k_means`, `:k_medoids`, `:convex_hull`, `:convex_hull_with_null`, or `:conical_hull`.
- `distance` (default `Distances.Euclidean()`): semimetric used to measure distance between data points from the the package Distances.jl.
- `weight_type` (default `:dirac`): the type of weights to find; possible values are:
  - `:dirac`: each period is represented by exactly one representative
    period (a one unit weight and the rest are zeros)
  - `:convex`: each period is represented as a convex sum of the
    representative periods (a sum with nonnegative weights adding into one)
  - `:conical`: each period is represented as a conical sum of the
    representative periods (a sum with nonnegative weights)
  - `:conical_bounded`: each period is represented as a conical sum of the
    representative periods (a sum with nonnegative weights) with the total
    weight bounded from above by one.

As you can see, there are several keyword arguments that can be combined to explore different clustering strategies. Our proposed method is the Hull Clustering with Blended Representative Periods, which can be activated by setting the following keyword arguments:

- `method = :convex_hull`
- `distance = Distances.CosineDist()`
- `weight_type = :convex`

You can read more about the proposed method in the [Concepts](@ref concepts) section.

So, let's cluster again using the proposed method:

```@example tutorial
for table_name in (          # hide
    "rep_periods_data",      # hide
    "rep_periods_mapping",   # hide
    "profiles_rep_periods",  # hide
    "timeframe_data",        # hide
)                            # hide
    DuckDB.query(connection, "DROP TABLE IF EXISTS $table_name") # hide
end                          # hide
using Distances
clusters = cluster!(connection,
                    period_duration,
                    num_rps;
                    method = :convex_hull,
                    distance = Distances.CosineDist(),
                    weight_type = :convex
                    )

nice_query("SHOW tables")
```

As you can see, the output tables names are the same as before, but the results will be different. You can explore the results again using SQL queries or export them to CSV or Parquet files using DuckDB's [writers](https://duckdb.org/docs/stable/guides/file_formats/overview).

Let's plot again the profiles in the Netherlands, but this time using the clustered profiles with the hull clustering method:

```@example tutorial
df = nice_query("""
    SELECT *
    FROM profiles_rep_periods
    WHERE profile_name LIKE 'NED_%'
    ORDER BY profile_name, timestep
""")
rep_periods = unique(df.rep_period)
plots = []

for rp in rep_periods
    df_rp = filter(row -> row.rep_period == rp, df)
    p = plot(size=(400, 300), title="Hull Clustering RP $rp")

    for group in groupby(df_rp, :profile_name)
        name = group.profile_name[1]
        plot!(p, group.timestep, group.value, label=name)
    end

    show_legend = (rp == rep_periods[1])
    plot!(p,
          xlabel="Timestep",
          ylabel="Value",
          xticks=0:2:period_duration,
          xlim=(1, period_duration),
          ylim=(0, 1),
          legend=show_legend ? :topleft : false,
          legendfontsize=6
         )
    push!(plots, p)
end

plot(plots..., layout=(2, 2), size=(800, 600))
```

The first difference you may notice is that the representative periods (RPs) obtained with hull clustering are more extreme than those obtained with the default method. This is because hull clustering selects RPs that are more likely to be constraint-binding in an optimization model.

!!! tip "The Projected gradient descent parameters"
    The parameters `niters` and `learning_rate` tell for how many iterations to run the descent and by how much to adjust the weights in each iterations. More iterations make the method slower but produce better results. Larger learning rate makes the method converge faster but in a less stable manner (i.e., weights might start going up and down a lot from iteration to iteration). Sometimes you need to find the right balance for yourself. In general, if the weights produced by the method look strange, try decreasing the learning rate and/or increasing the number of iterations.

For more details on the comparison of clustering methods please refer to the [Scientific References](@ref scientific-refs) section.

## Clustering by other columns

`TulipaClustering.jl` clusters by default using the columns `year`, i.e., it will create representative periods for each year in the input data. The total number of representative periods will be `num_rps * number_of_years`. This is useful when the profiles have a strong seasonal component that changes from year to year.

However, sometimes the user might want to cluster by other columns, e.g., `scenario` or `region`, or even by multiple columns, e.g., `year` and `scenario`. The package allows to cluster by different columns by passing a custom [`ProfilesTableLayout`](@ref) to [`cluster!`](@ref).

!!! warning "Required"
    The `cols_to_groupby` argument in [`ProfilesTableLayout`](@ref) is a vector of symbols, i.e., `cols_to_groupby = [:year, :scenario]`.

!!! note "The number of representative periods"
    When clustering by multiple columns, the total number of representative periods will be `num_rps * number_of_unique_combinations_of_groupby_columns`. For example, if the input data has 3 unique years and 2 unique scenarios, and the user wants to cluster by `year` and `scenario`, then the total number of representative periods will be `num_rps * 3 * 2 = num_rps * 6`.

## [Using a Custom Layout](@id custom_layout)

Let's say that you have a table that uses different names for the columns of your data.
For example, let's rename the column `timestep` to `hour` in the profiles table.

```@example tutorial
DuckDB.query(
  connection,
  "ALTER TABLE profiles
   RENAME COLUMN timestep to hour;
  ",
)

nice_query("FROM profiles LIMIT 10")
```

In this case, you can use the custom column name by passing a [`ProfilesTableLayout`](@ref) to [`cluster!`](@ref).

The layout names will also be preserved in the output tables. Below we cluster again, but ask passing the information to use `hour` instead of the default `timestep`:

```@example tutorial
for table_name in (          # hide
    "rep_periods_data",      # hide
    "rep_periods_mapping",   # hide
    "profiles_rep_periods",  # hide
    "timeframe_data",        # hide
)                            # hide
    DuckDB.query(connection, "DROP TABLE IF EXISTS $table_name") # hide
end                          # hide

layout = TulipaClustering.ProfilesTableLayout(; timestep = :hour)
clusters = cluster!(connection, period_duration, num_rps; layout)

nice_query("FROM profiles_rep_periods LIMIT 10")
```

Notice the column `hour` in the output above (instead of `timestep`).

## Extra Functions in High level API/DuckDB API

The high-level API of TulipaClustering focuses on using TulipaClustering as part of the [Tulipa workflow](https://tulipaenergy.github.io/TulipaEnergyModel.jl/stable/).
This API consists of three main functions: [`cluster!`](@ref), [`transform_wide_to_long!`](@ref), and [`dummy_cluster!`](@ref). These functions are designed to work with DuckDB connections and tables, making it easy to integrate clustering into your data processing pipeline. In the previous sections, we have already covered the usage of [`cluster!`](@ref) and [`transform_wide_to_long!`](@ref). In this section, we will explore the third function, [`dummy_cluster!`](@ref), which is useful for testing, debugging, and to prepare the data for TulipaEnergyModel without actually clustering the profiles.

### Dummy Clustering

A dummy cluster will essentially ignore the clustering, but it will create the necessary tables that are often used for the next steps in the [Tulipa workflow](https://tulipaenergy.github.io/TulipaEnergyModel.jl/stable/).

```@example tutorial
for table_name in (          # hide
    "rep_periods_data",      # hide
    "rep_periods_mapping",   # hide
    "profiles_rep_periods",  # hide
    "timeframe_data",        # hide
)                            # hide
    DuckDB.query(connection, "DROP TABLE IF EXISTS $table_name") # hide
end                          # hide

clusters = dummy_cluster!(connection; layout)

nice_query("FROM rep_periods_data LIMIT 10")
```

In this case the function created a single representative period for all the data.

Notice that we passed the `layout` argument to [`dummy_cluster!`](@ref) to ensure that the output tables have the correct column names, since we renamed the `timestep` column to `hour` in the previous section.

### Transform a wide profiles table into a long table

!!! warning "Required"
    The long table format is a requirement of TulipaClustering, even for the dummy clustering example.

A long table is a table where the profile names are stacked in a column with the corresponding values in a separate column.
However, sometimes the input data is in a wide format, i.e., each profile is in a separate column.

In those cases, you can use the function [`transform_wide_to_long!`](@ref) to transform a wide table into a long table. You need to provide the connection to DuckDB, the name of the source table (the wide table) and the name of the target table (the long table that will be created).

## Low level API

The [`cluster!`](@ref) function is a wrapper around the low-level clustering functions. It simplifies the process of clustering by handling the creation of temporary tables and managing the clustering workflow.

However, if you want to have more control over the clustering process, you can use the low-level functions directly. The low-level API consists of the following functions:

- [`split_into_periods!`](@ref): Splits the profiles into periods based on the specified period duration.
- [`find_representative_periods`](@ref): Finds the representative periods using the specified clustering method.
- [`fit_rep_period_weights!`](@ref): Fits the weights for the representative periods to map them to the original periods.

At the end of the clustering process, you will get a [`TulipaClustering.ClusteringResult`](@ref) struct that contains the detailed results of the clustering process:

- `profiles` is a dataframe with profiles for RPs,
- `weight_matrix` is a matrix of weights of RPs in blended periods,
- `clustering_matrix` and `rp_matrix` are matrices of profile data for each base and representative period (useful to keep for the next step, but you should not need these unless you want to do some extra math here)
- `auxiliary_data` contains some extra data that was generated during the clustering process and is generally not interesting to the user who is not planning to interact with the clustering method on a very low level. For example, if you use the `k-medoids` method, the `auxiliary_data` will contain the indices of the medoids in the original data.

So, although we recommend using the high-level API for most use cases, you can use the low-level functions if you need more control over the clustering process.
