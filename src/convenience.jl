export cluster!, dummy_cluster!, transform_wide_to_long!

"""
    cluster!(
        connection,
        period_duration,
        num_rps;
        input_database_schema = "",
        input_profile_table_name = "profiles",
        database_schema = "",
        drop_incomplete_last_period::Bool = false,
        method::Symbol = :k_means,
        distance::SemiMetric = SqEuclidean(),
        initial_representatives::AbstractDataFrame = DataFrame(),
        layout::ProfilesTableLayout = ProfilesTableLayout(),
        weight_type::Symbol = :convex,
        tol::Float64 = 1e-2,
        clustering_kwargs = Dict(),
        weight_fitting_kwargs = Dict(),
    )

Convenience function to cluster the table named in `input_profile_table_name`
using `period_duration` and `num_rps`. The resulting tables
`profiles_rep_periods`, `rep_periods_mapping`, and
`rep_periods_data` are loaded into `connection` in the `database_schema`, if
given, and enriched with `year` information.

This function extracts the table (expecting columns `profile_name`, `timestep`, `value`),
then calls [`split_into_periods!`](@ref),
[`find_representative_periods`](@ref), [`fit_rep_period_weights!`](@ref), and
finally `write_clustering_result_to_tables`.

## Arguments

**Required**

- `connection`: DuckDB connection
- `period_duration`: Duration of each period, i.e., number of `timestep`s.
- `num_rps`: Number of find_representative_periods

**Keyword arguments**

- `input_database_schema` (default `""`): Schema of the input tables
- `input_profile_table_name` (default `"profiles"`): Default name of the `profiles` table inside the above schemaa
- `database_schema` (default `""`): Schema of the output tables
- `drop_incomplete_last_period` (default `false`): controls how the last period is treated if it
  is not complete: if this parameter is set to `true`, the incomplete period
  is dropped and the weights are rescaled accordingly; otherwise, clustering
  is done for `n_rp - 1` periods, and the last period is added as a special
  shorter representative period
- `method` (default `:k_means``): clustering method to use, either `:k_means` and `:k_medoids`
- `distance` (default `Distances.SqEuclidean()`): semimetric used to measure distance between data points.
- `initial_representatives` initial representatives that should be
    included in the clustering. The period column in the initial representatives
    should be 1-indexed and the key columns should be the same as in the clustering data.
    For the hull methods it will be added before clustering, for :k_means and :k_medoids
    it will be added after clustering.
- `layout` (default `ProfilesTableLayout()`): describes the column names for `period`,
  `timestep`, and `value` in in-memory DataFrames. It does not change the SQL input
  table schema, which must contain `profile_name`, `timestep`, and `value`. Weight
  fitting operates on matrices and does not use `layout`.
- `weight_type` (default `:convex`): the type of weights to find; possible values are:
    - `:convex`: each period is represented as a convex sum of the
      representative periods (a sum with nonnegative weights adding into one)
    - `:conical`: each period is represented as a conical sum of the
      representative periods (a sum with nonnegative weights)
    - `:conical_bounded`: each period is represented as a conical sum of the
      representative periods (a sum with nonnegative weights) with the total
      weight bounded from above by one.
- `tol` (default `1e-2`): algorithm's tolerance; when the weights are adjusted by a value less
  then or equal to `tol`, they stop being fitted further.
- `clustering_kwargs` (default `Dict()`): Extra keyword arguments passed to [`find_representative_periods`](@ref)
- `weight_fitting_kwargs` (default `Dict()`): Extra keyword arguments passed to [`fit_rep_period_weights!`](@ref)
  (e.g., `niters`, `learning_rate`, `adaptive_grad`).
"""
function cluster!(
    connection,
    period_duration,
    num_rps;
    input_database_schema = "",
    input_profile_table_name = "profiles",
    database_schema = "",
    drop_incomplete_last_period::Bool = false,
    method::Symbol = :k_means,
    distance::SemiMetric = SqEuclidean(),
    initial_representatives::AbstractDataFrame = DataFrame(),
    layout::ProfilesTableLayout = ProfilesTableLayout(),
    weight_type::Symbol = :convex,
    tol::Float64 = 1e-2,
    clustering_kwargs = Dict(),
    weight_fitting_kwargs = Dict(),
)
    validate_data!(
        connection;
        input_database_schema,
        table_names = Dict("profiles" => input_profile_table_name),
        layout,
        initial_representatives,
    )
    _check_layout_consistency_with_cols_to_groupby(layout)

    if input_database_schema != ""
        input_profile_table_name = "$input_database_schema.$input_profile_table_name"
    end

    profiles =
        DuckDB.query(
            connection,
            "SELECT * FROM $input_profile_table_name
            ",
        ) |> DataFrame

    split_into_periods!(profiles; period_duration, layout)
    grouped_profiles_data = groupby(profiles, layout.cols_to_groupby)
    results_per_group = Dict(
        group_key => find_representative_periods(
            group,
            num_rps;
            drop_incomplete_last_period,
            method,
            distance,
            initial_representatives = _get_initial_representatives_for_group(
                initial_representatives,
                group_key,
            ),
            layout,
            clustering_kwargs...,
        ) for (group_key, group) in pairs(grouped_profiles_data)
    )
    for clustering_result in values(results_per_group)
        fit_rep_period_weights!(
            clustering_result;
            weight_type,
            tol,
            weight_fitting_kwargs...,
        )
    end
    write_clustering_result_to_tables(
        connection,
        results_per_group,
        num_rps;
        database_schema,
        layout,
    )

    return results_per_group
end

"""
    dummy_cluster!(connection)

Convenience function to create the necessary columns and tables when clustering
is not required.

This is essentially creating a single representative period with the size of
the whole profile.
See [`cluster!`](@ref) for more details of what is created.
"""
function dummy_cluster!(
    connection;
    input_database_schema = "",
    input_profile_table_name = "profiles",
    kwargs...,
)
    table_name = if input_database_schema != ""
        "$input_database_schema.$input_profile_table_name"
    else
        input_profile_table_name
    end
    period_duration = only([
        row.max_timestep for row in DuckDB.query(
            connection,
            "SELECT MAX(timestep) AS max_timestep FROM $table_name",
        )
    ])
    cluster!(connection, period_duration, 1; kwargs...)
end

"""
    transform_wide_to_long!(
        connection,
        wide_table_name,
        long_table_name;
    )

Convenience function to convert a table in wide format to long format using DuckDB.
Originally aimed at converting a profile table like the following:

| year | timestep | name1 | name2 | ⋯  | nameN |
| ---- | -------- | ----- | ----- | -- | ----- |
| 2030 |        1 |   1.0 |   2.5 | ⋯  |   0.0 |
| 2030 |        2 |   1.5 |   2.6 | ⋯  |   0.0 |
| 2030 |        3 |   2.0 |   2.6 | ⋯  |   0.0 |

To a table like the following:

| year | timestep | profile_name | value |
| ---- | -------- | ------------ | ----- |
| 2030 |        1 |        name1 |   1.0 |
| 2030 |        2 |        name1 |   1.5 |
| 2030 |        3 |        name1 |   2.0 |
| 2030 |        1 |        name2 |   2.5 |
| 2030 |        2 |        name2 |   2.6 |
| 2030 |        3 |        name2 |   2.6 |
|    ⋮ |        ⋮ |            ⋮ |     ⋮ |
| 2030 |        1 |        nameN |   0.0 |
| 2030 |        2 |        nameN |   0.0 |
| 2030 |        3 |        nameN |   0.0 |

This conversion is done using the `UNPIVOT` SQL command from DuckDB.

## Keyword arguments

- `exclude_columns = ["year", "timestep"]`: Which tables to exclude from the conversion.
  Note that if you have more columns that you want to exclude from the wide table, e.g., `scenario`,
  you can add them to this list, e.g., `["scenario", "year", "timestep"]`.
- `name_column = "profile_name"`: Name of the new column that contains the names of the old columns
- `value_column = "value"`: Name of the new column that holds the values from the old columns
"""
function transform_wide_to_long!(
    connection,
    wide_table_name,
    long_table_name;
    exclude_columns = ["year", "timestep"],
    name_column = "profile_name",
    value_column = "value",
)
    @assert length(exclude_columns) > 0
    exclude_str = join(exclude_columns, ", ")
    DuckDB.query(
        connection,
        "CREATE OR REPLACE TABLE $long_table_name AS
        UNPIVOT $wide_table_name
        ON COLUMNS(* EXCLUDE ($exclude_str))
        INTO
            NAME $name_column
            VALUE $value_column
        ORDER BY $name_column, $exclude_str
        ",
    )

    return
end

function _check_layout_consistency_with_cols_to_groupby(layout::ProfilesTableLayout)
    all_fields = fieldnames(ProfilesTableLayout)
    layout_fields = [getfield(layout, field) for field in all_fields]
    for col in layout.cols_to_groupby
        if !(col in layout_fields)
            throw(
                ArgumentError(
                    "Column '$col' in 'cols_to_groupby' is not defined in the layout",
                ),
            )
        end
    end
    return nothing
end

function _get_initial_representatives_for_group(
    initial_representatives::AbstractDataFrame,
    group_key::DataFrames.GroupKey{GroupedDataFrame{DataFrame}},
)
    # Return empty DataFrame if no initial representatives provided
    if isempty(initial_representatives)
        return DataFrame()
    end

    # Start with all rows as potential matches
    num_rows = nrow(initial_representatives)
    rows_matching_group = trues(num_rows)

    # For each grouping column, filter to rows that match the group's value
    for column_name in keys(group_key)
        group_value = group_key[column_name]
        column_values = initial_representatives[!, column_name]
        column_matches = column_values .== group_value

        # Keep only rows that match this column AND all previous columns
        rows_matching_group .&= column_matches
    end

    # Return the subset of initial representatives that belong to this group
    return initial_representatives[rows_matching_group, :]
end
