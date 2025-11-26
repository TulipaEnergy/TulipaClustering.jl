export write_clustering_result_to_tables

"""
    weight_matrix_to_df(weights)

Converts a weight matrix from a (sparse) matrix, which is more convenient for
internal computations, to a dataframe, which is better for saving into a file.
Zero weights are dropped to avoid cluttering the dataframe.
"""
function weight_matrix_to_df(
    weights::Union{SparseMatrixCSC{Float64, Int64}, Matrix{Float64}},
)
    weights = sparse(weights)
    periods, rep_periods, values = weights |> findnz
    result = DataFrame(; period = periods, rep_period = rep_periods, weight = values)
    sort!(result, [:period, :rep_period])
    return result
end

"""
    write_clustering_result_to_tables(
      connection,
      clustering_result;
      database_schema="",
      layout=ProfilesTableLayout()
    )

Writes a [`TulipaClustering.ClusteringResult`](@ref) into DuckDB tables in `connection`.
"""
function write_clustering_result_to_tables(
    connection,
    clustering_result::TulipaClustering.ClusteringResult;
    database_schema = "",
    layout::ProfilesTableLayout = ProfilesTableLayout(),
)

    # Below we register the dataframes as `t_<name>` in the general schema,
    # because we can't create them directly into the correct schema.
    # Then, we create tables in the correct schema copying these tables.
    # Finally, we drop them.
    prefix = ""
    if database_schema != ""
        DBInterface.execute(connection, "CREATE SCHEMA IF NOT EXISTS $database_schema")
        prefix = "$database_schema."
    end

    # Create the profiles_rep_periods table
    DuckDB.register_data_frame(
        connection,
        clustering_result.profiles,
        "t_profiles_rep_periods",
    )

    # Create the rep_periods_mapping table
    mapping_df = weight_matrix_to_df(clustering_result.weight_matrix)
    DuckDB.register_data_frame(connection, mapping_df, "t_rep_periods_mapping")

    # Create the rep_periods_data table
    aux = clustering_result.auxiliary_data
    num_rep_periods = size(clustering_result.weight_matrix, 2)
    rep_period_duration = fill(aux.period_duration, num_rep_periods)
    rep_period_duration[end] = aux.last_period_duration
    rp_data_df = DataFrame(;
        rep_period = 1:num_rep_periods,
        num_timesteps = rep_period_duration,
        resolution = 1.0,
    )
    DuckDB.register_data_frame(connection, rp_data_df, "t_rep_periods_data")

    # Create the timeframe_data table
    num_periods = size(clustering_result.weight_matrix, 1)
    period_duration = fill(aux.period_duration, num_periods)
    period_duration[end] = aux.last_period_duration
    period_data_df = DataFrame(; period = 1:num_periods, num_timesteps = period_duration)
    DuckDB.register_data_frame(connection, period_data_df, "t_timeframe_data")

    for table_name in (
        "profiles_rep_periods",
        "rep_periods_mapping",
        "rep_periods_data",
        "timeframe_data",
    )
        DuckDB.query(
            connection,
            "CREATE OR REPLACE TABLE $(prefix)$table_name AS FROM t_$table_name",
        )
        DuckDB.query(connection, "DROP VIEW t_$table_name")
    end

    return nothing
end

"""
    write_clustering_result_to_tables(
    connection,
    results_per_group::Dict{
        DataFrames.GroupKey{GroupedDataFrame{DataFrame}},
        TulipaClustering.ClusteringResult,
    },
    metadata_per_group::Dict,
    n_rp::Int;
    database_schema = "",
    layout::ProfilesTableLayout = ProfilesTableLayout(),
)

Writes clustering results from different groups into DuckDB tables in `connection`.
The results from different groups are combined into single tables, adjusting
the representative period indices to ensure uniqueness across groups.
"""
function write_clustering_result_to_tables(
    connection,
    results_per_group::Dict{
        DataFrames.GroupKey{GroupedDataFrame{DataFrame}},
        TulipaClustering.ClusteringResult,
    },
    metadata_per_group::Dict,
    n_rp::Int;
    database_schema = "",
    layout::ProfilesTableLayout = ProfilesTableLayout(),
)

    # Below we register the dataframes as `t_<name>` in the general schema,
    # because we can't create them directly into the correct schema.
    # Then, we create tables in the correct schema copying these tables.
    # Finally, we drop them.
    prefix = ""
    if database_schema != ""
        DBInterface.execute(connection, "CREATE SCHEMA IF NOT EXISTS $database_schema")
        prefix = "$database_schema."
    end

    combined_profiles = _combine_group_profiles(results_per_group, n_rp)
    combined_weight_matrix = _combine_weight_matrices(
        results_per_group,
        metadata_per_group,
        n_rp;
        layout = layout,
    )
    combined_rep_periods_data =
        _combine_rep_periods_data(results_per_group, n_rp; layout = layout)
    combined_timeframe_data =
        _combine_timeframe_data(results_per_group, metadata_per_group; layout = layout)

    # Create the profiles_rep_periods table
    DuckDB.register_data_frame(connection, combined_profiles, "t_profiles_rep_periods")

    # Create the rep_periods_mapping table
    DuckDB.register_data_frame(connection, combined_weight_matrix, "t_rep_periods_mapping")

    # Create the rep_periods_data table
    DuckDB.register_data_frame(connection, combined_rep_periods_data, "t_rep_periods_data")

    # Create the timeframe_data table
    DuckDB.register_data_frame(connection, combined_timeframe_data, "t_timeframe_data")

    for table_name in (
        "profiles_rep_periods",
        "rep_periods_mapping",
        "rep_periods_data",
        "timeframe_data",
    )
        DuckDB.query(
            connection,
            "CREATE OR REPLACE TABLE $(prefix)$table_name AS FROM t_$table_name",
        )
        DuckDB.query(connection, "DROP VIEW t_$table_name")
    end

    return nothing
end

"""
A helper function to compute the rep_period offset for group indexing.
"""
function _rep_period_offset(num_rep_periods::Int, group_idx::Int)
    return num_rep_periods * (group_idx - 1)
end

"""
A function to offset representative period indices so that groups have disjoint rep_period ranges.
For group index g, new_rep_period = old_rep_period + offset given by _rep_period_offset(n_rp, g).
"""
function _combine_group_profiles(
    results::Dict{
        DataFrames.GroupKey{GroupedDataFrame{DataFrame}},
        TulipaClustering.ClusteringResult,
    },
    n_rp::Int,
)
    profile_dfs = DataFrame[]
    for (g, (_, group_result)) in enumerate(pairs(results))
        df = group_result.profiles === nothing ? DataFrame() : copy(group_result.profiles)
        if !isempty(df)
            df.rep_period .+= _rep_period_offset(n_rp, g)
        end
        push!(profile_dfs, df)
    end
    return isempty(profile_dfs) ? DataFrame() : vcat(profile_dfs...; cols = :union)
end

"""
A function to combine weight matrices from different groups.
For group index g, new_rep_period = old_rep_period + offset given by _rep_period_offset(n_rp, g).
In addition, the group key columns and cross column values are added to the resulting dataframe.
The period column is updated to reflect the original period within each cross group.
"""
function _combine_weight_matrices(
    results::Dict{
        DataFrames.GroupKey{GroupedDataFrame{DataFrame}},
        TulipaClustering.ClusteringResult,
    },
    metadata_per_group::Dict,
    n_rp::Int;
    layout = ProfilesTableLayout(),
)
    weight_matrices_dfs = Vector{DataFrame}(undef, length(results))

    for (g, (group_key, group_result)) in enumerate(pairs(results))
        df = weight_matrix_to_df(group_result.weight_matrix)
        df.rep_period .+= _rep_period_offset(n_rp, g)

        # Get metadata for this group
        num_periods = metadata_per_group[group_key].num_periods
        cross_values_list = metadata_per_group[group_key].cross_values_list

        # For each row, calculate which cross group it belongs to and original period
        df[!, :_cross_idx] = [div(p - 1, num_periods) + 1 for p in df.period]
        df[!, :_original_period] = [mod(p - 1, num_periods) + 1 for p in df.period]

        # Add cross column values
        for col in layout.cols_to_crossby
            df[!, col] = [cross_values_list[idx][col] for idx in df._cross_idx]
        end

        # Add group key columns
        for col in keys(group_key)
            col_value = group_key[col]
            insertcols!(df, 1, col => fill(col_value, nrow(df)))
        end

        # Update period to original values if needed
        df.period = df._original_period
        select!(df, Not([:_cross_idx, :_original_period]))

        weight_matrices_dfs[g] = df
    end

    # Filter out empty DataFrames for cleaner concatenation
    non_empty_dfs = filter(!isempty, weight_matrices_dfs)

    combined_df =
        isempty(non_empty_dfs) ? DataFrame() : vcat(non_empty_dfs...; cols = :union)

    return combined_df
end

"""
A function to combine rep_periods_data from different groups.
For group index g, new_rep_period = old_rep_period + offset given by _rep_period_offset(n_rp, g).
In addition, the group key columns are added to the resulting dataframe.
"""
function _combine_rep_periods_data(
    results::Dict{
        DataFrames.GroupKey{GroupedDataFrame{DataFrame}},
        TulipaClustering.ClusteringResult,
    },
    n_rp::Int;
    layout = ProfilesTableLayout(),
)
    rep_periods_data_dfs = Vector{DataFrame}(undef, length(results))

    for (g, (group_key, group_result)) in enumerate(pairs(results))
        aux = group_result.auxiliary_data
        rep_period_duration = fill(aux.period_duration, n_rp)
        rep_period_duration[end] = aux.last_period_duration
        first_rep_period = 1 + _rep_period_offset(n_rp, g)
        last_rep_period = n_rp + _rep_period_offset(n_rp, g)

        df = DataFrame(;
            rep_period = first_rep_period:last_rep_period,
            num_timesteps = rep_period_duration,
            resolution = 1.0,
        )

        for col in keys(group_key)
            col_value = group_key[col]
            insertcols!(df, 1, col => fill(col_value, nrow(df)))
        end

        rep_periods_data_dfs[g] = df
    end

    # Filter out empty DataFrames for cleaner concatenation
    non_empty_dfs = filter(!isempty, rep_periods_data_dfs)

    return isempty(non_empty_dfs) ? DataFrame() : vcat(non_empty_dfs...; cols = :union)
end

"""
A function to combine timeframe_data from different groups.
Creates timeframe data with period information for each group.
The year column (specified by layout.year) is extracted from either group keys or cross column metadata.
For each unique year value, a row is created for each period with its duration information.
"""
function _combine_timeframe_data(
    results::Dict{
        DataFrames.GroupKey{GroupedDataFrame{DataFrame}},
        TulipaClustering.ClusteringResult,
    },
    metadata_per_group::Dict,
    ;
    layout = ProfilesTableLayout(),
)
    timeframe_data_dfs = Vector{DataFrame}(undef, length(results))
    year_col = layout.year
    period_col = layout.period

    for (g, (group_key, group_result)) in enumerate(pairs(results))
        # Find year values from metadata - check group_key first, then cross_columns
        year_values = []
        if year_col in keys(group_key)
            push!(year_values, group_key[year_col])
        else
            # Collect all unique year values from cross columns
            cross_values_list = metadata_per_group[group_key].cross_values_list
            for cross_values in cross_values_list
                if year_col in keys(cross_values)
                    push!(year_values, cross_values[year_col])
                end
            end
        end
        year_values = unique(year_values)

        aux = group_result.auxiliary_data
        num_periods = metadata_per_group[group_key].num_periods
        period_duration = fill(aux.period_duration, num_periods)
        period_duration[end] = aux.last_period_duration

        # Create a row for each year value
        df_rows = DataFrame[]
        if isempty(year_values)
            # No grouping by year - create a simple timeframe without year column
            df_row =
                DataFrame(; period_col => 1:num_periods, num_timesteps = period_duration)
            push!(df_rows, df_row)
        else
            for year_value in year_values
                df_row = DataFrame(;
                    year_col => year_value,
                    period_col => 1:num_periods,
                    num_timesteps = period_duration,
                )
                push!(df_rows, df_row)
            end
        end

        df = isempty(df_rows) ? DataFrame() : vcat(df_rows...; cols = :union)

        timeframe_data_dfs[g] = df
    end

    # Filter out empty DataFrames for cleaner concatenation
    non_empty_dfs = filter(!isempty, timeframe_data_dfs)

    combined_df =
        isempty(non_empty_dfs) ? DataFrame() : vcat(non_empty_dfs...; cols = :union)

    return unique(combined_df)
end
