export find_representative_periods, reshape_clustering_data!

function split_into_periods!(df::AbstractDataFrame, period_duration::Union{Int, Nothing} = nothing)
  if period_duration === nothing
    # If period_duration is nothing, then leave the time steps as is and everything
    # is just the same period
    df.period .= 1
  else
    # Split the time step index using modular arithmetic starting with 1
    indices = fldmod1.(df.time_step, period_duration)  # find the new indices
    indices = reinterpret(reshape, Int, indices)       # change to an array for indexing

    df.period = indices[1, :]     # first row is the floor quotient, i.e., the period index
    df.time_step = indices[2, :]  # second row is the remainder, i.e., the new time step
  end
  select!(df, :period, :time_step, :)  # move the time-related columns
end

function reshape_clustering_data!(
  data::ClusteringData;
  period_duration::Union{Int, Nothing} = nothing,
)
  # Split the data into periods of equal length for clustering
  split_into_periods!(data.demand, period_duration)
  split_into_periods!(data.generation_availability, period_duration)
end

function _df_to_matrix(df::AbstractDataFrame)::Tuple{Matrix{Float64}, AbstractDataFrame}
  col_names = setdiff(names(df), ["period", "value"])
  result = unstack(df, col_names, :period, :value)
  result = result[!, map(x -> !any(ismissing, x), eachcol(result))]  # remove columns with missing values
  matrix = select(result, Not(col_names)) |> Matrix{Float64}
  removed_columns = select(result, col_names)
  return matrix, removed_columns
end

function find_representative_periods(
  data::ClusteringData;
  n_rp::Int = 10,
  rescale_demand_data::Bool = true,
  method::Symbol = :k_means,
  args...,
)
  if method === :k_means
    demand_matrix, demand_columns = _df_to_matrix(data.demand)
    if rescale_demand_data
      # Generation availability is on a scale from 0 to 1, but demand is not;
      # rescale the demand profiles by dividing them by the largest possible value,
      # and remember this value so that the demands can be computed back from the
      # normalized values later on.
      demand_scaling_factor = rescale_demand_data ? maximum(data.demand.value) : 1.0
      demand_matrix ./= demand_scaling_factor
    end

    generation_matrix, generation_columns = _df_to_matrix(data.generation_availability)

    clustering_matrix = vcat(demand_matrix, generation_matrix)
    kmeans_result = kmeans(clustering_matrix, n_rp; args...)
    centroids = kmeans_result.centers
    centroids = DataFrame(centroids, string.(1:n_rp))

    n_time_steps_global = select(data.demand, :period, :time_step) |> unique |> nrow
    n_time_steps_per_period = data.demand.time_step |> unique |> length
    n_demand, n_periods = size(demand_matrix)

    demand = hcat(demand_columns, centroids[1:n_demand, :])
    demand = stack(demand, variable_name = :rep_period)
    demand.rep_period = parse.(Int, demand.rep_period)
    select!(demand, :rep_period, :time_step, :)

    if rescale_demand_data
      demand.value .*= demand_scaling_factor
    end

    generation = hcat(generation_columns, centroids[(n_demand + 1):end, :])
    generation = stack(generation, variable_name = :rep_period)
    generation.rep_period = parse.(Int, generation.rep_period)
    select!(generation, :rep_period, :time_step, :)

    w = n_time_steps_global / (n_periods * n_time_steps_per_period)
    weights =
      [kmeans_result.assignments[t] == r ? w : 0.0 for t = 1:n_periods, r = 1:n_rp] |> Matrix{Float64}

    return ClusteringResult(demand, generation, weights)
  elseif method === :k_medoids
  elseif method === :hull
  else
  end
end
