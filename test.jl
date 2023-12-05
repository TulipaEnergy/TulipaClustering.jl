using DataFrames
using TulipaClustering
using SparseArrays
using Distances

dir = INPUT_FOLDER = joinpath(@__DIR__, "test/inputs/EU")
clustering_data = TulipaClustering.read_clustering_data_from_csv_folder(dir)
TulipaClustering.reshape_clustering_data!(clustering_data; period_duration = 24 * 7)
rescale_demand_data = true
drop_incomplete_periods = true
distance = SqEuclidean()

n_rp = 10
method = :k_means

res = find_representative_periods(
  clustering_data;
  n_rp,
  rescale_demand_data,
  drop_incomplete_periods,
  method,
  distance,
)

# Check that the time series use the same periods and time steps
n_periods = clustering_data.demand.n_periods
period_duration = clustering_data.demand.period_duration
total_time_steps = clustering_data.demand.total_time_steps
if clustering_data.generation_availability.n_periods != n_periods ||
   clustering_data.generation_availability.period_duration != period_duration ||
   clustering_data.generation_availability.total_time_steps != total_time_steps
  throw(ArgumentError("Time series for demand and generation must have the same time steps"))
end

# Check if there are any incomplete periods that are not the last period
demand_df = TulipaClustering.get_df_without_key_columns(clustering_data.demand)
generation_df = TulipaClustering.get_df_without_key_columns(clustering_data.generation_availability)

incomplete_periods =
  TulipaClustering.find_incomplete_columns(demand_df) ∪
  TulipaClustering.find_incomplete_columns(generation_df)

n_incomplete_periods = length(incomplete_periods)
has_incomplete_periods = n_incomplete_periods > 0
if has_incomplete_periods && n_incomplete_periods > 1
  throw(DomainError(incomplete_periods, "Multiple periods have missing data"))
end

n_complete_periods = n_periods - n_incomplete_periods

# Find the period weights
complete_period_weight =
  has_incomplete_periods && discard_incomplete_periods ?
  total_time_steps / (n_complete_periods * period_duration) : 1.0
incomplete_period_weight =
  (total_time_steps - n_complete_periods * period_duration) / period_duration

# df = select(clustering_data.demand.data, clustering_data.demand.key_columns, incomplete_periods)
# #df = clustering_data.demand.data
# df = stack(df, variable_name = :period) |> dropmissing
# df = combine(groupby(df, :period), :time_step => maximum => :weight)
# df.weight .= df.weight ./ period_duration

#incomplete_period_mapping = [incomplete_periods .=> string.((n_rp - n_incomplete_periods + 1):n_rp)]
# Deal with the incomplete periods
#weights = spzeros(n_periods, n_rp)

# Compute the clustering matrix by concatenating the data matrices
demand_matrix = select(demand_df, Not(incomplete_periods)) |> dropmissing |> Matrix{Float64}

if rescale_demand_data
  # Generation availability is on a scale from 0 to 1, but demand is not;
  # rescale the demand profiles by dividing them by the largest possible value,
  # and remember this value so that the demands can be computed back from the
  # normalized values later on.
  demand_scaling_factor = maximum(demand_matrix[map(!ismissing, demand_matrix)])
  demand_matrix ./= demand_scaling_factor
end

generation_matrix = select(generation_df, Not(incomplete_periods)) |> dropmissing |> Matrix{Float64}
clustering_matrix = vcat(demand_matrix, generation_matrix)

needs_discarding = n_incomplete_periods > 0 && !discard_incomplete_periods
if needs_discarding
  rp_mapping = [incomplete_periods .=> string.((n_rp - n_incomplete_periods + 1):n_rp)]
  n_rp -= n_incomplete_periods  # incomplete periods become their own representatives
end

# Do the clustering
kmeans_result = kmeans(clustering_matrix, n_rp)

# Reinterpret the results
centroids = kmeans_result.centers
centroids = DataFrame(centroids, string.(1:n_rp))
if needs_discarding
  n_rp += n_incomplete_periods
end
n_demand = size(demand_matrix)[1]

demand_columns = select(clustering_data.demand.data, clustering_data.demand.key_columns)
demand_centroids = centroids[1:n_demand, :]
if rescale_demand_data
  demand_centroids .*= demand_scaling_factor
end
if needs_discarding
  demand_centroids = hcat(
    demand_centroids,
    select(demand_df, incomplete_periods .=> string.((n_rp - n_incomplete_periods + 1):n_rp)),
  )
end
demand = _matrix_to_df(demand_columns, demand_centroids)

generation_columns = select(
  clustering_data.generation_availability.data,
  clustering_data.generation_availability.key_columns,
)
generation_centroids = centroids[(n_demand + 1):end, :]
if needs_discarding
  generation_centroids = hcat(
    generation_centroids,
    select(generation_df, incomplete_periods .=> string.((n_rp - n_incomplete_periods + 1):n_rp)),
  )
end
generation = _matrix_to_df(generation_columns, generation_centroids)

if needs_discarding
  w = 1.0
  weights =
    [
      t < n_periods && kmeans_result.assignments[t] == r ? w : 0.0 for t = 1:n_periods, r = 1:n_rp
    ] |> Matrix{Float64}
  # TODO: add a zero column and row to weights
  weights[n_periods, n_rp] =
    (total_time_steps - n_complete_periods * period_duration) / period_duration
else
  w = total_time_steps / (n_complete_periods * period_duration)
  weights =
    [kmeans_result.assignments[t] == r ? w : 0.0 for t = 1:n_complete_periods, r = 1:n_rp] |> Matrix{Float64}
end
