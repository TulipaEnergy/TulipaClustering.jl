export ClusteringData

"""
Structure to hold the data needed for clustering.
"""
mutable struct ClusteringData
  demand::AbstractDataFrame
  generation_availability::AbstractDataFrame

  function ClusteringData(demand, generation_availability)
    return new(demand, generation_availability)
  end
end

"""
Structure to hold the time series used in clustering together with some
summary statistics on the data.
"""
mutable struct AuxiliaryClusteringData
  key_columns_demand::AbstractVector{Symbol}
  key_columns_generation_availability::AbstractVector{Symbol}
  period_duration::Int
  last_period_duration::Int
  n_periods::Int

  function AuxiliaryClusteringData(
    key_columns_demand,
    key_columns_generation_availability,
    period_duration,
    last_period_duration,
    n_periods,
  )
    return new(
      key_columns_demand,
      key_columns_generation_availability,
      period_duration,
      last_period_duration,
      n_periods,
    )
  end
end

"""
Structure to hold the clustering result.
"""
mutable struct ClusteringResult
  demand::AbstractDataFrame
  generation_availability::AbstractDataFrame
  weight_matrix::Matrix{Float64}

  function ClusteringResult(demand, generation_availability, weight_matrix)
    return new(demand, generation_availability, weight_matrix)
  end
end
