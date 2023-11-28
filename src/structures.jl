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
