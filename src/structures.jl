"""
Structure to hold the time series used in clustering together with some
summary statistics on the data.
"""
mutable struct AuxiliaryClusteringData
  key_columns::AbstractVector{Symbol}
  period_duration::Int
  last_period_duration::Int
  n_periods::Int

  function AuxiliaryClusteringData(key_columns, period_duration, last_period_duration, n_periods)
    return new(key_columns, period_duration, last_period_duration, n_periods)
  end
end

"""
Structure to hold the clustering result.
"""
mutable struct ClusteringResult
  profiles::AbstractDataFrame
  weight_matrix::Union{SparseMatrixCSC{Float64, Int64}, Matrix{Float64}}
  clustering_matrix::Union{Matrix{Float64}, Nothing}
  rp_matrix::Union{Matrix{Float64}, Nothing}
  auxiliary_data::Union{AuxiliaryClusteringData, Nothing}

  function ClusteringResult(profiles, weight_matrix, clustering_matrix, rp_matrix, auxiliary_data)
    return new(profiles, weight_matrix, clustering_matrix, rp_matrix, auxiliary_data)
  end

  function ClusteringResult(profiles, weight_matrix)
    return new(profiles, weight_matrix, nothing, nothing, nothing)
  end
end
