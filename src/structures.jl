export ClusteringData

"""
Structure to hold the time series used in clustering together with some
summary statistics on the data.
"""
mutable struct TimeSeriesData
  data::AbstractDataFrame
  key_columns::Union{AbstractVector{Symbol}, Nothing}
  total_time_steps::Union{Int, Nothing}
  period_duration::Union{Int, Nothing}
  n_periods::Union{Int, Nothing}

  function TimeSeriesData(data)
    if columnindex(data, :time_step) == 0
      throw(DomainError(data, "DataFrame does not contain a column `time_step`"))
    end
    return new(data, nothing, nothing, nothing, nothing)
  end
end

"""
Structure to hold the data needed for clustering.
"""
mutable struct ClusteringData
  demand::TimeSeriesData
  generation_availability::TimeSeriesData

  function ClusteringData(demand, generation_availability)
    return new(TimeSeriesData(demand), TimeSeriesData(generation_availability))
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
