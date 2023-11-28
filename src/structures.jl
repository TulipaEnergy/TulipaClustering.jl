export ClusteringData

"""
Structure to hold all parts of an energy problem.
"""
mutable struct ClusteringData
  demand::AbstractDataFrame
  generation_availability::AbstractDataFrame

  function ClusteringData(demand, generation_availability)
    return new(demand, generation_availability)
  end
end
