"""
Schema for the input demand.csv file.
"""
struct DemandData
  node::String    # Name of the node
  time_step::Int  # Time step ID
  value::Float64  # MW
end

"""
Schema for the input generation-availability.csv file.
"""
struct GenerationAvailabilityData
  node::String        # Name of the node
  technology::String  # Name of the generation technology
  time_step::Int      # Time step ID
  value::Float64      # Relative availability, between 0 and 1
end
