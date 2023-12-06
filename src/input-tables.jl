"""
Schema for the input `assets-profiles.csv` file.
"""
struct AssetProfiles
  asset::String               # Asset ID
  time_step::Int              # Time step ID
  value::Float64              # p.u. (per unit)
end
