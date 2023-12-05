using CSV
using DataFrames
using LinearAlgebra
using SparseArrays
using TulipaClustering
using Distances
using Test

# Folders names
const INPUT_FOLDER = joinpath(@__DIR__, "inputs")
const OUTPUT_FOLDER = joinpath(@__DIR__, "outputs")

# Run all files in test folder starting with `test-`
for file in readdir(@__DIR__)
  if !startswith("test-")(file)
    continue
  end
  include(file)
end
