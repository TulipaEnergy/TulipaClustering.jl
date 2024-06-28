using CSV
using DataFrames
using Distances
using DuckDB
using LinearAlgebra
using SparseArrays
using Test
using TulipaClustering
using TulipaIO

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
