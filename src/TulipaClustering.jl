module TulipaClustering

# Packages
using CSV
using Clustering
using DataFrames
using Distances
using DuckDB
using LinearAlgebra
using ProgressBars
using SparseArrays
using Statistics

include("structures.jl")
include("io.jl")
include("weight_fitting.jl")
include("cluster.jl")

end
