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
using TOML

include("structures.jl")
include("profiles-table-layout.jl")
include("data-validation.jl")
include("io.jl")
include("weight_fitting.jl")
include("cluster.jl")
include("convenience.jl")

end
