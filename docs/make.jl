using TulipaClustering
using Documenter

DocMeta.setdocmeta!(
    TulipaClustering,
    :DocTestSetup,
    :(using TulipaClustering);
    recursive = true,
)

const page_rename = Dict("developer.md" => "Developer docs") # Without the numbers
const numbered_pages = [
    file for file in readdir(joinpath(@__DIR__, "src")) if
    file != "index.md" && splitext(file)[2] == ".md"
]

makedocs(;
    modules = [TulipaClustering],
    authors = "Greg Neustroev <G.Neustroev@tudelft.nl> and contributors",
    repo = "https://github.com/TulipaEnergy/TulipaClustering.jl/blob/{commit}{path}#{line}",
    sitename = "TulipaClustering.jl",
    format = Documenter.HTML(;
        canonical = "https://TulipaEnergy.github.io/TulipaClustering.jl",
    ),
    pages = ["index.md"; numbered_pages],
)

env_push_preview = get(ENV, "PUSH_PREVIEW", "false")
push_preview = tryparse(Bool, env_push_preview)
if isnothing(push_preview)
    @warn """Couldn't parse '$env_push_preview' into a Bool"""
    push_preview = false
end
deploydocs(; repo = "github.com/TulipaEnergy/TulipaClustering.jl", push_preview)
