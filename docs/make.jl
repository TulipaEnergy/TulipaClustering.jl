using TulipaClustering
using Documenter

DocMeta.setdocmeta!(
  TulipaClustering,
  :DocTestSetup,
  :(using TulipaClustering; using DataFrames);
  recursive = true,
)

makedocs(;
  modules = [TulipaClustering],
  doctest = true,
  linkcheck = true,
  authors = "Greg Neustroev <G.Neustroev@tudelft.nl> and contributors",
  repo = "https://github.com/TulipaEnergy/TulipaClustering.jl/blob/{commit}{path}#{line}",
  sitename = "TulipaClustering.jl",
  format = Documenter.HTML(;
    prettyurls = get(ENV, "CI", "false") == "true",
    canonical = "https://TulipaEnergy.github.io/TulipaClustering.jl",
    edit_link = "main",
    assets = String[],
  ),
  pages = [
    "Home" => "index.md",
    "Contributing" => "contributing.md",
    "Reference" => "reference.md",
  ],
)

deploydocs(; repo = "github.com/TulipaEnergy/TulipaClustering.jl", devbranch = "main")
