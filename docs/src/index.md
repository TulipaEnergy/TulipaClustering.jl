```@meta
CurrentModule = TulipaClustering
DocTestSetup = quote
    using DataFrames
end
```

# Welcome

The [TulipaClustering.jl](https://github.com/TulipaEnergy/TulipaClustering.jl) package is a Julia package for finding representative periods (e.g., hours, days, or weeks) and their weights based on time series profiles for the energy sector (e.g., demand, availability, hydro inflows). The package uses DuckDB to handle the time series input data, then uses the parameter definition in the model and the main function [`cluster!`](@ref), and exports the results to DuckDB again. The user can export the results to other formats, e.g., CSV, if needed.

![TulipaClustering.jl overview](assets/TulipaClustering-overview.png)

# What is the Novelty in this Package?

This method employs a novel approach known as hull clustering with blended representative periods (RPs), which improves upon traditional clustering-based methods in two significant ways.

First, instead of selecting typical cluster centers, such as centroids or medoids, as RPs, the TulipaClustering hull methods utilize extreme points that are more likely to be constraint-binding.

Second, it represents base periods, or non-representative periods, as weighted combinations of RPs, such as convex or conic blends. This methods allows for a more accurate approximation of the entire time horizon using fewer representative periods.

One example of the improvements achieved by using this package to find representative periods is illustrated in the figure below. We compare the results from the energy system model [TulipaEnergyModel.jl](https://tulipaenergy.github.io/TulipaEnergyModel.jl/stable/) after applying `TulipaClustering.jl` to obtain the representative periods, using both the traditional setup and the hull clustering with blended RPs. The results demonstrate that hull clustering with blended RPs can effectively represent seasonal storage—such as the hydro reservoirs in Norway—while achieving lower regret (i.e., less approximation error), requiring fewer representatives and less time.

![Norway's hydro reservoir results](assets/hydro_reservoir_comparison_results.png)

!!! note "Can I use `TulipaClustering.jl` with other energy system optimization models?"
    Yes, you can 😁 Thanks to our DuckDB interface for inputs and outputs, the results can be exported to several formats, and as long as your energy system optimization model is able to use representative periods in its formulation, then you are ready to match the output of `TulipaClustering.jl` to your model and run your optimization. Have fun!

## [License](@id license)

This content is released under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0) License.

## Contributors

```@raw html
<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tbody>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/greg-neustroev"><img src="https://avatars.githubusercontent.com/u/32451432?v=4?s=100" width="100px;" alt="Greg Neustroev"/><br /><sub><b>Greg Neustroev</b></sub></a><br /><a href="#code-greg-neustroev" title="Code">💻</a> <a href="#doc-greg-neustroev" title="Documentation">📖</a> <a href="#maintenance-greg-neustroev" title="Maintenance">🚧</a> <a href="#review-greg-neustroev" title="Reviewed Pull Requests">👀</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/g-moralesespana"><img src="https://avatars.githubusercontent.com/u/42405171?v=4?s=100" width="100px;" alt="Germán Morales"/><br /><sub><b>Germán Morales</b></sub></a><br /><a href="#ideas-g-moralesespana" title="Ideas, Planning, & Feedback">🤔</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://abelsiqueira.com"><img src="https://avatars.githubusercontent.com/u/1068752?v=4?s=100" width="100px;" alt="Abel Soares Siqueira"/><br /><sub><b>Abel Soares Siqueira</b></sub></a><br /><a href="#code-abelsiqueira" title="Code">💻</a> <a href="#review-abelsiqueira" title="Reviewed Pull Requests">👀</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/datejada"><img src="https://avatars.githubusercontent.com/u/12887482?v=4?s=100" width="100px;" alt="Diego Alejandro Tejada Arango"/><br /><sub><b>Diego Alejandro Tejada Arango</b></sub></a><br /><a href="#code-datejada" title="Code">💻</a> <a href="#doc-datejada" title="Documentation">📖</a> <a href="#review-datejada" title="Reviewed Pull Requests">👀</a> <a href="#maintenance-datejada" title="Maintenance">🚧</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/clizbe"><img src="https://avatars.githubusercontent.com/u/11889283?v=4?s=100" width="100px;" alt="Lauren Clisby"/><br /><sub><b>Lauren Clisby</b></sub></a><br /><a href="#projectManagement-clizbe" title="Project Management">📆</a> <a href="#ideas-clizbe" title="Ideas, Planning, & Feedback">🤔</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/lottekremer"><img src="https://avatars.githubusercontent.com/u/119004215?v=4?s=100" width="100px;" alt="Lotte Kremer"/><br /><sub><b>Lotte Kremer</b></sub></a><br /><a href="#code-lottekremer" title="Code">💻</a> <a href="#ideas-lottekremer" title="Ideas, Planning, & Feedback">🤔</a></td>
    </tr>
  </tbody>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->
```
