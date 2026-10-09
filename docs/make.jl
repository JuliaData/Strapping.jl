using Documenter
using Strapping

DocMeta.setdocmeta!(Strapping, :DocTestSetup, :(using Strapping); recursive = true)

makedocs(
    modules = [Strapping],
    sitename = "Strapping.jl",
    authors = "Jacob Quinn and contributors",
    format = Documenter.HTML(
        prettyurls = true,
        canonical = "https://juliadata.github.io/Strapping.jl/stable/",
        collapselevel = 2,
    ),
    pages = [
        "Home" => "index.md",
        "Manual" => [
            "Mapping guide" => "guide.md",
            "Related objects example" => "example.md",
            "Version 2 migration" => "migration.md",
        ],
        "API reference" => "api.md",
    ],
    pagesonly = true,
    checkdocs = :exports,
)

deploydocs(repo = "github.com/JuliaData/Strapping.jl.git", devbranch = "main")
