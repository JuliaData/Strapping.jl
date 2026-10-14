using Documenter, Strapping

makedocs(;
    modules=[Strapping],
    format=Documenter.HTML(),
    pages=[
        "Home" => "index.md",
    ],
    repo="https://github.com/JuliaData/Strapping.jl/blob/{commit}{path}#L{line}",
    sitename="Strapping.jl",
    authors="Jacob Quinn",
)

deploydocs(;
    repo="github.com/JuliaData/Strapping.jl",
    devbranch = "main"
)
