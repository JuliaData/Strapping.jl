using Documenter, Strapping

makedocs(;
    modules=[Strapping],
    format=Documenter.HTML(; repolink="https://github.com/JuliaData/Strapping.jl"),
    pages=[
        "Home" => "index.md",
    ],
    sitename="Strapping.jl",
    authors="Jacob Quinn",
)

deploydocs(;
    repo="github.com/JuliaData/Strapping.jl",
    devbranch = "main"
)
