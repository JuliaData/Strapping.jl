using Strapping
using StructUtils
using Tables
using Test
using Logging

include("types.jl")

@testset "Strapping" begin
    include("construction.jl")
    include("deconstruction.jl")
    include("issues.jl")
    include("errors.jl")

    if get(ENV, "STRAPPING_QUALITY", "false") == "true"
        include("quality.jl")
    end
end
