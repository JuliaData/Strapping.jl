# Strapping.jl

[![CI](https://github.com/JuliaData/Strapping.jl/actions/workflows/ci.yml/badge.svg)](https://github.com/JuliaData/Strapping.jl/actions/workflows/ci.yml)
[![Stable documentation](https://img.shields.io/badge/docs-stable-blue.svg)](https://juliadata.github.io/Strapping.jl/stable/)
[![Development documentation](https://img.shields.io/badge/docs-dev-blue.svg)](https://juliadata.github.io/Strapping.jl/dev/)

Strapping maps Julia structs to and from any Tables.jl-compatible source. It
uses StructUtils.jl for construction, field metadata, and custom value
conversion. It has no StructTypes.jl dependency.

```julia
using Strapping, StructUtils, Tables

struct Member
    id::Int
    name::String
end
Base.:(==)(a::Member, b::Member) = a.id == b.id && a.name == b.name

StructUtils.@tags struct Club
    id::Int &(strapping=(id=true,),)
    name::String
    members::Vector{Member}
end
Base.:(==)(a::Club, b::Club) =
    a.id == b.id && a.name == b.name && a.members == b.members

club = Club(1, "chess club", [Member(1, "John"), Member(2, "Mary")])
table = Strapping.deconstruct(club)

Tables.columntable(table)
# (id = [1, 1], name = ["chess club", "chess club"],
#  members__length = [2, 2], members_id = [1, 2],
#  members_name = ["John", "Mary"])

Strapping.construct(Club, table) == club
# true
```

The public surface is intentionally namespaced. Use
`Strapping.construct`, `Strapping.deconstruct`, and `Strapping.Style`.
See the [manual](https://juliadata.github.io/Strapping.jl/dev/) for nullable
nested values, collection grouping, field tags, and the version 2 migration.
