```@meta
CurrentModule = Strapping
```

# [Related objects example](@id Related-objects-example)

This example maps a club and its related members to one flat table. It answers
the workflow requested in [issue #22](https://github.com/JuliaData/Strapping.jl/issues/22).

```@example related
using Strapping, StructUtils, Tables

struct Member
    id::Int
    first_name::String
    last_name::String
end
Base.:(==)(a::Member, b::Member) =
    a.id == b.id && a.first_name == b.first_name && a.last_name == b.last_name

StructUtils.@tags struct Club
    id::Int &(strapping=(id=true,),)
    name::String
    members::Vector{Member}
end
Base.:(==)(a::Club, b::Club) =
    a.id == b.id && a.name == b.name && a.members == b.members

clubs = [
    Club(
        1,
        "chess club",
        [
            Member(10, "John", "Smith"),
            Member(11, "Mary", "Miller"),
        ],
    ),
    Club(2, "book club", Member[]),
]

rows = Strapping.deconstruct(clubs)
columns = Tables.columntable(rows)

@assert columns.id == [1, 1, 2]
@assert columns.members__length == [2, 2, 0]
@assert isequal(columns.members_first_name, ["John", "Mary", missing])

round_trip = Strapping.construct(Vector{Club}, columns)
@assert round_trip == clubs
nothing
```

The root `id` repeats for each member row. `members__length` records two
members for the first club and an empty collection for the second club. The
placeholder row for the empty collection keeps the club's scalar fields in the
table without inventing a member.
