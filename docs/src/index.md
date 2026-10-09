```@meta
CurrentModule = Strapping
Description = "Map Julia structs to and from Tables.jl-compatible data with StructUtils.jl."
```

# Strapping.jl

Strapping converts concrete Julia structs to and from two-dimensional table
data. It is useful at the seam between domain objects and CSV files, Arrow
tables, data frames, or database query results.

The package has two main operations:

- [`Strapping.construct`](@ref) builds one struct or a vector of structs from
  a Tables.jl-compatible source.
- [`Strapping.deconstruct`](@ref) exposes one struct or a vector of structs as
  a Tables.jl row table.

Strapping delegates field construction and value conversion to StructUtils.jl.
This keeps the interface small and lets the same field tags, defaults,
`lift`, and `lower` methods work across packages.

## Installation

```julia
import Pkg
Pkg.add("Strapping")
```

## Quick start

```@example quickstart
using Strapping, Tables

struct Point
    x::Int
    y::Float64
end

points = Strapping.construct(Vector{Point}, (y=[2.5, 3.5], x=[1, 2]))
@assert points == [Point(1, 2.5), Point(2, 3.5)]

table = Strapping.deconstruct(points)
@assert Tables.columntable(table) == (x=[1, 2], y=[2.5, 3.5])
nothing
```

Column order does not matter. Strapping matches columns by field name.

Continue with the [mapping guide](@ref Mapping-guide) or the complete
[related objects example](@ref Related-objects-example).
