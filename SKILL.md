---
name: strapping-jl
description: Use when mapping Julia structs to Tables.jl sources with Strapping, configuring field tags, or maintaining nullable and collection round trips.
---

# Using Strapping.jl

## Construct structs from a table

```julia
using Strapping

struct Point
    x::Int
    y::Float64
end

points = Strapping.construct(Vector{Point}, (x=[1, 2], y=[2.5, 3.5]))
```

Column order does not matter.

## Expose structs as a table

```julia
using Tables

rows = Strapping.deconstruct(points)
columns = Tables.columntable(rows)
```

## Configure a mapping

Use `StructUtils.@tags` with the `strapping` namespace:

```julia
using StructUtils

StructUtils.@tags struct Group
    id::Int &(strapping=(id=true, name=:group_id),)
    members::Vector{Point} &(strapping=(prefix=:member_,),)
end
```

Supported tags are `id`, `name`, `prefix`, `ignore`, and `flatten`.
Consult `docs/src/guide.md` before changing nullable or collection behavior.
