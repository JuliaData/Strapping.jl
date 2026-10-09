```@meta
CurrentModule = Strapping
```

# [Mapping guide](@id Mapping-guide)

## The mapping model

Strapping derives one mapping plan from the target struct type. It reuses that
plan for every input or output row in the operation.

The default rules are:

1. A scalar field maps to a column with the same name.
2. A nested concrete struct maps to prefixed columns.
3. A concrete collection field expands into repeated rows.
4. A general union stays in one table cell.
5. A field marked `flatten=false` stays in one table cell.

For example, `address.city` maps to `address_city`. Prefixes compose at deeper
nesting levels.

## StructUtils field tags

Use `StructUtils.@tags` and the `strapping` namespace to change a mapping.

| Tag | Meaning |
| --- | --- |
| `id=true` | Group consecutive rows that belong to one root object. |
| `name=:column` | Rename a scalar column or the base name of a nested field. |
| `prefix=:text_` | Replace the default prefix for a nested struct or collection. |
| `ignore=true` | Exclude a field. The target type must provide a default value. |
| `flatten=false` | Store a nested struct or collection in one table cell. |

```@example tags
using Strapping, StructUtils, Tables

struct Coordinate
    x::Int
    y::Int
end

StructUtils.@tags struct Location
    id::Int &(strapping=(id=true, name=:location_id),)
    coordinate::Coordinate &(strapping=(prefix=:position_,),)
end

location = Location(7, Coordinate(10, 20))
columns = Tables.columntable(Strapping.deconstruct(location))
@assert columns == (location_id=[7], position_x=[10], position_y=[20])
nothing
```

The default [`Strapping.Style`](@ref) selects the `strapping` tag namespace.
You can pass another `StructUtils.StructStyle` with the `style` keyword when a
domain needs different `fieldtags`, `lift`, `lower`, or unknown-field policy.

## Nullable nested values

A flattened nullable struct needs an explicit state marker. Without one, a
table cannot distinguish `nothing` from a present struct whose fields are all
null.

Strapping writes a `__kind` column for this case:

- `:value` means the nested value is present.
- `:nothing` means the field is `nothing`.
- `:missing` means the field is `missing`.

```@example nullable
using Strapping, Tables

struct Bounds
    low::Union{Nothing,Int}
    high::Union{Nothing,Int}
end

struct Measurement
    id::Int
    bounds::Union{Nothing,Bounds}
end

values = [
    Measurement(1, nothing),
    Measurement(2, Bounds(nothing, nothing)),
]

columns = Tables.columntable(Strapping.deconstruct(values))
@assert columns.bounds__kind == [:nothing, :value]
@assert Strapping.construct(Vector{Measurement}, columns) == values
nothing
```

When an older input table has no `__kind` column, Strapping infers a null
nested value only when every mapped nested column is null. The explicit marker
is the only lossless representation of the all-null-present case.

## Collections and row grouping

One concrete collection can expand into repeated rows. Strapping writes a
`__length` column. This makes an empty collection different from a collection
with one null element.

When you construct `Vector{T}` and `T` has an expanded collection, tag one
root scalar field with `id=true`. Rows with the same ID must be consecutive.
When you construct one `T`, all supplied rows form that object if no ID is
configured.

Only one expanded collection is allowed in a mapping plan. Two collections
would require an implicit zip or Cartesian product. Strapping rejects that
ambiguous case. Mark all but one with `flatten=false` if table cells can store
those values.

Collections must implement `length` and integer `getindex`. This lets
Strapping expose a lazy row table without copying collection elements.

## Tables behavior

`Strapping.deconstruct` returns a row table with a stable schema. It supports
`Tables.rows`, `Tables.schema`, indexed and named column access, and `length`.
Tables.jl sinks can consume it directly.

`Strapping.construct` accepts any Tables.jl-compatible source. Extra columns
are ignored. Missing mapped columns use StructUtils field defaults when the
target type provides them.
