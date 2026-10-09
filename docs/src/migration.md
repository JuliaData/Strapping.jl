```@meta
CurrentModule = Strapping
```

# Version 2 migration

Version 2 is a deliberate interface reset. It removes StructTypes.jl and uses
StructUtils.jl throughout.

## Dependency and type setup

Remove `StructTypes` from the application environment. Plain concrete structs
need no registration method.

```julia
# Version 1
StructTypes.StructType(::Type{Record}) = StructTypes.Struct()

# Version 2
# No declaration is needed.
```

Use StructUtils macros or interface methods for defaults, mutable no-argument
construction, custom scalar wrappers, and field metadata.

## Configuration mapping

| Version 1 | Version 2 |
| --- | --- |
| `StructTypes.idproperty(T) = :id` | Add `&(strapping=(id=true,),)` to the ID field. |
| `StructTypes.fieldprefix(T, :child) = :nested_` | Add `&(strapping=(prefix=:nested_,),)` to `child`. |
| `StructTypes.excludes(T)` | Add `&(strapping=(ignore=true,),)` and provide a StructUtils default. |
| `StructTypes.lower` / `construct` | Use `StructUtils.lower` / `lift`. |
| `StructTypes.Mutable()` | Use `StructUtils.@noarg` or overload `StructUtils.noarg`. |
| `StructTypes.CustomStruct()` | Use `StructUtils.@nonstruct`, `lower`, and `lift`. |

## Table schema changes

Version 2 emits explicit metadata columns:

- Nullable flattened structs and collections get `field__kind`.
- Flattened collections get `field__length`.

These columns fix the old ambiguity between an absent nested value, a present
all-null nested value, an empty collection, and a one-element null collection.

Version 2 also matches all input columns by name. It does not use table column
order as struct field order.

## Scope changes

The root mapping target must be a concrete struct or concrete NamedTuple.
Dynamic dictionaries and root arrays are no longer treated as implicit struct
schemas. Dictionary fields stay in one cell unless a user converts them to a
concrete struct first.

A general union, such as `Union{String,Vector{String}}`, stays in one table
cell. Only a nullable union with one concrete non-null struct or collection is
flattened automatically.

The old `silencewarnings` keyword remains available when constructing one
object from a table with extra rows.
