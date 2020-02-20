# Strapping.jl

This guide provides documentation around the `Strapping.construct` and `Strapping.deconstruct` functions.
This package was born from a desire for straightforward, not-too-magical ORM capabilities in Julia, which
means being able to transform, for example, 2D SQL query results from a database into a `Vector` of custom
application objects, without having to write your own adapter code. Strapping.jl integrates with the
[StructTypes.jl](https://github.com/JuliaData/StructTypes.jl) package, which allows customizing Julia structs
and their fields.

If anything isn't clear or you find bugs, don't hesitate to [open a new issue](https://github.com/JuliaData/Strapping.jl/issues/new), even just for a question, or come chat with us on the
[#data](https://julialang.slack.com/messages/data/) slack channel with questions, concerns, or clarifications.

```@contents
Depth = 2
```

## `Strapping.construct`

```@docs
Strapping.construct
```

## `Strapping.deconstruct`

```@docs
Strapping.deconstruct
```
