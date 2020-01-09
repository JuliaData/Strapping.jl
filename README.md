# Strapping.jl

<!-- [![Stable](https://img.shields.io/badge/docs-stable-blue.svg)](https://juliadata.github.io/Strapping.jl/stable)
[![Dev](https://img.shields.io/badge/docs-dev-blue.svg)](https://juliadata.github.io/Strapping.jl/dev) -->
[![Build Status](https://travis-ci.org/JuliaDatabases/Strapping.jl.svg?branch=master)](https://travis-ci.org/JuliaDatabases/Strapping.jl)
[![Codecov](https://codecov.io/gh/JuliaDatabases/Strapping.jl/branch/master/graph/badge.svg)](https://codecov.io/gh/JuliaDatabases/Strapping.jl)

Strapping stands for **ST**ruct **R**elational M**APPING**, and provides ORM-like functionality for Julia, including:

* automatically deserializing/constructing Julia structs from database query results (using [DBInterface.jl](https://github.com/JuliaDatabases/DBInterface.jl))
* ability to handle complicated aggregate types, with aggregate or collection fields
* integration with the [StructTypes.jl](https://github.com/JuliaData/StructTypes.jl) package for specifying struct and struct field options

## API

```julia
Strapping.select(conn::DBInterface.Connection, sql, T, args...; kw...) => T
Strapping.select(stmt::DBInterface.Statement, T, args...; kw...) => T
Strapping.select(conn::DBInterface.Connection, sql, Vector{T}, args...; kw...) => Vector{T}
Strapping.select(stmt::DBInterface.Statement, Vector{T}, args...; kw...) => Vector{T}
```
Providing a `DBInterface.Connection` `conn` and `sql` string, or an already-prepared `DBInterface.Statement` `stmt`,
plus any positional parameters `args`, or named parameters `kw`, bind any parameters, execute the query,
then use results (satisfying the `DBInterface.Cursor` interface) to construct a `T` or `Vector{T}`. The `StructTypes.StructType`
of `T` will be used to construct `T`; i.e. for `StructTypes.Struct`, fields in the resultset will be passed to `T`'s constructor;
for `StructTypes.Mutable`, each field will be retrieved from the resultset and set; for scalar types (`Int`, `Float64`, `String`),
the first field in the resultset will be returned (e.g. `Int` for `SELECT COUNT(*) FROM table`). For aggregate type fields,
it's expected that `StructTypes.fieldprefix(FT)` is defined, where `FT` is the type of the aggregate field. The field prefix
will be prepended to the aggregate field's field names and retrieved from the resultset. For collection type fields,
it's expected that `StructTypes.idproperty(T)` is defined to uniquely identify `T` instances in the resultset rows.
For each row of a single `T` instance, the collection field values are collected and set for the `T` instance.