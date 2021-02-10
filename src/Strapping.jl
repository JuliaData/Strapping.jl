module Strapping

using Tables, StructTypes

struct Error <: Exception
    msg::String
end

"""
    Strapping.construct(T, tbl)
    Strapping.construct(Vector{T}, tbl)

Given a [Tables.jl](https://github.com/JuliaData/Tables.jl)-compatible input table source `tbl`,
construct an instance of `T` (single object, first method), or `Vector{T}` (list of objects, 2nd method).

The 1st method will throw an error if the input table is empty, and warn if there are more rows
than necessary to construct a single `T`.

The 2nd method will return an empty list for an empty input source, and construct as many `T` as are
found until the input table is exhausted.

`Strapping.construct` utilizes the [StructTypes.jl](https://github.com/JuliaData/StructTypes.jl) package
for determining the `StructTypes.StructType` trait of `T` and constructing an instance appropriately:
  * `StructTypes.Struct`/`StructTypes.Mutable`: field reflection will be used to retrieve values from the input table row, with field customizations respected, like excluded fields, field-specific keyword args, etc.
  * `StructTypes.DictType`: each column name/value of the table row will be used as a key/value pair to be passed to the `DictType` constructor
  * `StructTypes.ArrayType`: column values will be "collected" as an array to be passed to the `ArrayType` constructor
  * `StructTypes.StringType`/`StructTypes.NumberType`/`StructTypes.BoolType`/`StructTypes.NullType`: only the first value of the row will be passed to the scalar type constructor

Note that for `StructTypes.DictType` and `StructTypes.ArrayType`, "aggregate" value/eltypes are not allowed, since
the entire row is treated as key/value pairs or array elements. That means, for example, I can't have a table with rows like `tbl = [(a=1, b=2)]` and try to do `Strapping.construct(Dict{Symbol, Dict{Int, Int}}, tbl)`. It first attempts to map column names to the outer `Dict` keys, (`a` and `b`), but then tries to map the values `1` and `2`
to `Dict{Int, Int}` and fails.

For structs with `ArrayType` fields, the first row values will be used for other scalar fields, and subsequent rows
will be iterated for the `ArrayType` field values. For example, I may wish to construct a type like:
```julia
struct TestResult
    id::Int
    values::Vector{Float64}
end
StructTypes.StructType(::Type{TestResult}) = StructTypes.Struct()
StructTypes.idproperty(::Type{TestResult}) = :id
```

and my input table would look something like, `tbl = (id=[1, 1, 1], values=[3.14, 3.15, 3.16])`. I can then construct my type like:

```julia
julia> Strapping.construct(TestResult, tbl)
TestResult(1, [3.14, 3.15, 3.16])
```

Note that along with defining the `StructTypes.StructType` trait for `TestResult`, I also needed to define `StructTypes.idproperty` to signal which field of my struct is a "unique key" identifier. This enables Strapping to distinguish which rows belong to a particular instance of `TestResult`. This allows the slightly more complicated example of returning multiple `TestResult`s from a single table:

```julia
julia> tbl = (id=[1, 1, 1, 2, 2, 2], values=[3.14, 3.15, 3.16, 40.1, 0.01, 2.34])
(id = [1, 1, 1, 2, 2, 2], values = [3.14, 3.15, 3.16, 40.1, 0.01, 2.34])

julia> Strapping.construct(Vector{TestResult}, tbl)
2-element Array{TestResult,1}:
 TestResult(1, [3.14, 3.15, 3.16])
 TestResult(2, [40.1, 0.01, 2.34])
```

Here, we actually have _two_ `TestResult` objects in our `tbl`, and Strapping uses the `id` field to identify object owners for a row. Note that currently the table rows need to be sorted on the `idproperty` field, i.e. rows belonging to the same object must appear consecutively in the input table rows.

Now let's discuss "aggregate" type fields. Let's say I have a struct like:

```julia
struct Experiment
    id::Int
    name::String
    testresults::TestResult
end
StructTypes.StructType(::Type{Experiment}) = StructTypes.Struct()
StructTypes.idproperty(::Type{Experiment}) = :id
```

So my `Experiment` type also as an `id` field, in addition to a `name` field, and an "aggregate" field of `testresults`. How should the input table source account for `testresults`, which is itself a struct made up of its own `id` and `values` fields? The key here is "flattening" nested structs into a single set of table column names, and utilizing the `StructTypes.fieldprefix` function, which allows specifying a `Symbol` prefix to identify an aggregate field's columns in the table row. So, in the case of our `Experiment`, we can do:

```julia
StructTypes.fieldprefix(::Type{Experiment}, nm::Symbol) = nm == :testresults ? :testresults_ : :_
```

Note that this is the default definition, so we don't really need to define this, but for illustration purposes, we'll walk through it. We're saying that for the `:testresults` field name, we should expect its column names in the table row to start with `:testresults_`. So the table data for an `Experiment` instance, would look something like:

```julia
tbl = (id=[1, 1, 1], name=["exp1", "exp1", "exp1"], testresults_id=[1, 1, 1], testresults_values=[3.14, 3.15, 3.16])
```

This pattern generalizes to structs with multiple aggregate fields, or aggregate fields that themselves have aggregate fields (nested aggregates); in the nested case, the prefixes are concatenated, like `testresults_specifictestresult_id`.
"""
function construct end

function construct(::Type{T}, source; silencewarnings::Bool=false, kw...) where {T}
    rows = Tables.rows(source)
    state = iterate(rows)
    state === nothing && throw(Error("can't construct `$T` from empty source"))
    row, st = state
    x, state = construct(rows, st, row, T; kw...)
    state === nothing || (silencewarnings && println("warning: additional source rows left after reading `$T`"))
    return x
end

function construct(::Type{Vector{T}}, source; kw...) where {T}
    rows = Tables.rows(source)
    state = iterate(rows)
    A = Vector{T}(undef, 0)
    while state !== nothing
        row, st = state
        x, state = construct(rows, st, row, T; kw...)
        push!(A, x)
    end
    return A
end

# collection handler: will iterate results for collection fields
function construct(rows, st, row, ::Type{T}; kw...) where {T}
    x = construct(StructTypes.StructType(T), row, T; kw...)
    idprop = StructTypes.idproperty(T)
    if idprop !== :_
        id = Tables.getcolumn(row, idprop)
        state = iterate(rows, st)
        if state !== nothing
            while state !== nothing
                row, st = state
                Tables.getcolumn(row, idprop) == id || break
                construct!(StructTypes.StructType(T), row, x; kw...)
                state = iterate(rows, st)
            end
        end
    else
        state = iterate(rows, st)
    end
    return x, state
end

# aggregate handlers (don't take specific `col`/`nm` arguments)
# construct versions construct initial object
# construct! versions take existing object and append additional elements to collection fields
function construct(::StructTypes.Struct, row, ::Type{T}; kw...) where {T}
    coloffset = Ref{Int}(0)
    return StructTypes.construct(T) do i, nm, TT
        construct(StructTypes.StructType(TT), T, row, i, coloffset, Symbol(), nm, TT; kw...)
    end
end

function construct!(::StructTypes.Struct, row, x::T; kw...) where {T}
    coloffset = Ref{Int}(0)
    return StructTypes.foreachfield(x) do i, nm, TT, v
        construct!(StructTypes.StructType(TT), T, row, i, coloffset, Symbol(), nm, TT, v; kw...)
    end
end

function construct(::StructTypes.Mutable, row, ::Type{T}; kw...) where {T}
    x = T()
    coloffset = Ref{Int}(0)
    StructTypes.mapfields!(x) do i, nm, TT
        y = construct(StructTypes.StructType(TT), T, row, i, coloffset, Symbol(), nm, TT; kw...)
        return y
    end
    return x
end

function construct!(::StructTypes.Mutable, row, x::T; kw...) where {T}
    coloffset = Ref{Int}(0)
    return StructTypes.foreachfield(x) do i, nm, TT, v
        construct!(StructTypes.StructType(TT), T, row, i, coloffset, Symbol(), nm, TT, v; kw...)
    end
end

# default aggregate
construct(::StructTypes.Struct, row, ::Type{Any}; kw...) =
    construct(StructTypes.DictType(), row, Dict{String, Any}; kw...)

construct(::StructTypes.DictType, row, ::Type{T}; kw...) where {T} = construct(StructTypes.DictType(), row, T, Symbol, Any; kw...)
construct(::StructTypes.DictType, row, ::Type{T}; kw...) where {T <: NamedTuple} = construct(StructTypes.DictType(), row, T, Symbol, Any; kw...)
construct(::StructTypes.DictType, row, ::Type{Dict}; kw...) = construct(StructTypes.DictType(), row, Dict, String, Any; kw...)
construct(::StructTypes.DictType, row, ::Type{T}; kw...) where {T <: AbstractDict} = construct(StructTypes.DictType(), row, T, keytype(T), valtype(T); kw...)

function construct(::StructTypes.DictType, row, ::Type{T}, ::Type{K}, ::Type{V}; kw...) where {T, K, V}
    #TODO: formally disallow aggregate types as V?
    # the problem is we're already treating the DictType as the aggregate, so
    # it's impossible to distinguish between multiple aggregate type V
    # from a single resultset set of fields
    x = Dict{K, V}()
    for (i, nm) in enumerate(Tables.columnnames(row))
        val = construct(StructTypes.StructType(V), T, row, i, Ref{Int}(0), Symbol(), nm, V; kw...)
        if K == Symbol
            x[nm] = val
        else
            x[StructTypes.construct(K, String(nm))] = val
        end
    end
    return StructTypes.construct(T, x; kw...)
end

function construct!(::StructTypes.DictType, row, x::T; kw...) where {T}
    for (i, nm) in enumerate(Tables.columnnames(row))
        v = x[nm]
        V = typeof(v)
        construct!(StructTypes.StructType(V), T, row, i, Ref{Int}(0), Symbol(), nm, V, v; kw...)
    end
    return
end

construct(::StructTypes.ArrayType, row, ::Type{T}; kw...) where {T} = construct(StructTypes.ArrayType(), row, T, Base.IteratorEltype(T) == Base.HasEltype() ? eltype(T) : Any; kw...)
construct(::StructTypes.ArrayType, row, ::Type{T}, ::Type{eT}; kw...) where {T, eT} = constructarray(row, T, eT; kw...)
construct(::StructTypes.ArrayType, row, ::Type{Tuple}, ::Type{eT}; kw...) where {eT} = constructarray(row, Tuple, eT; kw...)

function constructarray(row, ::Type{T}, ::Type{eT}; kw...) where {T, eT}
    #TODO: disallow aggregate eT? same problem as DictType; we're treating the ArrayType as our aggregate
    nms = Tables.columnnames(row)
    N = length(nms)
    x = Vector{eT}(undef, N)
    for (i, nm) in enumerate(nms)
        x[i] = construct(StructTypes.StructType(eT), T, row, i, Ref{Int}(0), Symbol(), nm, eT; kw...)
    end
    return StructTypes.construct(T, x; kw...)
end

function construct!(::StructTypes.ArrayType, row, x::T; kw...) where {T}
    for (i, nm) in enumerate(Tables.columnnames(row))
        v = x[i]
        V = typeof(v)
        construct!(StructTypes.StructType(V), T, row, i, Ref{Int}(0), Symbol(), nm, V, v; kw...)
    end
    return
end

function construct(::StructTypes.ArrayType, row, ::Type{T}, ::Type{eT}; kw...) where {T <: Tuple, eT}
    return StructTypes.construct(T) do i, nm, TT
        construct(StructTypes.StructType(TT), T, row, i, Ref{Int}(0), Symbol(), nm, TT; kw...)
    end
end

# constructing a single scalar from a row
# for example, you want the result to be a single Int from: SELECT COUNT(*) FROM table
function construct(::StructTypes.StringType, row, ::Type{T}; kw...) where {T}
    return StructTypes.construct(T, Tables.getcolumn(row, 1))
end

function construct(::StructTypes.NumberType, row, ::Type{T}; kw...) where {T}
    return StructTypes.construct(T, StructTypes.numbertype(T)(Tables.getcolumn(row, 1)))
end

function construct(::StructTypes.BoolType, row, ::Type{T}; kw...) where {T}
    return StructTypes.construct(T, Tables.getcolumn(row, 1))
end

function construct(::StructTypes.NullType, row, ::Type{T}; kw...) where {T}
    return StructTypes.construct(T, Tables.getcolumn(row, 1))
end

## field construction: here we have a specific col::Int/nm::Symbol argument for a parent aggregate
# Struct field
function construct(::StructTypes.Struct, PT, row, col, coloffset, prefix, nm, ::Type{T}; kw...) where {T}
    prefix = Symbol(prefix, StructTypes.fieldprefix(PT, nm))
    off = 0
    x = StructTypes.construct(T) do i, nm, TT
        off += 1
        construct(StructTypes.StructType(TT), T, row, coloffset[] + col + i - 1, coloffset, prefix, nm, TT; kw...)
    end
    coloffset[] += off - 1
    return x
end

function construct!(::Union{StructTypes.Struct, StructTypes.Mutable}, PT, row, col, coloffset, prefix, nm, ::Type{T}, v; kw...) where {T}
    off = 0
    StructTypes.foreachfield(v) do i, nm, TT, v
        off += 1
    end
    coloffset[] += off - 1
    return
end

function construct(::StructTypes.Mutable, PT, row, col, coloffset, prefix, nm, ::Type{T}; kw...) where {T}
    prefix = Symbol(prefix, StructTypes.fieldprefix(PT, nm))
    off = 0
    x = T()
    StructTypes.mapfields!(x) do i, nm, TT
        off += 1
        construct(StructTypes.StructType(TT), T, row, coloffset[] + col + i - 1, coloffset, prefix, nm, TT; kw...)
    end
    coloffset[] += off - 1
    return x
end

construct(::StructTypes.DictType, PT, row, col, coloffset, prefix, nm, ::Type{T}; kw...) where {T} = construct(StructTypes.DictType(), PT, row, col, coloffset, prefix, nm, T, Symbol, Any; kw...)
construct(::StructTypes.DictType, PT, row, col, coloffset, prefix, nm, ::Type{T}; kw...) where {T <: NamedTuple} = construct(StructTypes.DictType(), PT, row, col, coloffset, prefix, nm, T, Symbol, Any; kw...)
construct(::StructTypes.DictType, PT, row, col, coloffset, prefix, nm, ::Type{Dict}; kw...) = construct(StructTypes.DictType(), PT, row, col, coloffset, prefix, nm, Dict, String, Any; kw...)
construct(::StructTypes.DictType, PT, row, col, coloffset, prefix, nm, ::Type{T}; kw...) where {T <: AbstractDict} = construct(StructTypes.DictType(), PT, row, col, coloffset, prefix, nm, T, keytype(T), valtype(T); kw...)

function construct(::StructTypes.DictType, PT, row, col, coloffset, prefix, nm, ::Type{T}, ::Type{K}, ::Type{V}; kw...) where {T, K, V}
    prefix = String(Symbol(prefix, StructTypes.fieldprefix(PT, nm)))
    off = 0
    x = Dict{K, V}()
    for (i, nm) in enumerate(Tables.columnnames(row))
        if startswith(String(nm), prefix)
            val = construct(StructTypes.StructType(V), T, row, i, Ref{Int}(0), Symbol(), nm, V; kw...)
            off += 1
            if K == Symbol
                x[nm] = val
            else
                x[StructTypes.construct(K, String(nm))] = val
            end
        end
    end
    coloffset[] += off
    return StructTypes.construct(T, x; kw...)
end

function construct(::StructTypes.ArrayType, PT, row, col, coloffset, prefix, nm, ::Type{T}; kw...) where {T}
    eT = Base.IteratorEltype(T) == Base.HasEltype() ? eltype(T) : Any
    return StructTypes.construct(T, [construct(StructTypes.StructType(eT), PT, row, col, coloffset, prefix, nm, eT; kw...)]; kw...)
end

function construct!(::StructTypes.ArrayType, PT, row, col, coloffset, prefix, nm, ::Type{T}, v; kw...) where {T}
    eT = Base.IteratorEltype(T) == Base.HasEltype() ? eltype(T) : Any
    push!(v, construct(StructTypes.StructType(eT), PT, row, col, coloffset, prefix, nm, eT; kw...))
    return
end

# for all other construct!, we ignore
construct!(ST, PT, row, col, coloffset, prefix, nm, T, v; kw...) = nothing

# scalar handlers (take a `col` argument)
getvalue(row, ::Type{T}, col::Int, nm::Symbol) where {T} = Tables.getcolumn(row, T, col, nm)
getvalue(row, ::Type{T}, col::Int, nm::Int) where {T} = Tables.getcolumn(row, nm)

construct(::StructTypes.Struct, PT, row, col, coloffset, prefix, nm, ::Type{Any}; kw...) = getvalue(row, Any, col, Symbol(prefix, nm))
construct(::StructTypes.Struct, PT, row, col, coloffset, prefix, nm, U::Union; kw...) = getvalue(row, U, col, Symbol(prefix, nm))
construct(::StructTypes.StringType, PT, row, col, coloffset, prefix, nm, ::Type{T}; kw...) where {T} = StructTypes.construct(T, getvalue(row, T, col, Symbol(prefix, nm)))
construct(::StructTypes.NumberType, PT, row, col, coloffset, prefix, nm, ::Type{T}; kw...) where {T} =
    StructTypes.construct(T, StructTypes.numbertype(T)(getvalue(row, T, col, Symbol(prefix, nm))))
construct(::StructTypes.BoolType, PT, row, col, coloffset, prefix, nm, ::Type{T}; kw...) where {T} = StructTypes.construct(T, getvalue(row, T, col, Symbol(prefix, nm)))
construct(::StructTypes.NullType, PT, row, col, coloffset, prefix, nm, ::Type{T}; kw...) where {T} = StructTypes.construct(T, getvalue(row, T, col, Symbol(prefix, nm)))

# deconstruct

"""
    Strapping.deconstruct(x::T)
    Strapping.deconstruct(x::Vector{T})

The inverse of `Strapping.construct`, where an object instance `x::T` or `Vector` of objects `x::Vector{T}` is "deconstructed" into a Tables.jl-compatible row iterator. This works following the same patterns outlined in `Strapping.construct` with regards to `ArrayType` and aggregate fields. Specifically, `ArrayType` fields will cause multiple rows to be outputted, one row per collection element, with other scalar fields being repeated in each row. Similarly for aggregate fields, the field prefix will be used (`StructTypes.fieldprefix`) and nested aggregates will all be flattened into a single list of column names with aggregate prefixes.

In general, this allows outputting any "object" as a 2D table structure that could be stored in any Tables.jl-compatible sink format, e.g. csv file, sqlite table, mysql database table, feather file, etc.
"""
function deconstruct end

# single object or vector => Tables.rows iterator
struct DeconstructedRowsIterator{T, A}
    values::A
    lens::Vector{Int}
    len::Int
    names::Vector{Symbol}
    types::Vector{Type}
    fieldindices::Vector{Tuple{Int, Int}}
    fieldnames::Vector{Tuple{Symbol, Symbol}}
    lookup::Dict{Symbol, Int}
end

Tables.isrowtable(::Type{<:DeconstructedRowsIterator}) = true

# disabled because 
# Tables.schema(x::DeconstructedRowsIterator) = Tables.Schema(x.names, x.types)

struct DeconstructedRow{T} <: Tables.AbstractRow
    x::T # a single object we're deconstructing
    index::Int # index of this specific row (may be > 1 for objects w/ collection fields)
    names::Vector{Symbol}
    fieldindices::Vector{Tuple{Int, Int}}
    fieldnames::Vector{Tuple{Symbol, Symbol}}
    lookup::Dict{Symbol, Int}
end

obj(x::DeconstructedRow) = getfield(x, :x)
ind(x::DeconstructedRow) = getfield(x, :index)
names(x::DeconstructedRow) = getfield(x, :names)
inds(x::DeconstructedRow) = getfield(x, :fieldindices)
nms(x::DeconstructedRow) = getfield(x, :fieldnames)
lookup(x::DeconstructedRow) = getfield(x, :lookup)

Tables.columnnames(row::DeconstructedRow) = names(row)
Tables.getcolumn(row::DeconstructedRow, ::Type{T}, i::Int, nm::Symbol) where {T} =
    getfieldvalue(obj(row), ind(row), inds(row)[i], nms(row)[i])
Tables.getcolumn(row::DeconstructedRow, i::Int) =
    getfieldvalue(obj(row), ind(row), inds(row)[i], nms(row)[i])
Tables.getcolumn(row::DeconstructedRow, nm::Symbol) = Tables.getcolumn(row, lookup(row)[nm])

mutable struct DeconstructClosure{PT}
    len::Int
    names::Vector{Symbol}
    types::Vector{Type}
    fieldindices::Vector{Tuple{Int, Int}}
    fieldnames::Vector{Tuple{Symbol, Symbol}}
    parentindex::Int
    parentname::Symbol
    prefix::Symbol
    j::Int
    nm::Symbol
    parenttype::Type{PT}
end

DeconstructClosure(PT) = DeconstructClosure(1, Symbol[], Type[], Tuple{Int, Int}[], Tuple{Symbol, Symbol}[], 0, Symbol(), Symbol(), 1, Symbol(), PT)

function (f::DeconstructClosure)(i, nm, TT, v; kw...)
    len = valuelength(StructTypes.StructType(TT), v)
    if len > f.len
        f.len = len
    end
    nametypeindex!(v, i, nm, f)
    return
end

mutable struct DeconstructLenClosure
    len::Int
end

function (f::DeconstructLenClosure)(i, nm, TT, v; kw...)
    len = valuelength(StructTypes.StructType(TT), v)
    if len > f.len
        f.len = len
    end
    return
end

deconstructobj!(x::T, c) where {T} = deconstructobj!(StructTypes.StructType(T), x, c)
deconstructobj!(ST, x, c) = StructTypes.foreachfield(c, x)

function deconstructobj!(::StructTypes.DictType, x, c)
    i = 1
    for (k, v) in StructTypes.keyvaluepairs(x)
        c(i, k, typeof(v), v)
        i += 1
    end
    return
end

deconstruct(x) = deconstruct([x])

function deconstruct(values::A) where {T, A <: AbstractVector{T}}
    c = DeconstructClosure(T)
    flens = DeconstructLenClosure(0)
    lens = Int[]
    len = 0
    i = 0
    for x in values
        if i == 0
            deconstructobj!(x, c)
            push!(lens, c.len)
            len += c.len
        else
            flens.len = 0
            deconstructobj!(x, flens)
            len += flens.len
            push!(lens, flens.len)
        end
        i += 1
    end
    lookup = Dict(nm => i for (i, nm) in enumerate(c.names))
    return DeconstructedRowsIterator{T, A}(values, lens, len, c.names, c.types, c.fieldindices, c.fieldnames, lookup)
end

Base.eltype(x::DeconstructedRowsIterator{T}) where {T} = DeconstructedRow{T}
Base.length(x::DeconstructedRowsIterator) = x.len

# single object w/ plain scalar fields: 1 obj => 1 row
# single object w/ scalar field and collection field: 1 obj => N rows, where N = length(collection)
# single object w/ aggregate object field: 1 obj => 1 row
# single object w/ aggregate object collection field: 1 obj => N rows, where N = length(collection)
@inline function Base.iterate(x::DeconstructedRowsIterator, (i, j, k)=(1, 1, 1))
    k > x.len && return nothing
    if j == x.lens[i]
        return DeconstructedRow(x.values[i], j, x.names, x.fieldindices, x.fieldnames, x.lookup), (i + 1, 1, k + 1)
    else
        return DeconstructedRow(x.values[i], j, x.names, x.fieldindices, x.fieldnames, x.lookup), (i, j + 1, k + 1)
    end
end

valuelength(ST, x) = 1
valuelength(::StructTypes.ArrayType, x) = length(x)

getfieldvalue(x::T, ind, (i, j), (nm1, nm2)) where {T} = getfieldvalue(StructTypes.StructType(T), x, ind, (i, j), (nm1, nm2))
getfieldvalue(ST, x, ind, (i, j), (nm1, nm2)) = x
getfieldvalue(::StructTypes.ArrayType, x, ind, (i, j), (nm1, nm2)) = getfieldvalue(x[ind], ind, (i, j), (nm1, nm2))
getfieldvalue(::Union{StructTypes.Struct, StructTypes.Mutable}, x, ind, (i, j), (nm1, nm2)) = getsubfieldvalue(Core.getfield(x, i), ind, (i, j), (nm1, nm2))
getfieldvalue(::StructTypes.DictType, x, ind, (i, j), (nm1, nm2)) = getsubfieldvalue(x[nm1], ind, (i, j), (nm1, nm2))

getsubfieldvalue(x::T, ind, (i, j), (nm1, nm2)) where {T} = getsubfieldvalue(StructTypes.StructType(T), x, ind, (i, j), (nm1, nm2))
getsubfieldvalue(ST, x, ind, (i, j), (nm1, nm2)) = x
getsubfieldvalue(::StructTypes.ArrayType, x, ind, (i, j), (nm1, nm2)) = getsubfieldvalue(x[ind], ind, (i, j), (nm1, nm2))
getsubfieldvalue(::Union{StructTypes.Struct, StructTypes.Mutable}, x, ind, (i, j), (nm1, nm2)) = getsubfieldvalue(Core.getfield(x, j), ind, (i, j), (nm1, nm2))
function getsubfieldvalue(::StructTypes.DictType, x, ind, (i, j), (nm1, nm2))
    getsubfieldvalue(x[nm2], ind, (i, j), (nm1, nm2))
end

rowtypeof(x::T) where {T} = rowtypeof(StructTypes.StructType(T), x)
rowtypeof(::StructTypes.ArrayType, x) = eltype(x)
rowtypeof(ST, x) = typeof(x)

nametypeindex!(x::T, i, nm, c) where {T} = nametypeindex!(StructTypes.StructType(T), x, i, nm, c)

function nametypeindex!(ST, x, i, nm, c)
    push!(c.names, Symbol(c.prefix, nm))
    push!(c.types, rowtypeof(x))
    push!(c.fieldindices, (c.parentindex > 0 ? c.parentindex : i, c.j))
    push!(c.fieldnames, (c.parentindex > 0 ? c.parentname : nm, nm))
    c.j += 1
    return
end

function nametypeindex!(::Union{StructTypes.Struct, StructTypes.Mutable, StructTypes.DictType}, x::T, i, nm, c) where {T}
    c2 = DeconstructClosure(T)
    c2.names = c.names
    c2.types = c.types
    c2.fieldindices = c.fieldindices
    c2.fieldnames = c.fieldnames
    c2.parentindex = i
    c2.parentname = nm
    c2.prefix = Symbol(c.prefix, StructTypes.fieldprefix(c.parenttype, nm))
    deconstructobj!(x, c2)
    return
end

function nametypeindex!(::StructTypes.ArrayType, x::T, i, nm, c) where {T}
    nametypeindex!(x[1], i, nm, c)
end

end # module
