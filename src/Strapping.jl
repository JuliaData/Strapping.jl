module Strapping

using Tables, StructTypes
# using DebugPrinting

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
    x, state = construct(T, rows, row, st; kw...)
    state === nothing || (silencewarnings && println("warning: additional source rows left after reading `$T`"))
    return x
end

function construct(::Type{Vector{T}}, source; kw...) where {T}
    rows = Tables.rows(source)
    state = iterate(rows)
    A = Vector{T}(undef, 0)
    while state !== nothing
        row, st = state
        x, state = construct(T, rows, row, st; kw...)
        push!(A, x)
    end
    return A
end

# collection handler: will iterate results for collection fields
function construct(::Type{T}, rows, row, st; kw...) where {T}
    x = construct(StructTypes.StructType(T), T, row; kw...)
    idprop = StructTypes.idproperty(T)
    # @print 2 idprop
    if idprop !== :_
        id = Tables.getcolumn(row, idprop)
        state = iterate(rows, st)
        if state !== nothing
            while state !== nothing
                row, st = state
                Tables.getcolumn(row, idprop) == id || break
                construct!(StructTypes.StructType(T), x, row; kw...)
                state = iterate(rows, st)
            end
        end
    else
        state = iterate(rows, st)
    end
    return x, state
end

# aggregate handlers
# construct versions construct initial object
# construct! versions take existing object and append additional elements to collection fields
function construct(::StructTypes.CustomStruct, ::Type{T}, row, prefix=Symbol(), offset=Ref(0); kw...) where {T}
    S = StructTypes.lowertype(T)
    x = construct(StructTypes.StructType(S), S, row, prefix, offset; kw...)
    return StructTypes.construct(T, x)
end

function construct(::StructTypes.Struct, ::Type{T}, row, prefix=Symbol(), offset=Ref(0); kw...) where {T}
    # @show (T, T isa Union, prefix, offset)
    return StructTypes.construct(T) do i, nm, TT
        x = construct(StructTypes.StructType(TT), TT, row, Symbol(prefix, StructTypes.fieldprefix(T, nm)), offset, offset[] + 1, Symbol(prefix, nm); kw...)
        # @print 3 x
        return x
    end
end

function construct(::ST, ::Type{T}, row, prefix, offset, i, nm; kw...) where {ST <: Union{StructTypes.Struct, StructTypes.Mutable, StructTypes.CustomStruct}, T}
    # @print 3 (T, prefix, offset, i, nm)
    if T isa Union
        construct(ST(), Any, row, prefix, offset, i, nm; kw...)
    else
        construct(ST(), T, row, prefix, offset; kw...)
    end
end

function construct!(::StructTypes.CustomStruct, x::T, row, prefix=Symbol(), offset=Ref(0); kw...) where {T}
    y = StructTypes.lower(x)
    return construct!(StructTypes.StructType(y), y, row, prefix, offset; kw...)
end

function construct!(::Union{StructTypes.Struct, StructTypes.Mutable}, x::T, row, prefix=Symbol(), offset=Ref(0); kw...) where {T}
    # @print 3 (T, prefix, offset)
    return StructTypes.foreachfield(x) do i, nm, TT, v
        y = construct!(StructTypes.StructType(TT), v, row, Symbol(prefix, StructTypes.fieldprefix(T, nm)), offset, offset[] + 1, Symbol(prefix, nm); kw...)
        # @print 3 y
        return y
    end
end

function construct!(::ST, x::T, row, prefix, offset, i, nm; kw...) where {ST <: Union{StructTypes.Struct, StructTypes.Mutable, StructTypes.CustomStruct}, T}
    # @print 3 (T, prefix, offset, i, nm)
    construct!(ST(), x, row, prefix, offset; kw...)
end

function construct(::StructTypes.Mutable, ::Type{T}, row, prefix=Symbol(), offset=Ref(0); kw...) where {T}
    # @print 3 (T, prefix, offset)
    x = T()
    StructTypes.mapfields!(x) do i, nm, TT
        y = construct(StructTypes.StructType(TT), TT, row, Symbol(prefix, StructTypes.fieldprefix(T, nm)), offset, offset[] + 1, Symbol(prefix, nm); kw...)
        # @print 3 y
        return y
    end
    return x
end

# fallback for scalars when called from "aggregate" method
construct(ST, T, row, prefix=Symbol(), offset=Ref(0); kw...) = construct(ST, T, row, prefix, offset, 1, Tables.columnnames(row)[1]; kw...)
construct!(ST, x, row, prefix=Symbol(), offset=Ref(0); kw...) = nothing
function construct!(ST, x, row, prefix, offset, i, nm; kw...)
    offset[] += 1
    nothing
end

function construct(::StructTypes.NumberType, ::Type{T}, row, prefix, offset, i, nm; kw...) where {T}
    offset[] += 1
    StructTypes.construct(T, StructTypes.numbertype(T)(getvalue(row, T, i, nm)))
end

# fallback for scalars
function construct(ST, ::Type{T}, row, prefix, offset, i, nm; kw...) where {T}
    offset[] += 1
    StructTypes.construct(T, getvalue(row, T, i, nm))
end

function construct(::StructTypes.Struct, ::Type{Any}, row, prefix, offset, i, nm; kw...)
    offset[] += 1
    Tables.getcolumn(row, nm)
end

# scalar handlers (take a `col` argument)
function getvalue(row, ::Type{T}, col::Int, nm::Symbol) where {T}
    # @print 3 (T, col, nm)
    Tables.getcolumn(row, T, col, nm)
end
function getvalue(row, ::Type{T}, col::Int, nm::Int) where {T}
    # @print 3 (T, col, nm)
    Tables.getcolumn(row, nm)
end

# default aggregate
construct(::StructTypes.Struct, ::Type{Any}, row, prefix=Symbol(), offset=Ref(0); kw...) =
    construct(StructTypes.DictType(), Dict{String, Any}, row; kw...)

# note these don't take `prefix`/`offset` args because we don't expect them to ever be called with a prefix/offset (i.e. only called from line 117)
construct(::StructTypes.DictType, ::Type{T}, row; kw...) where {T} = construct(StructTypes.DictType(), T, row, Symbol, Any; kw...)
construct(::StructTypes.Struct, ::Type{T}, row; kw...) where {T <: NamedTuple} = construct(StructTypes.DictType(), T, row, Symbol, Any; kw...)
construct(::StructTypes.DictType, ::Type{T}, row; kw...) where {T <: NamedTuple} = construct(StructTypes.DictType(), T, row, Symbol, Any; kw...)
construct(::StructTypes.DictType, ::Type{Dict}, row; kw...) = construct(StructTypes.DictType(), Dict, row, String, Any; kw...)
construct(::StructTypes.DictType, ::Type{T}, row; kw...) where {T <: AbstractDict} = construct(StructTypes.DictType(), T, row, keytype(T), valtype(T); kw...)

const AggregateStructTypes = Union{StructTypes.Struct, StructTypes.Mutable, StructTypes.DictType, StructTypes.ArrayType}
@noinline aggregatefieldserror(T) = error("$(StructTypes.StructType(T)) $T not allowed as aggregate field when calling Strapping.construct")

function construct(::StructTypes.DictType, ::Type{T}, row, ::Type{K}, ::Type{V}; kw...) where {T, K, V}
    # check for aggregate types as V
    # the problem is we're already treating the DictType as the aggregate, so
    # it "slurps" all columns as Dict keys, making it
    # impossible to distinguish between multiple aggregate type V
    (V === Any || !(StructTypes.StructType(V) isa AggregateStructTypes)) || aggregatefieldserror(T)
    x = Dict{K, V}()
    for (i, nm) in enumerate(Tables.columnnames(row))
        val = construct(StructTypes.StructType(V), V, row, Symbol(), Ref(0), i, nm; kw...)
        if K === Symbol
            x[nm] = val
        else
            x[StructTypes.construct(K, String(nm))] = val
        end
    end
    return StructTypes.construct(T, x; kw...)
end

# construct(::StructTypes.Struct, ::Type{T}, row, prefix, offset, i, nm; kw...) where {T <: NamedTuple} = construct(StructTypes.DictType(), T, row, prefix, offset, i, nm; kw...)
construct(::StructTypes.DictType, ::Type{T}, row, prefix, offset, i, nm; kw...) where {T} = construct(StructTypes.DictType(), T, row, prefix, offset, i, nm, Symbol, Any; kw...)
construct(::StructTypes.Struct, ::Type{T}, row, prefix, offset, i, nm; kw...) where {T <: NamedTuple} = construct(StructTypes.DictType(), T, row, prefix, offset, i, nm, Symbol, Any; kw...)
construct(::StructTypes.DictType, ::Type{T}, row, prefix, offset, i, nm; kw...) where {T <: NamedTuple} = construct(StructTypes.DictType(), T, row, prefix, offset, i, nm, Symbol, Any; kw...)
construct(::StructTypes.DictType, ::Type{Dict}, row, prefix, offset, i, nm; kw...) = construct(StructTypes.DictType(), Dict, row, prefix, offset, i, nm, String, Any; kw...)
construct(::StructTypes.DictType, ::Type{T}, row, prefix, offset, i, nm; kw...) where {T <: AbstractDict} = construct(StructTypes.DictType(), T, row, prefix, offset, i, nm, keytype(T), valtype(T); kw...)

function construct(::StructTypes.DictType, ::Type{T}, row, prefix, offset, i, nm, ::Type{K}, ::Type{V}; kw...) where {T, K, V}
    (V === Any || !(StructTypes.StructType(V) isa AggregateStructTypes)) || aggregatefieldserror(T)
    x = Dict{K, V}()
    nms = Tables.columnnames(row)
    for j = i:length(nms)
        nm = nms[j]
        val = construct(StructTypes.StructType(V), V, row, prefix, offset, j, nm; kw...)
        if K === Symbol
            x[nm] = val
        else
            x[StructTypes.construct(K, String(nm))] = val
        end
    end
    return StructTypes.construct(T, x; kw...)
end

function construct!(::StructTypes.DictType, x::T, row; kw...) where {T}
    for (i, nm) in enumerate(Tables.columnnames(row))
        v = x[nm]
        V = typeof(v)
        construct!(StructTypes.StructType(V), v, row, Symbol(), Ref(0), i, nm; kw...)
    end
    return
end

construct(::StructTypes.ArrayType, ::Type{T}, row; kw...) where {T} =
    _construct(StructTypes.ArrayType(), T, row, Base.IteratorEltype(T) == Base.HasEltype() ? eltype(T) : Any; kw...)
construct(::StructTypes.ArrayType, ::Type{Tuple}, row; kw...) =
    _construct(StructTypes.ArrayType(), Tuple, row, Any; kw...)

function _construct(::StructTypes.ArrayType, ::Type{T}, row, ::Type{eT}; kw...) where {T, eT}
    # disallow aggregate eT, same problem as DictType; we're treating the ArrayType as our aggregate
    (eT === Any || !(StructTypes.StructType(eT) isa AggregateStructTypes)) || aggregatefieldserror(T)
    nms = Tables.columnnames(row)
    N = length(nms)
    x = Vector{eT}(undef, N)
    for (i, nm) in enumerate(nms)
        x[i] = construct(StructTypes.StructType(eT), eT, row, Symbol(), Ref(0), i, nm; kw...)
    end
    return StructTypes.construct(T, x; kw...)
end

function construct!(::StructTypes.ArrayType, x::T, row; kw...) where {T}
    for (i, nm) in enumerate(Tables.columnnames(row))
        v = x[i]
        V = typeof(v)
        construct!(StructTypes.StructType(V), v, row, Symbol(), Ref(0), i, nm; kw...)
    end
    return
end

function construct(::StructTypes.ArrayType, ::Type{T}, row, ::Type{eT}; kw...) where {T <: Tuple, eT}
    return StructTypes.construct(T) do i, nm, TT
        construct(StructTypes.StructType(TT), TT, row, Symbol(), Ref(0); kw...)
    end
end

function construct(::StructTypes.ArrayType, ::Type{T}, row, prefix, offset, i, nm; kw...) where {T}
    eT = Base.IteratorEltype(T) == Base.HasEltype() ? eltype(T) : Any
    return StructTypes.construct(T, [construct(StructTypes.StructType(eT), eT, row, prefix, offset, i, nm; kw...)]; kw...)
end

function construct!(::StructTypes.ArrayType, x::T, row, prefix, offset, i, nm; kw...) where {T}
    eT = Base.IteratorEltype(T) == Base.HasEltype() ? eltype(T) : Any
    push!(x, construct(StructTypes.StructType(eT), eT, row, prefix, offset, i, nm; kw...))
    return
end

# deconstruct

"""
    Strapping.deconstruct(x::T)
    Strapping.deconstruct(x::Vector{T})

The inverse of `Strapping.construct`, where an object instance `x::T` or `Vector` of objects `x::Vector{T}` is "deconstructed" into a Tables.jl-compatible row iterator. This works following the same patterns outlined in `Strapping.construct` with regards to `ArrayType` and aggregate fields. Specifically, `ArrayType` fields will cause multiple rows to be outputted, one row per collection element, with other scalar fields being repeated in each row. Note this requires no more than one collection field, since otherwise it's ambiguous on how to repeat rows. Similarly for aggregate fields, the field prefix will be used (`StructTypes.fieldprefix`) and nested aggregates will all be flattened into a single list of column names with aggregate prefixes.

In general, this allows outputting any "object" as a 2D table structure that could be stored in any Tables.jl-compatible sink format, e.g. csv file, sqlite table, mysql database table, arrow file, etc.

Note that currently the code requires collection fields to be indexable (i.e. supports `getindex` and `length`) as well as 
to have `eltype` defined. Also note that if any collection fields are empty, it will result in the output columns having `Union{T, Missing}`
types since the non-collection fields will still be output as a single row, with collection field(s) being `missing`.
"""
function deconstruct end

mutable struct FieldNode
    index::Int
    name::Symbol
    subfield::Union{FieldNode, Nothing}
end

Base.copy(x::FieldNode) = FieldNode(x.index, x.name, x.subfield === nothing ? nothing : copy(x.subfield))

# recurse until find empty subfield and set `sub`
function setsubfield!(fn::FieldNode, sub::FieldNode)
    if fn.subfield === nothing
        fn.subfield = sub
    else
        setsubfield!(fn.subfield, sub)
    end
    return
end

getfieldvalue(x::T, ind, fn) where {T} = getfieldvalue(StructTypes.StructType(T), x, ind, fn)

function getfieldvalue(::StructTypes.DictType, x, ind, fn)
    val = x[fn.name]
    return getfieldvalue(val, ind, fn.subfield)
end

getfieldvalue(::StructTypes.ArrayType, x, ind, fn) = isempty(x) ? missing : getfieldvalue(x[ind], 0, fn)

function getfieldvalue(::Union{StructTypes.Struct, StructTypes.Mutable}, x, ind, fn)
    val = Core.getfield(x, fn.index)
    return getfieldvalue(val, ind, fn.subfield)
end

function getfieldvalue(::StructTypes.CustomStruct, x, ind, fn)
    y = StructTypes.lower(x)
    return getfieldvalue(StructTypes.StructType(y), y, ind, fn)
end

function getfieldvalue(ST, x, ind, fn)
    @assert fn === nothing
    return x
end

# single object or vector => Tables.rows iterator
struct DeconstructedRowsIterator{T, A}
    values::A
    lens::Vector{Int}
    len::Int
    names::Vector{Symbol}
    types::Vector{Type}
    fields::Vector{FieldNode}
    lookup::Dict{Symbol, Int}
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
        return DeconstructedRow(x.values[i], j, x.names, x.fields, x.lookup), (i + 1, 1, k + 1)
    else
        return DeconstructedRow(x.values[i], j, x.names, x.fields, x.lookup), (i, j + 1, k + 1)
    end
end

Tables.isrowtable(::Type{<:DeconstructedRowsIterator}) = true
Tables.schema(x::DeconstructedRowsIterator) = Tables.Schema(x.names, x.types)

struct DeconstructedRow{T} <: Tables.AbstractRow
    x::T # a single object we're deconstructing
    index::Int # index of this specific row (may be > 1 for objects w/ collection fields)
    names::Vector{Symbol}
    fields::Vector{FieldNode}
    lookup::Dict{Symbol, Int}
end

obj(x::DeconstructedRow) = getfield(x, :x)
ind(x::DeconstructedRow) = getfield(x, :index)
names(x::DeconstructedRow) = getfield(x, :names)
fields(x::DeconstructedRow) = getfield(x, :fields)
lookup(x::DeconstructedRow) = getfield(x, :lookup)

Tables.columnnames(row::DeconstructedRow) = names(row)
Tables.getcolumn(row::DeconstructedRow, ::Type{T}, i::Int, nm::Symbol) where {T} =
    getfieldvalue(obj(row), ind(row), fields(row)[i])
Tables.getcolumn(row::DeconstructedRow, i::Int) =
    getfieldvalue(obj(row), ind(row), fields(row)[i])
Tables.getcolumn(row::DeconstructedRow, nm::Symbol) = Tables.getcolumn(row, lookup(row)[nm])

mutable struct DeconstructClosure{}
    len::Int
    names::Vector{Symbol}
    types::Vector{Type}
    fields::Vector{FieldNode}
    prefix::Symbol
    fieldnode::Union{FieldNode, Nothing}
    i::Int
    processedArrayType::Bool
    emptyArrayType::Bool
    parentType::Type
end

DeconstructClosure() = DeconstructClosure(1, Symbol[], Type[], FieldNode[], Symbol(), nothing, 1, false, false, Any)

# if we're direct root child, sub is full field ancestry (i.e. name/index are name/index of root object)
# if we're further nested than direct root child,
# we make a copy of the fieldnode ancestry and set
# our new node as new leaf node
function getfieldnode(f::DeconstructClosure, sub::FieldNode)
    if f.fieldnode === nothing
        return sub
    else
        fn = copy(f.fieldnode)
        setsubfield!(fn, sub)
        return fn
    end
end

function reset!(f::DeconstructClosure)
    f.i = 1
    f.len = 1
    f.processedArrayType = false
    f.emptyArrayType = false
    return
end

(f::DeconstructClosure)(x::T) where {T} = f(StructTypes.StructType(T), x)

struct EmptyArrayTypeValue end

function (f::DeconstructClosure)(::Union{StructTypes.Struct, StructTypes.Mutable}, x::T) where {T}
    # x is root object
    reset!(f)
    f.parentType = T
    StructTypes.foreachfield(x) do i, nm, TT, v
        # each root field is a separate prefix/fieldnode ancestry branch
        f.prefix = Symbol()
        f.fieldnode = nothing
        f(i, nm, TT, v)
    end
    return
end

function (f::DeconstructClosure)(::StructTypes.CustomStruct, x::T) where {T}
    y = StructTypes.lower(x)
    return f(StructTypes.StructType(y), y)
end

function (f::DeconstructClosure)(::StructTypes.DictType, x::T) where {T}
    # x is root object
    reset!(f)
    f.parentType = T
    i = 1
    for (k, v) in StructTypes.keyvaluepairs(x)
        # each root field is a separate prefix/fieldnode ancestry branch
        f.prefix = Symbol()
        f.fieldnode = nothing
        f(i, k, typeof(v), v)
        i += 1
    end
    return
end

(f::DeconstructClosure)(i, nm, TT, v; kw...) = f(StructTypes.StructType(TT), i, nm, TT, v)
(f::DeconstructClosure)(i, nm, TT; kw...) = f(StructTypes.StructType(TT), i, nm, TT, EmptyArrayTypeValue())
function (f::DeconstructClosure)(::Union{StructTypes.Struct, StructTypes.Mutable}, i, nm, TT, v)
    f.prefix = Symbol(f.prefix, StructTypes.fieldprefix(f.parentType, nm))
    f.parentType = TT
    f.fieldnode = getfieldnode(f, FieldNode(i, nm, nothing))
    StructTypes.foreachfield(f, v)
    return
end

function (f::DeconstructClosure)(::Union{StructTypes.Struct, StructTypes.Mutable}, i, nm, TT, ::EmptyArrayTypeValue)
    f.prefix = Symbol(f.prefix, StructTypes.fieldprefix(f.parentType, nm))
    f.parentType = TT
    f.fieldnode = getfieldnode(f, FieldNode(i, nm, nothing))
    StructTypes.foreachfield(f, TT)
    return
end

function (f::DeconstructClosure)(::StructTypes.DictType, i, nm, TT, x)
    f.prefix = Symbol(f.prefix, StructTypes.fieldprefix(f.parentType, nm))
    f.parentType = TT
    f.fieldnode = getfieldnode(f, FieldNode(i, nm, nothing))
    i = 1
    for (k, v) in StructTypes.keyvaluepairs(x)
        f(i, k, typeof(v), v)
        i += 1
    end
    return
end

(f::DeconstructClosure)(::StructTypes.DictType, i, nm, TT, ::EmptyArrayTypeValue) =
    error("unable to detect field types from empty StructTypes.ArrayType object with StructType.DictType eltype (since DictType keys/values are flattened each as fields and should always contain the same keys from element to element)")

@noinline multiplearraytype(TT) = error("multiple StructTypes.ArrayType objects detected during Strapping.deconstruct; only one is allowed for determining repeated deconstructed rows; see StructTypes.excludes(T) = (:nm,) for excluding fields from deconstructing consideration")
@noinline lengthrequired(T) = error("length(::$T) is required for Strapping.deconstruct")
@noinline eltyperequired(T) = erorr("eltype($T) is required for Strapping.deconstruct")

function (f::DeconstructClosure)(::StructTypes.ArrayType, i, nm, TT, v)
    f.processedArrayType && multiplearraytype(TT)
    f.processedArrayType = true
    Base.haslength(v) || lengthrequired(TT)
    f.len = max(1, length(v))
    T = eltype(v)
    st = iterate(v)
    f.emptyArrayType = st === nothing
    f(i, nm, T, st === nothing ? EmptyArrayTypeValue() : st[1])
    return
end

function (f::DeconstructClosure)(ST, i, nm, TT, v)
    if f.i > length(f.names)
        # first time deconstructing obj
        push!(f.names, Symbol(f.prefix, nm))
        push!(f.types, f.emptyArrayType ? Union{Missing, TT} : TT)
        push!(f.fields, getfieldnode(f, FieldNode(i, nm, nothing)))
    else
        # promote field type if needed
        T = f.types[f.i]
        TTT = promote_type(TT, T)
        if !(TTT <: T)
            f.types[f.i] = f.emptyArrayType ? Union{Missing, TTT} : TTT
        end
    end
    f.i += 1
    return
end

deconstruct(x) = deconstruct([x])

function deconstruct(values::A) where {T, A <: AbstractVector{T}}
    c = DeconstructClosure()
    lens = Int[]
    len = 0
    i = 0
    for x in values
        c(x)
        push!(lens, c.len)
        len += c.len
        i += 1
    end
    lookup = Dict(nm => i for (i, nm) in enumerate(c.names))
    return DeconstructedRowsIterator{T, A}(values, lens, len, c.names, c.types, c.fields, lookup)
end

end # module
