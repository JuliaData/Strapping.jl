module Strapping

using Tables, StructTypes

struct Error <: Exception
    msg::String
end

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
    prefix = Symbol(prefix, get(StructTypes.fieldprefixes(PT), nm, Symbol()))
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
    prefix = Symbol(prefix, get(StructTypes.fieldprefixes(PT), nm, Symbol()))
    off = 0
    x = T()
    StructTypes.mapfields!(x) do i, nm, TT
        off += 1
        construct(StructTypes.StructType(TT), T, row, coloffset[] + col + i - 1, coloffset, prefix, nm, TT; kw...)
    end
    coloffset[] += off - 1
    return x
end

function construct(::StructTypes.DictType, PT, row, col, coloffset, prefix, nm, ::Type{T}; kw...) where {T}
    prefix = String(Symbol(prefix, get(StructTypes.fieldprefixes(PT), nm, Symbol())))
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

# single object or vector => Tables.rows iterator
struct DeconstructedRowsIterator{T}
    values::Vector{T}
    lens::Vector{Int}
    len::Int
    names::Vector{Symbol}
    types::Vector{Type}
    fieldindices::Vector{Tuple{Int, Int}}
    lookup::Dict{Symbol, Int}
end

Tables.isrowtable(::Type{<:DeconstructedRowsIterator}) = true

Tables.schema(x::DeconstructedRowsIterator) = Tables.Schema(x.names, x.types)

struct DeconstructedRow{T} <: Tables.AbstractRow
    x::T # a single object we're deconstructing
    index::Int # index of this specific row (may be > 1 for objects w/ collection fields)
    names::Vector{Symbol}
    fieldindices::Vector{Tuple{Int, Int}}
    lookup::Dict{Symbol, Int}
end

obj(x::DeconstructedRow) = getfield(x, :x)
ind(x::DeconstructedRow) = getfield(x, :index)
names(x::DeconstructedRow) = getfield(x, :names)
inds(x::DeconstructedRow) = getfield(x, :fieldindices)
lookup(x::DeconstructedRow) = getfield(x, :lookup)

Tables.columnnames(row::DeconstructedRow) = names(x)
Tables.getcolumn(row::DeconstructedRow, ::Type{T}, i::Int, nm::Symbol) where {T} =
    getfieldvalue(obj(row), ind(row), inds(row)[i])
Tables.getcolumn(row::DeconstructedRow, i::Int) =
    getfieldvalue(obj(row), ind(row), inds(row)[i])
Tables.getcolumn(row::DeconstructedRow, nm::Symbol) = Tables.getcolumn(row, lookup(row)[nm])

mutable struct DeconstructClosure{PT}
    len::Int
    names::Vector{Symbol}
    types::Vector{Type}
    fieldindices::Vector{Tuple{Int, Int}}
    parentindex::Int
    prefix::Symbol
    j::Int
    parenttype::Type{PT}
end

DeconstructClosure(PT) = DeconstructClosure(1, Symbol[], Type[], Tuple{Int, Int}[], 0, Symbol(), 1, PT)

function (f::DeconstructClosure)(i, nm, TT, v; kw...)
    len = valuelength(StructTypes.StructType(TT), v)
    if len > f.len
        f.len = len
    end
    nametypeindex!(v, i, nm, f)
    return
end

deconstruct(x) = deconstruct([x])

function deconstruct(values::Vector{T}) where {T}
    c = DeconstructClosure(T)
    x = values[1]
    StructTypes.foreachfield(c, x)
    len = c.len
    lens = [len]
    for i = 2:length(values)
        c.len = 1
        StructTypes.foreachfield(c, values[i])
        len += c.len
        push!(lens, c.len)
    end
    names = c.names
    lookup = Dict(nm => i for (i, nm) in enumerate(names))
    return DeconstructedRowsIterator(values, lens, len, names, c.types, c.fieldindices, lookup)
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
        return DeconstructedRow(x.values[i], j, x.names, x.fieldindices, x.lookup), (i + 1, 1, k + 1)
    else
        return DeconstructedRow(x.values[i], j, x.names, x.fieldindices, x.lookup), (i, j + 1, k + 1)
    end
end

valuelength(ST, x) = 1
valuelength(::StructTypes.ArrayType, x) = length(x)

indexedvalue(x::T, i) where {T} = indexedvalue(StructTypes.StructType(T), x, i)
indexedvalue(ST, x, i) = x
indexedvalue(::StructTypes.ArrayType, x, i) = x[i]

getfieldvalue(x::T, ind, (i, j)) where {T} = getfieldvalue(StructTypes.StructType(T), x, ind, (i, j))
getfieldvalue(ST, x, ind, (i, j)) = x
getfieldvalue(::StructTypes.ArrayType, x, ind, (i, j)) = getfieldvalue(x[ind], ind, (i, j))
getfieldvalue(::Union{StructTypes.Struct, StructTypes.Mutable}, x, ind, (i, j)) = getsubfieldvalue(Core.getfield(x, i), ind, (i, j))

getsubfieldvalue(x::T, ind, (i, j)) where {T} = getsubfieldvalue(StructTypes.StructType(T), x, ind, (i, j))
getsubfieldvalue(ST, x, ind, (i, j)) = x
getsubfieldvalue(::StructTypes.ArrayType, x, ind, (i, j)) = getsubfieldvalue(x[ind], ind, (i, j))
getsubfieldvalue(::Union{StructTypes.Struct, StructTypes.Mutable}, x, ind, (i, j)) = getsubfieldvalue(Core.getfield(x, j), ind, (i, j))

rowtypeof(x::T) where {T} = rowtypeof(StructTypes.StructType(T), x)
rowtypeof(::StructTypes.ArrayType, x) = eltype(x)
rowtypeof(ST, x) = typeof(x)

nametypeindex!(x::T, i, nm, c) where {T} = nametypeindex!(StructTypes.StructType(T), x, i, nm, c)

function nametypeindex!(ST, x, i, nm, c)
    push!(c.names, Symbol(c.prefix, nm))
    push!(c.types, rowtypeof(x))
    push!(c.fieldindices, (c.parentindex > 0 ? c.parentindex : i, c.j))
    c.j += 1
    return
end

function nametypeindex!(::Union{StructTypes.Struct, StructTypes.Mutable}, x::T, i, nm, c) where {T}
    c2 = DeconstructClosure(T)
    c2.names = c.names
    c2.types = c.types
    c2.fieldindices = c.fieldindices
    c2.parentindex = i
    c2.prefix = Symbol(c.prefix, get(StructTypes.fieldprefixes(c.parenttype), nm, Symbol()))
    StructTypes.foreachfield(c2, x)
    return
end

function nametypeindex!(::StructTypes.ArrayType, x::T, i, nm, c) where {T}
    nametypeindex!(x[1], i, nm, c)
end

end # module
