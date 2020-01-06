module ORM

using DBInterface, StructTypes

struct ORMException
    msg::String
end

select(conn::DBInterface.Connection, sql::AbstractString, ::Type{T}, args...; kw...) where {T} = select(DBInterface.prepare(conn, sql), T, args...; kw...)

function select(stmt::DBInterface.Statement, ::Type{T}, args...; kw...) where {T}
    results = DBInterface.execute!(stmt, args...; kw...)
    state = iterate(results)
    state === nothing && throw(ORMException("can't select `$T` from empty resultset"))
    row, st = state
    x, state = select(results, st, row, T; kw...)
    state === nothing || println("warning: additional result rows in query after reading `$T`")
    return x
end

function select(stmt::DBInterface.Statement, ::Type{Vector{T}}, args...; kw...) where {T}
    results = DBInterface.execute!(stmt, args...; kw...)
    state = iterate(results)
    A = Vector{T}(undef, 0)
    while state !== nothing
        row, st = state
        x, state = select(results, st, row, T; kw...)
        push!(A, x)
    end
    return A
end

# collection handler: will iterate results for collection fields
function select(results, st, row, ::Type{T}; kw...) where {T}
    x = select(StructTypes.StructType(T), row, T; kw...)
    idprop = StructTypes.idproperty(T)
    if idprop !== :_
        id = getproperty(row, idprop)
        state = iterate(results, st)
        if state !== nothing
            while state !== nothing
                row, st = state
                getproperty(row, idprop) == id || break
                select!(StructTypes.StructType(T), row, x; kw...)
                state = iterate(results, st)
            end
        end
    else
        state = iterate(results, st)
    end
    return x, state
end

# aggregate handlers (don't take specific `col`/`nm` arguments)
# select versions construct initial object
# select! versions take existing object and append additional elements to collection fields
function select(::StructTypes.Struct, row, ::Type{T}; kw...) where {T}
    return StructTypes.construct(T) do i, nm, TT
        select(StructTypes.StructType(TT), row, i, nm, TT; kw...)
    end
end

function select!(::StructTypes.Struct, row, x::T; kw...) where {T}
    return StructTypes.foreachfield(x) do i, nm, TT, v
        select!(StructTypes.StructType(TT), row, i, nm, TT, v; kw...)
    end
end

function select(::StructTypes.Mutable, row, ::Type{T}; kw...) where {T}
    x = T()
    StructTypes.mapfields!(x) do i, nm, TT
        y = select(StructTypes.StructType(TT), row, i, nm, TT; kw...)
        return y
    end
    return x
end

function select!(::StructTypes.Mutable, row, x::T; kw...) where {T}
    return StructTypes.foreachfield(x) do i, nm, TT, v
        select!(StructTypes.StructType(TT), row, i, nm, TT, v; kw...)
    end
end

# default aggregate
select(::StructTypes.Struct, row, ::Type{Any}; kw...) =
    select(StructTypes.DictType(), row, Dict{String, Any}; kw...)

select(::StructTypes.DictType, row, ::Type{T}; kw...) where {T} = select(StructTypes.DictType(), row, T, Symbol, Any; kw...)
select(::StructTypes.DictType, row, ::Type{T}; kw...) where {T <: NamedTuple} = select(StructTypes.DictType(), row, T, Symbol, Any; kw...)
select(::StructTypes.DictType, row, ::Type{Dict}; kw...) = select(StructTypes.DictType(), row, Dict, String, Any; kw...)
select(::StructTypes.DictType, row, ::Type{T}; kw...) where {T <: AbstractDict} = select(StructTypes.DictType(), row, T, keytype(T), valtype(T); kw...)

function select(::StructTypes.DictType, row, ::Type{T}, ::Type{K}, ::Type{V}; kw...) where {T, K, V}
    #TODO: disallow aggregate types as V?
    x = Dict{K, V}()
    for (i, nm) in enumerate(propertynames(row))
        val = select(StructTypes.StructType(V), row, i, nm, V; kw...)
        if K == Symbol
            x[nm] = val
        else
            x[StructTypes.construct(K, String(nm))] = val
        end
    end
    return StructTypes.construct(T, x; kw...)
end

function select!(::StructTypes.DictType, row, x; kw...)
    for (i, nm) in enumerate(propertynames(row))
        v = x[nm]
        V = typeof(v)
        select!(StructTypes.StructType(V), row, i, nm, V, v; kw...)
    end
    return
end

select(::StructTypes.ArrayType, row, ::Type{T}; kw...) where {T} = select(StructTypes.ArrayType(), row, T, Base.IteratorEltype(T) == Base.HasEltype() ? eltype(T) : Any; kw...)
select(::StructTypes.ArrayType, row, ::Type{T}, ::Type{eT}; kw...) where {T, eT} = selectarray(row, T, eT; kw...)
select(::StructTypes.ArrayType, row, ::Type{Tuple}, ::Type{eT}; kw...) where {eT} = selectarray(row, Tuple, eT; kw...)

function selectarray(row, ::Type{T}, ::Type{eT}; kw...) where {T, eT}
    #TODO: disallow aggregate eT?
    nms = propertynames(row)
    N = length(nms)
    x = Vector{eT}(undef, N)
    for (i, nm) in enumerate(nms)
        x[i] = select(StructTypes.StructType(eT), row, i, nm, eT; kw...)
    end
    return StructTypes.construct(T, x; kw...)
end

function select!(::StructTypes.ArrayType, row, x; kw...)
    for (i, nm) in enumerate(propertynames(row))
        v = x[i]
        V = typeof(v)
        select!(StructTypes.StructType(V), row, i, nm, V, v; kw...)
    end
    return
end

function select(::StructTypes.ArrayType, row, ::Type{T}, ::Type{eT}; kw...) where {T <: Tuple, eT}
    return StructTypes.construct(T) do i, nm, TT
        select(StructTypes.StructType(TT), row, i, nm, TT; kw...)
    end
end

# selecting a single scalar from a row
function select(::StructTypes.StringType, row, ::Type{T}; kw...) where {T}
    return StructTypes.construct(T, row[1])
end

function select(::StructTypes.NumberType, row, ::Type{T}; kw...) where {T}
    return StructTypes.construct(T, StructTypes.numbertype(T)(row[1]))
end

function select(::StructTypes.BoolType, row, ::Type{T}; kw...) where {T}
    return StructTypes.construct(T, row[1])
end

function select(::StructTypes.NullType, row, ::Type{T}; kw...) where {T}
    return StructTypes.construct(T, row[1])
end

## field selection
# Struct field
function select(::StructTypes.Struct, row, col, nm, ::Type{T}; kw...) where {T}
    prefix = StructTypes.fieldprefix(T)
    return StructTypes.construct(T) do i, nm, TT
        select(StructTypes.StructType(TT), row, col + i - 1, Symbol(prefix, nm), TT; kw...)
    end
end

function select(::StructTypes.Mutable, row, col, nm, ::Type{T}; kw...) where {T}
    prefix = StructTypes.fieldprefix(T)
    x = T()
    StructTypes.mapfields!(x) do i, nm, TT
        select(StructTypes.StructType(TT), row, col + i - 1, Symbol(prefix, nm), TT; kw...)
    end
    return x
end

function select(::StructTypes.DictType, row, col, nm, ::Type{T}; kw...) where {T}
    prefix = String(StructTypes.fieldprefix(T))
    x = Dict{K, V}()
    for (i, nm) in enumerate(propertynames(row))
        if startswith(String(nm), prefix)
            val = select(StructTypes.StructType(V), row, i, nm, V; kw...)
            if K == Symbol
                x[nm] = val
            else
                x[StructTypes.construct(K, String(nm))] = val
            end
        end
    end
    return StructTypes.construct(T, x; kw...)
end

function select(::StructTypes.ArrayType, row, col, nm, ::Type{T}; kw...) where {T}
    eT = Base.IteratorEltype(T) == Base.HasEltype() ? eltype(T) : Any
    return StructTypes.construct(T, [select(StructTypes.StructType(eT), row, col, nm, eT; kw...)]; kw...)
end

function select!(::StructTypes.ArrayType, row, col, nm, ::Type{T}, v; kw...) where {T}
    eT = Base.IteratorEltype(T) == Base.HasEltype() ? eltype(T) : Any
    push!(v, select(StructTypes.StructType(eT), row, col, nm, eT; kw...))
    return
end

# for all other select!, we ignore
select!(ST, row, col, nm, T, v; kw...) = nothing

# scalar handlers (take a `col` argument)
getvalue(row, col::Int, nm::Symbol) = getproperty(row, nm)
getvalue(row, col::Int, nm::Int) = getindex(row, nm)

select(::StructTypes.Struct, row, col, nm, ::Type{Any}; kw...) = getvalue(row, col, nm)
select(::StructTypes.Struct, row, col, nm, U::Union; kw...) = getvalue(row, col, nm)
select(::StructTypes.StringType, row, col, nm, ::Type{T}; kw...) where {T} = StructTypes.construct(T, getvalue(row, col, nm))
select(::StructTypes.NumberType, row, col, nm, ::Type{T}; kw...) where {T} =
    StructTypes.construct(T, StructTypes.numbertype(T)(getvalue(row, col, nm)))
select(::StructTypes.BoolType, row, col, nm, ::Type{T}; kw...) where {T} = StructTypes.construct(T, getvalue(row, col, nm))
select(::StructTypes.NullType, row, col, nm, ::Type{T}; kw...) where {T} = StructTypes.construct(T, getvalue(row, col, nm))

end # module
