struct DeconstructedRows{T,A,S}
    values::A
    plan::Plan
    lengths::Vector{Int}
    length::Int
    style::S
end

struct DeconstructedRow{T,P,S} <: Tables.AbstractRow
    value::T
    index::Int
    plan::P
    style::S
end

struct Absent end
const ABSENT = Absent()

Base.IteratorEltype(::Type{<:DeconstructedRows}) = Base.HasEltype()
Base.eltype(::Type{DeconstructedRows{T,A,S}}) where {T,A,S} = DeconstructedRow{T,Plan,S}
Base.length(rows::DeconstructedRows) = rows.length
Base.isempty(rows::DeconstructedRows) = rows.length == 0

function Base.iterate(rows::DeconstructedRows, state = (1, 1))
    object, index = state
    object > length(rows.values) && return nothing
    row = DeconstructedRow(rows.values[object], index, rows.plan, rows.style)
    next = index == rows.lengths[object] ? (object + 1, 1) : (object, index + 1)
    return row, next
end

Tables.istable(::Type{<:DeconstructedRows}) = true
Tables.rowaccess(::Type{<:DeconstructedRows}) = true
Tables.rows(rows::DeconstructedRows) = rows
Tables.schema(rows::DeconstructedRows) = Tables.Schema(rows.plan.names, rows.plan.types)

@inline _value(row::DeconstructedRow) = getfield(row, :value)
@inline _index(row::DeconstructedRow) = getfield(row, :index)
@inline _plan(row::DeconstructedRow) = getfield(row, :plan)
@inline _style(row::DeconstructedRow) = getfield(row, :style)

Tables.columnnames(row::DeconstructedRow) = _plan(row).names
Tables.getcolumn(row::DeconstructedRow, index::Int) =
    _columnvalue(_value(row), _index(row), _plan(row).columns[index], _style(row))
Tables.getcolumn(row::DeconstructedRow, name::Symbol) =
    Tables.getcolumn(row, _plan(row).lookup[name])
Tables.getcolumn(row::DeconstructedRow, ::Type, index::Int, name::Symbol) =
    Tables.getcolumn(row, index)

function _pathvalue(value, path, row, collection_at)
    current = value
    for (depth, index) in pairs(path)
        _isnull(current) && return ABSENT
        current = getfield(current, index)
        if depth == collection_at
            _isnull(current) && return ABSENT
            Base.haslength(current) ||
                _error("`$(typeof(current))` must implement `length`")
            isempty(current) && return ABSENT
            hasmethod(getindex, Tuple{typeof(current),Int}) ||
                _error("`$(typeof(current))` must implement integer `getindex`")
            row <= length(current) || return ABSENT
            current = current[row]
        end
    end
    return current
end

function _columnvalue(value, row, column::ColumnPlan, style)
    result = _pathvalue(value, column.path, row, column.collection_at)
    result === ABSENT && return missing
    if column.operation == KIND_COLUMN
        return result === nothing ? :nothing : result === missing ? :missing : :value
    elseif column.operation == LENGTH_COLUMN
        _isnull(result) && return 0
        Base.haslength(result) || _error("`$(typeof(result))` must implement `length`")
        return length(result)
    else
        return StructUtils.lower(style, result, column.tags)
    end
end

function _rootfield(value, field::FieldPlan)
    current = value
    for index in field.path
        _isnull(current) && return current
        current = getfield(current, index)
    end
    return current
end

function _rowcount(value, collection::Union{Nothing,FieldPlan})
    collection === nothing && return 1
    values = _rootfield(value, collection)
    _isnull(values) && return 1
    Base.haslength(values) || _error("`$(typeof(values))` must implement `length`")
    return max(1, length(values))
end

function _validate_object_ids(values, plan::Plan, style)
    length(values) <= 1 && return
    plan.collection === nothing && return
    plan.id_column === nothing && _error(
        "deconstructing multiple `$(plan.target)` values with a flattened collection requires one root field tagged `id=true`",
    )
    column = plan.columns[plan.lookup[plan.id_column]]
    ids = Any[]
    for value in values
        id = _columnvalue(value, 1, column, style)
        any(existing -> isequal(existing, id), ids) && _error(
            "root IDs must be unique when deconstructing multiple values; duplicate $(repr(id))",
        )
        push!(ids, id)
    end
    return
end

"""
    Strapping.deconstruct(value; style=Strapping.Style())
    Strapping.deconstruct(values::AbstractVector; style=Strapping.Style())

Return a Tables.jl row table that flattens one struct or a vector of structs.
Nested concrete structs use prefixed columns. One concrete collection may
expand into repeated rows. Nullable nested values include a `__kind` column,
and collections include a `__length` column, so `nothing`, `missing`, empty
collections, and present values round-trip without ambiguity.
"""
deconstruct(value::T; style::StructUtils.StructStyle = DEFAULT_STYLE) where {T} =
    deconstruct(T[value]; style)

function deconstruct(
    values::A;
    style::StructUtils.StructStyle = DEFAULT_STYLE,
) where {T,A<:AbstractVector{T}}
    plan = _plan(T, style)
    _validate_object_ids(values, plan, style)
    lengths = [_rowcount(value, plan.collection) for value in values]
    return DeconstructedRows{T,A,typeof(style)}(values, plan, lengths, sum(lengths), style)
end
