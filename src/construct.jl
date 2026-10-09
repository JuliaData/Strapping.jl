struct InputRows{R}
    rows::R
    names::Set{Symbol}
end

struct StructSource{R} <: Tables.AbstractRow
    input::InputRows{R}
    fields::Vector{FieldPlan}
    row::Int
end

struct CollectionSource{R} <: AbstractVector{Any}
    input::InputRows{R}
    field::FieldPlan
    length::Int
end

struct TaggedCollectionSource{R} <: AbstractVector{Any}
    input::InputRows{R}
    field::FieldPlan
    length::Int
end

struct ExplicitNothing end
struct ExplicitMissing end

StructUtils.make(style::StructUtils.StructStyle, ::Type{T}, ::ExplicitNothing) where {T} =
    (nothing, StructUtils.defaultstate(style))
StructUtils.make(
    style::StructUtils.StructStyle,
    ::Type{T},
    ::ExplicitNothing,
    tags,
) where {T} = (nothing, StructUtils.defaultstate(style))
StructUtils.make(style::StructUtils.StructStyle, ::Type{T}, ::ExplicitMissing) where {T} =
    (missing, StructUtils.defaultstate(style))
StructUtils.make(
    style::StructUtils.StructStyle,
    ::Type{T},
    ::ExplicitMissing,
    tags,
) where {T} = (missing, StructUtils.defaultstate(style))
StructUtils.make(
    style::StructUtils.StructStyle,
    ::Type{T},
    source::TaggedCollectionSource,
) where {T} = _maketaggedcollection(style, T, source)

@inline function _preservenull(value, nulls::UInt8)
    nulls == (HAS_NOTHING | HAS_MISSING) || return value
    value === nothing && return ExplicitNothing()
    value === missing && return ExplicitMissing()
    return value
end

const AnyCollectionSource = Union{CollectionSource,TaggedCollectionSource}

Base.IndexStyle(::Type{<:AnyCollectionSource}) = IndexLinear()
Base.size(source::AnyCollectionSource) = (source.length,)
Base.length(source::AnyCollectionSource) = source.length

@inline _sourceinput(source::StructSource) = getfield(source, :input)
@inline _sourcefields(source::StructSource) = getfield(source, :fields)
@inline _sourcerow(source::StructSource) = getfield(source, :row)
@inline _row(source::StructSource) =
    _sourceinput(source).rows[_sourcerow(source) == 0 ? 1 : _sourcerow(source)]
@inline _hascolumn(input::InputRows, name::Symbol) = name in input.names
@inline _getcolumn(row, name::Symbol) = Tables.getcolumn(row, name)
@inline _fieldcolumn(field::FieldPlan)::Symbol = something(field.column)

function _decodekind(value)
    value === nothing && return :nothing
    value === missing && return :missing
    value === true && return :value
    value === false && return :nothing
    value isa Symbol && return value
    value isa AbstractString && return Symbol(value)
    _error(
        "invalid nullable field marker $(repr(value)); expected `value`, `nothing`, or `missing`",
    )
end

function _inferredkind(source::StructSource, field::FieldPlan)
    rows = _sourcerow(source) == 0 ? _sourceinput(source).rows : (_row(source),)
    for name in field.data_columns
        _hascolumn(_sourceinput(source), name) || continue
        for row in rows
            _isnull(_getcolumn(row, name)) || return :value
        end
    end
    _allowsnothing(field.nulls) && return :nothing
    _allowsmissing(field.nulls) && return :missing
    return :value
end

function _fieldkind(source::StructSource, field::FieldPlan)
    field.marker === nothing && return :value
    if _hascolumn(_sourceinput(source), field.marker)
        kind = _decodekind(_getcolumn(_row(source), field.marker))
    else
        kind = _inferredkind(source, field)
    end
    kind === :value && return kind
    kind === :nothing && _allowsnothing(field.nulls) && return kind
    kind === :missing && _allowsmissing(field.nulls) && return kind
    _error(
        "marker `$(field.marker)` selects `$kind`, which `$(field.declared_type)` does not allow",
    )
end

function _collectionlength(source::StructSource, field::FieldPlan)
    if field.length_column !== nothing &&
       _hascolumn(_sourceinput(source), field.length_column)
        value = _getcolumn(_row(source), field.length_column)
        value isa Integer || _error("`$(field.length_column)` must contain integer lengths")
        value >= 0 || _error("`$(field.length_column)` cannot contain a negative length")
        expected = Int(value)
        for row in _sourceinput(source).rows
            other = _getcolumn(row, field.length_column)
            other == expected ||
                _error("`$(field.length_column)` must be constant within an object")
        end
    else
        expected = length(_sourceinput(source).rows)
    end
    expected == 0 && return 0
    expected == length(_sourceinput(source).rows) || _error(
        "`$(field.length_column)` says $expected elements, but the object has $(length(_sourceinput(source).rows)) rows",
    )
    return expected
end

function _fieldvalue(source::StructSource, field::FieldPlan)
    if field.kind == LEAF
        column = _fieldcolumn(field)
        _hascolumn(_sourceinput(source), column) || return nothing, false
        value = _getcolumn(_row(source), column)
        return _preservenull(value, field.nulls), true
    end

    available = field.marker !== nothing && _hascolumn(_sourceinput(source), field.marker)
    available |= any(name -> _hascolumn(_sourceinput(source), name), field.data_columns)
    available || return nothing, false

    kind = _fieldkind(source, field)
    kind === :nothing && return ExplicitNothing(), true
    kind === :missing && return ExplicitMissing(), true

    if field.kind == STRUCT
        return StructSource(_sourceinput(source), field.children, _sourcerow(source)), true
    else
        count = _collectionlength(source, field)
        Source =
            isempty(field.tags) && field.element_nulls != (HAS_NOTHING | HAS_MISSING) ?
            CollectionSource : TaggedCollectionSource
        return Source(_sourceinput(source), field, count), true
    end
end

function Tables.columnnames(source::StructSource)
    names = Symbol[]
    for field in _sourcefields(source)
        _, present = _fieldvalue(source, field)
        present && push!(names, field.name)
    end
    return names
end

function Tables.getcolumn(source::StructSource, name::Symbol)
    for field in _sourcefields(source)
        field.name === name || continue
        value, present = _fieldvalue(source, field)
        present && return value
        break
    end
    throw(ArgumentError("column `$name` is not present in the nested struct source"))
end

Tables.getcolumn(source::StructSource, index::Int) =
    Tables.getcolumn(source, Tables.columnnames(source)[index])
Tables.getcolumn(source::StructSource, ::Type, index::Int, name::Symbol) =
    Tables.getcolumn(source, name)

function Base.getindex(source::AnyCollectionSource, index::Int)
    @boundscheck checkbounds(source, index)
    field = source.field
    if isempty(field.children)
        return _getcolumn(source.input.rows[index], _fieldcolumn(field))
    else
        return StructSource(source.input, field.children, index)
    end
end

function _maketaggedcollection(style, ::Type{T}, source::TaggedCollectionSource) where {T}
    E = eltype(T)
    values = Vector{E}(undef, length(source))
    field = source.field
    for index in eachindex(values)
        raw = _getcolumn(source.input.rows[index], _fieldcolumn(field))
        values[index] =
            _makefield(style, E, _preservenull(raw, field.element_nulls), field.tags)
    end
    T === Vector{E} && return values, StructUtils.defaultstate(style)
    return StructUtils.make(style, T, values)
end

function _input(rows)
    isempty(rows) && return InputRows(rows, Set{Symbol}())
    names = Set(Symbol(name) for name in Tables.columnnames(first(rows)))
    return InputRows(rows, names)
end

function _make(T, rows, plan::Plan, style)
    source = StructSource(_input(rows), plan.fields, 0)
    return StructUtils.make(T, source, style)
end

function _groupranges(rows, id_column::Symbol)
    ranges = UnitRange{Int}[]
    isempty(rows) && return ranges
    first_index = 1
    id = _getcolumn(rows[1], id_column)
    seen = Set{Any}((id,))
    for index = 2:length(rows)
        next_id = _getcolumn(rows[index], id_column)
        if !isequal(next_id, id)
            push!(ranges, first_index:(index-1))
            first_index = index
            id = next_id
            id in seen && _error(
                "rows for each root ID must be consecutive; $(repr(id)) appears in more than one group",
            )
            push!(seen, id)
        end
    end
    push!(ranges, first_index:length(rows))
    return ranges
end

struct LeafSpec{Name,Nulls,T}
    tags::T
end

struct NestedSpec{T,Marker,Nulls,C}
    children::C
end

function _columnspec(T, fields, style)
    T <: NamedTuple && return nothing
    StructUtils.noarg(style, T) && return nothing
    length(fields) == fieldcount(T) || return nothing

    specs = Any[]
    for (index, field) in pairs(fields)
        field.index == index || return nothing
        if field.kind == LEAF
            column = _fieldcolumn(field)
            push!(specs, LeafSpec{column,field.nulls,typeof(field.tags)}(field.tags))
        elseif field.kind == STRUCT
            children = _columnspec(field.value_type, field.children, style)
            children === nothing && return nothing
            push!(
                specs,
                NestedSpec{field.value_type,field.marker,field.nulls,typeof(children)}(
                    children,
                ),
            )
        else
            return nothing
        end
    end
    return Tuple(specs)
end

function _columnsource(T, table, plan::Plan, style)
    Tables.columnaccess(typeof(table)) || return nothing
    plan.collection === nothing || return nothing
    specs = _columnspec(T, plan.fields, style)
    specs === nothing && return nothing

    columns = Tables.columns(table)
    available = Set(Symbol(name) for name in Tables.columnnames(columns))
    all(name -> name in available, plan.names) || return nothing
    isempty(plan.names) && return nothing

    count = length(Tables.getcolumn(columns, first(plan.names)))
    return columns, count, specs
end

@inline function _makefield(style, ::Type{T}, source, tags) where {T}
    value, _ = StructUtils.make(style, T, source, tags)
    return value
end

@inline function _columnfield(
    style,
    ::Type{T},
    columns,
    row::Int,
    spec::LeafSpec{Name,Nulls},
) where {T,Name,Nulls}
    source = @inbounds Tables.getcolumn(columns, Name)[row]
    return _makefield(style, T, _preservenull(source, Nulls), spec.tags)
end

@inline function _columnfield(
    style,
    ::Type{T},
    columns,
    row::Int,
    spec::NestedSpec{Value,Marker,Nulls},
) where {T,Value,Marker,Nulls}
    if Marker !== nothing
        kind = _decodekind(@inbounds Tables.getcolumn(columns, Marker)[row])
        if kind === :nothing
            _allowsnothing(Nulls) || _error("`$T` does not allow `nothing`")
            return nothing
        elseif kind === :missing
            _allowsmissing(Nulls) || _error("`$T` does not allow `missing`")
            return missing
        elseif kind !== :value
            _error("invalid nullable field marker $(repr(kind))")
        end
    end
    return _makecolumns(Value, columns, row, style, spec.children)
end

@generated function _makecolumns(::Type{T}, columns, row::Int, style, specs::S) where {T,S}
    values = Any[]
    for index = 1:fieldcount(T)
        field_type = fieldtype(T, index)
        push!(
            values,
            :(_columnfield(style, $field_type, columns, row, getfield(specs, $index))),
        )
    end
    return :(T($(values...)))
end

function _makecolumnsvector(::Type{T}, columns, count, style, specs) where {T}
    values = Vector{T}(undef, count)
    @inbounds for row = 1:count
        values[row] = _makecolumns(T, columns, row, style, specs)
    end
    return values
end

"""
    Strapping.construct(T, table; style=Strapping.Style(), silencewarnings=false)
    Strapping.construct(Vector{T}, table; style=Strapping.Style())

Construct one or many values of `T` from a Tables.jl-compatible `table`.
Columns match fields by name, so column order does not matter. Nested concrete
structs use prefixed columns. A concrete collection field consumes repeated
rows. Use the Strapping `id=true` field tag to group repeated rows when
constructing `Vector{T}`.
"""
function construct(
    ::Type{T},
    table;
    style::StructUtils.StructStyle = DEFAULT_STYLE,
    silencewarnings::Bool = false,
) where {T}
    plan = _plan(T, style)
    source = _columnsource(T, table, plan, style)
    if source !== nothing
        columns, count, specs = source
        count == 0 && _error("cannot construct `$T` from an empty table")
        value = _makecolumns(T, columns, 1, style, specs)
        if count > 1 && !silencewarnings
            @warn "additional table rows remain after constructing `$T`" remaining =
                (count - 1)
        end
        return value
    end

    rows = collect(Tables.rows(table))
    isempty(rows) && _error("cannot construct `$T` from an empty table")

    used = if plan.collection === nothing
        1
    elseif plan.id_column === nothing
        length(rows)
    else
        first_id = _getcolumn(rows[1], plan.id_column)
        boundary = length(rows) + 1
        for index = 2:length(rows)
            if !isequal(_getcolumn(rows[index], plan.id_column), first_id)
                boundary = index
                break
            end
        end
        boundary - 1
    end
    value = _make(T, view(rows, 1:used), plan, style)
    if used < length(rows) && !silencewarnings
        @warn "additional table rows remain after constructing `$T`" remaining =
            (length(rows) - used)
    end
    return value
end

function construct(
    ::Type{Vector{T}},
    table;
    style::StructUtils.StructStyle = DEFAULT_STYLE,
) where {T}
    plan = _plan(T, style)
    source = _columnsource(T, table, plan, style)
    if source !== nothing
        columns, count, specs = source
        return _makecolumnsvector(T, columns, count, style, specs)
    end

    rows = collect(Tables.rows(table))
    isempty(rows) && return T[]

    if plan.collection === nothing
        return [_make(T, view(rows, index:index), plan, style) for index in eachindex(rows)]
    end
    plan.id_column === nothing && _error(
        "constructing `Vector{$T}` with a flattened collection requires one root field tagged `id=true`",
    )
    ranges = _groupranges(rows, plan.id_column)
    return [_make(T, view(rows, range), plan, style) for range in ranges]
end
