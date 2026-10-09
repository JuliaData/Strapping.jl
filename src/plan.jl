const LEAF = UInt8(1)
const STRUCT = UInt8(2)
const COLLECTION = UInt8(3)

const VALUE_COLUMN = UInt8(1)
const KIND_COLUMN = UInt8(2)
const LENGTH_COLUMN = UInt8(3)

const HAS_NOTHING = UInt8(1)
const HAS_MISSING = UInt8(2)

struct ColumnPlan
    name::Symbol
    type::Any
    path::Vector{Int}
    collection_at::Int
    operation::UInt8
    tags::NamedTuple
end

struct FieldPlan
    index::Int
    name::Symbol
    declared_type::Any
    value_type::Any
    kind::UInt8
    tags::NamedTuple
    path::Vector{Int}
    nulls::UInt8
    element_nulls::UInt8
    marker::Union{Nothing,Symbol}
    length_column::Union{Nothing,Symbol}
    column::Union{Nothing,Symbol}
    children::Vector{FieldPlan}
    data_columns::Vector{Symbol}
end

struct Plan
    target::Any
    fields::Vector{FieldPlan}
    columns::Vector{ColumnPlan}
    names::Tuple
    types::Tuple
    lookup::Dict{Symbol,Int}
    id_column::Union{Nothing,Symbol}
    collection::Union{Nothing,FieldPlan}
end

mutable struct PlanBuilder{S}
    style::S
    columns::Vector{ColumnPlan}
    collections::Vector{FieldPlan}
    id_column::Union{Nothing,Symbol}
end

PlanBuilder(style) = PlanBuilder(style, ColumnPlan[], FieldPlan[], nothing)

function _nullabletype(T)
    types = T isa Union ? Base.uniontypes(T) : Any[T]
    nulls = UInt8(0)
    values = Any[]
    for type in types
        if type === Nothing
            nulls |= HAS_NOTHING
        elseif type === Missing
            nulls |= HAS_MISSING
        else
            push!(values, type)
        end
    end
    return length(values) == 1 ? only(values) : T, nulls, length(values) == 1
end

@inline _allowsnothing(nulls::UInt8) = nulls & HAS_NOTHING != 0
@inline _allowsmissing(nulls::UInt8) = nulls & HAS_MISSING != 0

function _iscollection(style, T)
    T <: NamedTuple && return false
    return isconcretetype(T) && StructUtils.arraylike(style, T)
end

function _isstruct(style, T)
    isconcretetype(T) || return false
    StructUtils.dictlike(style, T) && return false
    StructUtils.arraylike(style, T) && return false
    return StructUtils.structlike(style, T) || StructUtils.noarg(style, T)
end

function _customlower(style, T)
    T isa DataType || return false
    try
        style_method = which(StructUtils.lower, Tuple{typeof(style),T})
        value_method = which(StructUtils.lower, Tuple{T})
        return style_method.module !== StructUtils || value_method.module !== StructUtils
    catch
        return true
    end
end

function _schematype(style, T, tags, maybemissing::Bool)
    lowered = haskey(tags, :lower) || haskey(tags, :dateformat) || _customlower(style, T)
    S = lowered ? Any : T
    return maybemissing && S !== Any ? Union{Missing,S} : S
end

function _addcolumn!(builder::PlanBuilder, name, T, path, collection_at, operation, tags)
    push!(builder.columns, ColumnPlan(name, T, copy(path), collection_at, operation, tags))
    return name
end

function _fieldplans!(
    builder::PlanBuilder,
    T,
    prefix,
    parent_path,
    inherited_collection,
    nullable_parent,
)
    fields = FieldPlan[]
    for index = 1:fieldcount(T)
        name = fieldname(T, index)
        tags = StructUtils.fieldtags(builder.style, T, name)
        get(tags, :ignore, false) && continue
        push!(
            fields,
            _fieldplan!(
                builder,
                T,
                index,
                name,
                fieldtype(T, index),
                tags,
                prefix,
                parent_path,
                inherited_collection,
                nullable_parent,
            ),
        )
    end
    return fields
end

function _fieldplan!(
    builder::PlanBuilder,
    parent,
    index,
    name,
    declared,
    tags,
    prefix,
    parent_path,
    inherited_collection,
    nullable_parent,
)
    path = [parent_path; index]
    external = _outputname(get(tags, :name, name), name)
    value_type, nulls, simple_union = _nullabletype(declared)
    flatten = get(tags, :flatten, true)
    kind = if flatten && simple_union && _iscollection(builder.style, value_type)
        COLLECTION
    elseif flatten && simple_union && _isstruct(builder.style, value_type)
        STRUCT
    else
        LEAF
    end

    marker = nothing
    element_nulls = UInt8(0)
    length_column = nothing
    column = nothing
    children = FieldPlan[]
    data_columns = Symbol[]

    if kind == LEAF
        column = Symbol(prefix, external)
        T = _schematype(
            builder.style,
            declared,
            tags,
            nullable_parent || inherited_collection > 0,
        )
        _addcolumn!(builder, column, T, path, inherited_collection, VALUE_COLUMN, tags)
        push!(data_columns, column)
    else
        child_prefix =
            haskey(tags, :prefix) ? Symbol(prefix, _symbol(tags.prefix, :prefix)) :
            Symbol(prefix, external, :_)

        if nulls != 0
            marker = Symbol(child_prefix, :_kind)
            T = inherited_collection > 0 ? Union{Missing,Symbol} : Symbol
            _addcolumn!(builder, marker, T, path, inherited_collection, KIND_COLUMN, (;))
        end

        if kind == STRUCT
            children = _fieldplans!(
                builder,
                value_type,
                child_prefix,
                path,
                inherited_collection,
                nullable_parent || nulls != 0,
            )
            for child in children
                append!(data_columns, child.data_columns)
            end
        else
            length_column = Symbol(child_prefix, :_length)
            T = inherited_collection > 0 ? Union{Missing,Int} : Int
            _addcolumn!(
                builder,
                length_column,
                T,
                path,
                inherited_collection,
                LENGTH_COLUMN,
                (;),
            )
            push!(data_columns, length_column)

            element_type = eltype(value_type)
            _, element_nulls, _ = _nullabletype(element_type)
            current_collection = length(path)
            if _isstruct(builder.style, element_type)
                children = _fieldplans!(
                    builder,
                    element_type,
                    child_prefix,
                    path,
                    current_collection,
                    true,
                )
                for child in children
                    append!(data_columns, child.data_columns)
                end
            else
                column = Symbol(prefix, external)
                T = _schematype(builder.style, element_type, tags, true)
                _addcolumn!(
                    builder,
                    column,
                    T,
                    path,
                    current_collection,
                    VALUE_COLUMN,
                    tags,
                )
                push!(data_columns, column)
            end
        end
    end

    field = FieldPlan(
        index,
        name,
        declared,
        value_type,
        kind,
        tags,
        path,
        nulls,
        element_nulls,
        marker,
        length_column,
        column,
        children,
        data_columns,
    )

    if get(tags, :id, false) && length(path) == 1
        kind == LEAF || _error("the `id=true` field tag requires a scalar field")
        builder.id_column === nothing || _error("only one field can have the `id=true` tag")
        builder.id_column = column
    end
    kind == COLLECTION && push!(builder.collections, field)
    return field
end

function _plan(T, style::StructUtils.StructStyle)
    _isstruct(style, T) || _error(
        "`$T` is not a concrete struct-like type; Strapping maps concrete structs and NamedTuples",
    )
    builder = PlanBuilder(style)
    fields = _fieldplans!(builder, T, Symbol(), Int[], 0, false)
    length(builder.collections) <= 1 || _error(
        "`$T` contains multiple flattened collections; set `flatten=false` on all but one collection",
    )

    names = Tuple(column.name for column in builder.columns)
    length(names) == length(unique(names)) ||
        _error("`$T` maps more than one field to the same table column")
    types = Tuple(column.type for column in builder.columns)
    lookup = Dict(name => index for (index, name) in pairs(names))
    collection = isempty(builder.collections) ? nothing : only(builder.collections)
    return Plan(
        T,
        fields,
        builder.columns,
        names,
        types,
        lookup,
        builder.id_column,
        collection,
    )
end
