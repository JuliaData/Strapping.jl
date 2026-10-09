"""
    Strapping.Error(message)

An error caused by an invalid struct-to-table mapping or table input.
"""
struct Error <: Exception
    msg::String
end

Base.showerror(io::IO, err::Error) = print(io, "Strapping.Error: ", err.msg)

"""
    Strapping.Style()

The `StructUtils.StructStyle` used by Strapping. Field tags in the
`strapping` namespace configure the tabular mapping.
"""
struct Style <: StructUtils.StructStyle end

StructUtils.fieldtagkey(::Style) = :strapping

const DEFAULT_STYLE = Style()

@noinline _error(message) = throw(Error(message))

@inline _isnull(x) = x === nothing || x === missing

function _symbol(x, option::Symbol)
    x isa Symbol && return x
    x isa AbstractString && return Symbol(x)
    _error("the `$option` field tag must be a Symbol or string; got $(repr(x))")
end

function _outputname(value, field::Symbol)
    value === nothing && return field
    value isa Tuple && return isempty(value) ? field : _symbol(first(value), :name)
    return _symbol(value, :name)
end
