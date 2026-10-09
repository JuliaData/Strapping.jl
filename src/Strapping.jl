"""
Map Julia structs to and from Tables.jl-compatible two-dimensional data.

Strapping uses StructUtils.jl for struct construction, field metadata, and
domain-value conversion. Its public interface is namespaced and intentionally
small: [`Strapping.construct`](@ref), [`Strapping.deconstruct`](@ref), and
[`Strapping.Style`](@ref).
"""
module Strapping

using StructUtils
using Tables

include("style.jl")
include("plan.jl")
include("construct.jl")
include("deconstruct.jl")

end
