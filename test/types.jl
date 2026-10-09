module StrappingTestTypes

using StructUtils

export Address,
    BothNull,
    BothNullCollection,
    Club,
    ClubMember,
    Code,
    Coded,
    Customer,
    DuplicateColumns,
    Experiment,
    Interval,
    MutablePoint,
    NoIdCollection,
    NullableLeaf,
    NullableNested,
    OptionalNested,
    OptionalResults,
    OptionalScalar,
    Person,
    Point,
    Renamed,
    Result,
    Route,
    Stop,
    TaggedCollection,
    Trip,
    TripEnd,
    TwoCollections,
    UnionCell,
    WithIgnored

struct Point
    x::Int
    y::Float64
end

StructUtils.@noarg mutable struct MutablePoint
    x::Int
    y::Float64
end

StructUtils.@tags struct Person
    id::Int & (strapping = (id = true,),)
    name::String
end

struct Address
    city::String
    zip::Int
end

struct Customer
    id::Int
    name::String
    address::Address
end

StructUtils.@tags struct Result
    id::Int & (strapping = (id = true,),)
    values::Vector{Float64}
end

StructUtils.@tags struct Experiment
    id::Int & (strapping = (id = true,),)
    name::String
    result::Result
end

StructUtils.@nonstruct struct Code
    value::String
end

StructUtils.lower(x::Code) = x.value
StructUtils.lift(::Type{Code}, x) = Code(String(x))

struct Coded
    id::Int
    code::Code
end

StructUtils.@tags struct Renamed
    id::Int & (strapping = (id = true, name = :record_id),)
    point::Point & (strapping = (prefix = :coordinates_,),)
end

StructUtils.@defaults struct WithIgnored
    id::Int
    visible::String
    cache::String = "default" & (strapping = (ignore = true,),)
end

struct OptionalScalar
    name::String
    value::Union{Nothing,Int}
end

struct BothNull
    value::Union{Missing,Nothing,Int}
end

StructUtils.@tags struct BothNullCollection
    id::Int & (strapping = (id = true,),)
    values::Vector{Union{Missing,Nothing,Int}}
end

_parseint(value) = parse(Int, value)

StructUtils.@tags struct TaggedCollection
    id::Int & (strapping = (id = true,),)
    values::Vector{Int} & (strapping = (lower = string, lift = _parseint),)
end

struct NullableLeaf
    a::Union{Nothing,Int}
    b::Union{Missing,String}
end

struct OptionalNested
    id::Int
    value::Union{Nothing,NullableLeaf}
end

struct NullableNested
    id::Int
    value::Union{Missing,Nothing,NullableLeaf}
end

StructUtils.@tags struct OptionalResults
    id::Int & (strapping = (id = true,),)
    values::Union{Nothing,Vector{Point}}
end

struct UnionCell
    body::Union{String,Vector{String}}
end

struct Interval
    value::Int
end

struct Stop
    code::Int
    hours::Union{Nothing,Interval}
end

StructUtils.@tags struct Route
    id::Int & (strapping = (id = true,),)
    stops::Vector{Stop}
end

struct Trip
    start::Stop
    finish::Stop
end

struct TripEnd
    finish::Stop
end

struct ClubMember
    id::Int
    first_name::String
    last_name::String
end

StructUtils.@tags struct Club
    id::Int & (strapping = (id = true,),)
    name::String
    members::Vector{ClubMember}
end

struct TwoCollections
    left::Vector{Int}
    right::Vector{Int}
end

struct NoIdCollection
    id::Int
    values::Vector{Int}
end

StructUtils.@tags struct DuplicateColumns
    first::Int & (strapping = (name = :same,),)
    second::Int & (strapping = (name = :same,),)
end

function _equal_fields(a::T, b::T) where {T}
    return all(isequal(getfield(a, index), getfield(b, index)) for index = 1:fieldcount(T))
end

for T in (
    Point,
    Person,
    Address,
    Customer,
    Result,
    Experiment,
    Code,
    Coded,
    Renamed,
    WithIgnored,
    OptionalScalar,
    BothNull,
    BothNullCollection,
    TaggedCollection,
    NullableLeaf,
    OptionalNested,
    NullableNested,
    OptionalResults,
    UnionCell,
    Interval,
    Stop,
    Route,
    Trip,
    TripEnd,
    ClubMember,
    Club,
)
    @eval Base.:(==)(a::$T, b::$T) = _equal_fields(a, b)
end

end


using .StrappingTestTypes
