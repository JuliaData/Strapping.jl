using Test, StructTypes, Tables, Strapping

tbl = (a=[10], b=[3.14])
tbl2 = (id=[10, 10, 10], floats=[3.14, 3.15, 3.16])

struct AB
    a::Int
    b::Float64
end
StructTypes.StructType(::Type{AB}) = StructTypes.Struct()

ab = AB(10, 3.14)
@test Strapping.construct(AB, tbl) == ab
@test Strapping.construct(Vector{AB}, tbl) == [ab]

mutable struct ABM
    a::Int
    b::Float64
    ABM() = new()
    ABM(a::Int, b::Float64) = new(a, b)
end
Base.:(==)(a::ABM, b::ABM) = a.a == b.a && a.b == b.b
StructTypes.StructType(::Type{ABM}) = StructTypes.Mutable()

abm = ABM(10, 3.14)
@test Strapping.construct(ABM, tbl) == abm
@test Strapping.construct(Vector{ABM}, tbl) == [abm]

@test Strapping.construct(NamedTuple, tbl) == (a=10, b=3.14)
@test Strapping.construct(NamedTuple{(:a, :b), Tuple{Int, Float64}}, tbl) == (a=10, b=3.14)
@test Strapping.construct(NamedTuple{(:a, :b)}, tbl) == (a=10, b=3.14)
@test Strapping.construct(Dict, tbl) == Dict("a" => 10, "b" => 3.14)
@test Strapping.construct(Dict{String, Float64}, tbl) == Dict("a" => 10.0, "b" => 3.14)
@test Strapping.construct(Dict{Symbol, Any}, tbl) == Dict(:a => 10, :b => 3.14)
@test Strapping.construct(Array, tbl) == [10, 3.14]
@test Strapping.construct(Vector{Float64}, tbl) == [10.0] # because Float64 is scalar, only 1st field of result is used, other fields are ignored
@test Strapping.construct(Set, tbl) == Set([10, 3.14])
@test Strapping.construct(Tuple, tbl) == (10, 3.14)
@test Strapping.construct(Tuple{Int, Float64}, tbl) == (10, 3.14)
@test Strapping.construct(Vector{Any}, tbl) == [Dict("a" => 10, "b" => 3.14)]

struct AB2
    id::Int
    floats::Vector{Float64}
end

Base.:(==)(a::AB2, b::AB2) = a.id == b.id && a.floats == b.floats
StructTypes.StructType(::Type{AB2}) = StructTypes.Struct()
StructTypes.idproperty(::Type{AB2}) = :id

ab2 = AB2(10, [3.14, 3.15, 3.16])
@test Strapping.construct(AB2, tbl2) == ab2
@test Strapping.construct(Vector{AB2}, tbl2) == [ab2]

struct AB3
    a::Int
    ab::AB
end

StructTypes.StructType(::Type{AB3}) = StructTypes.Struct()

tbl3 = (a=[1], ab_a=[10], ab_b=[3.14])

ab3 = AB3(1, AB(10, 3.14))
@test Strapping.construct(AB3, tbl3) == ab3
@test Strapping.construct(Vector{AB3}, tbl3) == [ab3]

mutable struct AB4
    a::Int
    b::Float64
    abs::Vector{AB}
    AB4() = new()
    AB4(a::Int, b::Float64, abs::Vector{AB}) = new(a, b, abs)
end

Base.:(==)(a::AB4, b::AB4) = a.a == b.a && a.b == b.b && a.abs == b.abs
StructTypes.StructType(::Type{AB4}) = StructTypes.Mutable()
StructTypes.idproperty(::Type{AB4}) = :a

tbl4 = (a=[10, 10, 10], b=[3.14, 3.14, 3.14], abs_a=[10, 10, 10], abs_b=[3.14, 3.15, 3.16])

ab4 = AB4(10, 3.14, AB[AB(10, 3.14), AB(10, 3.15), AB(10, 3.16)])
@test Strapping.construct(AB4, tbl4) == ab4
@test Strapping.construct(Vector{AB4}, tbl4) == [ab4]

# multiple levels of nesting
struct AB5
    a::Int
    ab::AB3
end

StructTypes.StructType(::Type{AB5}) = StructTypes.Struct()

tbl5 = (a=[10], ab_a=[10], ab_ab_a=[10], ab_ab_b=[3.14])

ab5 = AB5(10, AB3(10, AB(10, 3.14)))
@test Strapping.construct(AB5, tbl5) == ab5
@test Strapping.construct(Vector{AB5}, tbl5) == [ab5]

# deconstruction
@test columntable(Strapping.deconstruct(ab)) == tbl
@test columntable(Strapping.deconstruct(ab2)) == tbl2
@test columntable(Strapping.deconstruct(ab3)) == tbl3
@test columntable(Strapping.deconstruct(ab4)) == tbl4
@test columntable(Strapping.deconstruct(ab5)) == tbl5

struct AB6
    a::Int
    b::Vector{Float64}
    c::AB
    d::Vector{AB}
end

Base.:(==)(a::AB6, b::AB6) = a.a == b.a && a.b == b.b && a.c == b.c && a.d == b.d
StructTypes.StructType(::Type{AB6}) = StructTypes.Struct()
StructTypes.idproperty(::Type{AB6}) = :a

tbl6 = (a=[1, 1, 1], b=[3.14, 3.15, 3.16], c_a=[2, 2, 2], c_b=[0.01, 0.01, 0.01], d_a=[10, 11, 12], d_b=[1.1, 2.2, 3.3])

ab6 = AB6(1, [3.14, 3.15, 3.16], AB(2, 0.01), [AB(10, 1.1), AB(11, 2.2), AB(12, 3.3)])
@test Strapping.construct(AB6, tbl6) == ab6
@test Strapping.construct(Vector{AB6}, tbl6) == [ab6]
@test columntable(Strapping.deconstruct(ab6)) == tbl6

# https://github.com/JuliaData/Strapping.jl/issues/3
struct TestStruct
    a::Float64
    b::Float64
    id::Int
end

StructTypes.StructType(::Type{TestStruct}) = StructTypes.Struct()
StructTypes.idproperty(::Type{TestStruct}) = :id

data = [ TestStruct(rand(2)..., n) for n = 1:5]

tbl = Strapping.deconstruct(data)
@test length(Tables.columntable(tbl)[1]) == 5

struct AA1
    a::Int64
    b::Dict{Symbol, Any}
end

StructTypes.StructType(::Type{AA1}) = StructTypes.Struct()

data = [ AA1(1, Dict{Symbol, Any}(:aa => 2, :bb => 3)) ]
tbl = Strapping.deconstruct(data)
tbl2 = Tables.columntable(tbl)
@test tbl2.a == [1]
@test tbl2.b_aa == [2]
@test tbl2.b_bb == [3]

#8
struct TestResult
    id::Int
    values::Vector{Float64}
end
StructTypes.StructType(::Type{TestResult}) = StructTypes.Struct()
StructTypes.idproperty(::Type{TestResult}) = :id

tbl = (id=[1, 1, 1, 2, 2, 2], values=[3.14, 3.15, 3.16, 40.1, 0.01, 2.34])
testresult = Strapping.construct(Vector{TestResult}, tbl)

struct Experiment
    id::Int
    name::String
    testresults::TestResult
end
StructTypes.StructType(::Type{Experiment}) = StructTypes.Struct()
StructTypes.idproperty(::Type{Experiment}) = :id

StructTypes.fieldprefix(::Type{Experiment}, nm::Symbol) = nm == :testresults ? :testresults_ : :_

tbl2 = (id=[1, 1, 1], name=["exp1", "exp1", "exp1"], testresults_id=[1, 1, 1], testresults_values=[3.14, 3.15, 3.16])
experiment = Strapping.construct(Experiment, tbl2)
@test experiment.id == 1
@test experiment.name == "exp1"
@test experiment.testresults.values == [3.14, 3.15, 3.16]