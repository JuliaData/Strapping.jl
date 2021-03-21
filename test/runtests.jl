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
    b::AB
    c::Vector{AB}
end

Base.:(==)(a::AB6, b::AB6) = a.a == b.a && a.b == b.b && a.c == b.c
StructTypes.StructType(::Type{AB6}) = StructTypes.Struct()
StructTypes.idproperty(::Type{AB6}) = :a

tbl6 = (a=[1, 1, 1], b_a=[2, 2, 2], b_b=[0.01, 0.01, 0.01], c_a=[10, 11, 12], c_b=[1.1, 2.2, 3.3])

ab6 = AB6(1, AB(2, 0.01), [AB(10, 1.1), AB(11, 2.2), AB(12, 3.3)])
@test Strapping.construct(AB6, tbl6) == ab6
@test Strapping.construct(Vector{AB6}, tbl6) == [ab6]
@test columntable(Strapping.deconstruct(ab6)) == tbl6

# https://github.com/JuliaData/Strapping.jl/issues/12
struct AB7
    id::Int
    values::Vector{Float64}
end

Base.:(==)(a::AB7, b::AB7) = a.id == b.id && a.values == b.values
StructTypes.StructType(::Type{AB7}) = StructTypes.Struct()
StructTypes.idproperty(::Type{AB7}) = :id

ab7 = AB7(1, Float64[])
tbl = columntable(Strapping.deconstruct(ab7))
@test tbl.id[1] == 1
@test tbl.values[1] === missing

struct AB9
    a::Int
    b::String
    c::Float64
    d::String
    e::Int
    f::String
end
StructTypes.StructType(::Type{AB9}) = StructTypes.Struct()

struct AB10
    id::Int
    ab9::AB9
end
StructTypes.StructType(::Type{AB10}) = StructTypes.Struct()

struct AB11
    id::Int
    ab10::AB10
end
StructTypes.StructType(::Type{AB11}) = StructTypes.Struct()

ab11 = AB11(1, AB10(2, AB9(3, "4", 5.0, "6", 7, "8")))
tbl = columntable(Strapping.deconstruct(ab11))
@test length(tbl) == 8
@test tbl.id[1] == 1
@test tbl[end][1] == "8"

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

struct Service
    id::String
    data::NamedTuple
end
StructTypes.StructType(::Type{Service}) = StructTypes.Struct()
StructTypes.idproperty(::Type{Service}) = :id
StructTypes.fieldprefix(::Type{Service}, nm::Symbol) = Symbol()

Tables.isrowtable(::Type{Service}) = true

function Tables.getcolumn(row::Service,i::Int)
    if i == 1
        return getfield(row,1)
    else
        return row.data[i-1]
    end
end

function Tables.getcolumn(row::Service,nm::Symbol)
    if nm == :id
        return row.id
    else
        return getproperty(row.data,nm)
    end
end

function Tables.columnnames(row::Service)
    vcat(:id,keys(row.data)...)
end

x = Service("qux",(a=1,b=3))
xd = Strapping.deconstruct(x)
xc = Strapping.construct(Service,xd)

@test xc.id == "qux"
@test xc.data == (a=1, b=3)

x2 = Service("foo",(a=1,b=3))
X = [x,x2]
Xd = Strapping.deconstruct(X)
Xc = Strapping.construct(Vector{Service},Xd)

@test length(Xc) == 2
@test Xc[2].id == "foo"
@test Xc[2].data == (a=1, b=3)

struct Wrapper
    x::NamedTuple{(:a, :b), Tuple{Int, String}}
end

StructTypes.StructType(::Type{Wrapper}) = StructTypes.CustomStruct()
StructTypes.lower(x::Wrapper) = x.x
StructTypes.lowertype(::Type{Wrapper}) = fieldtype(Wrapper, :x)
w = Wrapper((a=1, b="hey"))

tbl = Strapping.deconstruct(w) |> Tables.columntable
@test tbl == (a = [1], b = ["hey"])
w2 = Strapping.construct(Wrapper, tbl)
@test w == w2
