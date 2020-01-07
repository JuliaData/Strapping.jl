using Test, StructTypes, SQLite, DBInterface, Strapping

db = SQLite.DB()

# setup two tables
DBInterface.execute!(db, "CREATE TABLE T (a INT, b REAL)")
DBInterface.execute!(db, "INSERT INTO T VALUES(10, 3.14)")

DBInterface.execute!(db, "CREATE TABLE S (id INT, floats REAL)")
DBInterface.execute!(db, "INSERT INTO S VALUES(10, 3.14)")
DBInterface.execute!(db, "INSERT INTO S VALUES(10, 3.15)")
DBInterface.execute!(db, "INSERT INTO S VALUES(10, 3.16)")

struct AB
    a::Int
    b::Float64
end
StructTypes.StructType(::Type{AB}) = StructTypes.Struct()

@test Strapping.select(db, "select * from T", AB) == AB(10, 3.14)
@test Strapping.select(db, "select * from T", Vector{AB}) == [AB(10, 3.14)]

mutable struct ABM
    a::Int
    b::Float64
    ABM() = new()
    ABM(a::Int, b::Float64) = new(a, b)
end
Base.:(==)(a::ABM, b::ABM) = a.a == b.a && a.b == b.b
StructTypes.StructType(::Type{ABM}) = StructTypes.Mutable()

@test Strapping.select(db, "select * from T", ABM) == ABM(10, 3.14)
@test Strapping.select(db, "select * from T", Vector{ABM}) == [ABM(10, 3.14)]

@test Strapping.select(db, "select * from T", NamedTuple) == (a=10, b=3.14)
@test Strapping.select(db, "select * from T", NamedTuple{(:a, :b), Tuple{Int, Float64}}) == (a=10, b=3.14)
@test Strapping.select(db, "select * from T", NamedTuple{(:a, :b)}) == (a=10, b=3.14)
@test Strapping.select(db, "select * from T", Dict) == Dict("a" => 10, "b" => 3.14)
@test Strapping.select(db, "select * from T", Dict{String, Float64}) == Dict("a" => 10.0, "b" => 3.14)
@test Strapping.select(db, "select * from T", Dict{Symbol, Any}) == Dict(:a => 10, :b => 3.14)
@test Strapping.select(db, "select * from T", Array) == [10, 3.14]
@test Strapping.select(db, "select * from T", Vector{Float64}) == [10.0] # because Float64 is scalar, only 1st field of result is used, other fields are ignored
@test Strapping.select(db, "select * from T", Set) == Set([10, 3.14])
@test Strapping.select(db, "select * from T", Tuple) == (10, 3.14)
@test Strapping.select(db, "select * from T", Tuple{Int, Float64}) == (10, 3.14)
@test Strapping.select(db, "select * from T", Vector{Any}) == [Dict("a" => 10, "b" => 3.14)]

struct AB2
    id::Int
    floats::Vector{Float64}
end

Base.:(==)(a::AB2, b::AB2) = a.id == b.id && a.floats == b.floats
StructTypes.StructType(::Type{AB2}) = StructTypes.Struct()
StructTypes.idproperty(::Type{AB2}) = :id

@test Strapping.select(db, "select * from S", AB2) == AB2(10, [3.14, 3.15, 3.16])
@test Strapping.select(db, "select * from S", Vector{AB2}) == [AB2(10, [3.14, 3.15, 3.16])]

struct AB3
    a::Int
    ab::AB
end

StructTypes.StructType(::Type{AB3}) = StructTypes.Struct()
StructTypes.fieldprefix(::Type{AB}) = :ab_

@test Strapping.select(db, "select a, a as ab_a, b as ab_b from T", AB3) == AB3(10, AB(10, 3.14))
@test Strapping.select(db, "select a, a as ab_a, b as ab_b from T", Vector{AB3}) == [AB3(10, AB(10, 3.14))]

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

@test Strapping.select(db, "select a.*, b.id as ab_a, b.floats as ab_b from T as a, S as b", AB4) == AB4(10, 3.14, AB[AB(10, 3.14), AB(10, 3.15), AB(10, 3.16)])
@test Strapping.select(db, "select a.*, b.id as ab_a, b.floats as ab_b from T as a, S as b", Vector{AB4}) == [AB4(10, 3.14, AB[AB(10, 3.14), AB(10, 3.15), AB(10, 3.16)])]