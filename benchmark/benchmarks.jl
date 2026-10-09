using BenchmarkTools
using Strapping
using StructUtils
using Tables

struct Point
    x::Int
    y::Float64
end

struct Sample
    id::Int
    point::Point
end

StructUtils.@tags struct Series
    id::Int & (strapping = (id = true,),)
    points::Vector{Point}
end

const POINT_TABLE = (x = collect(1:100_000), y = fill(1.5, 100_000))
const SAMPLE_TABLE =
    (id = collect(1:100_000), point_x = collect(1:100_000), point_y = fill(1.5, 100_000))
const POINTS = [Point(index, 1.5) for index = 1:100_000]
const SERIES = Series(1, POINTS)

const SUITE = BenchmarkGroup()
SUITE["construct", "flat"] = @benchmarkable Strapping.construct(Vector{Point}, POINT_TABLE)
SUITE["construct", "nested"] =
    @benchmarkable Strapping.construct(Vector{Sample}, SAMPLE_TABLE)
SUITE["deconstruct", "flat"] =
    @benchmarkable Tables.columntable(Strapping.deconstruct(POINTS))
SUITE["round-trip", "collection"] = @benchmarkable begin
    rows = Strapping.deconstruct(SERIES)
    Strapping.construct(Series, rows)
end

if abspath(PROGRAM_FILE) == @__FILE__
    tune!(SUITE)
    results = run(SUITE; verbose = true)
    display(median(results))
end
