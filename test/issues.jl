using Arrow
using DataFrames

@testset "issue regressions" begin
    @testset "#16 and #20: nullable fields" begin
        values = [
            OptionalNested(1, NullableLeaf(2, "set")),
            OptionalNested(2, nothing),
            OptionalNested(3, NullableLeaf(nothing, missing)),
        ]
        table = Tables.columntable(Strapping.deconstruct(values))
        @test table.value__kind == [:value, :nothing, :value]
        @test isequal(table.value_a, Union{Missing,Nothing,Int}[2, missing, nothing])
        @test isequal(table.value_b, Union{Missing,String}["set", missing, missing])
        @test Strapping.construct(Vector{OptionalNested}, table) == values

        nullable = [
            NullableNested(1, missing),
            NullableNested(2, nothing),
            NullableNested(3, NullableLeaf(nothing, missing)),
        ]
        nullable_table = Strapping.deconstruct(nullable)
        @test Tables.columntable(nullable_table).value__kind == [:missing, :nothing, :value]
        @test Strapping.construct(Vector{NullableNested}, nullable_table) == nullable

        scalar = [OptionalScalar("none", nothing), OptionalScalar("some", 4)]
        @test Strapping.construct(Vector{OptionalScalar}, Strapping.deconstruct(scalar)) ==
              scalar

        both_nulls = [BothNull(nothing), BothNull(missing), BothNull(4)]
        @test isequal(
            Strapping.construct(Vector{BothNull}, Strapping.deconstruct(both_nulls)),
            both_nulls,
        )

        both_null_elements = BothNullCollection(1, [nothing, missing, 4])
        @test isequal(
            Strapping.construct(
                BothNullCollection,
                Strapping.deconstruct(both_null_elements),
            ),
            both_null_elements,
        )

        cells = [UnionCell("text"), UnionCell(["a", "b"])]
        cell_table = Tables.columntable(Strapping.deconstruct(cells))
        @test cell_table.body == Union{String,Vector{String}}["text", ["a", "b"]]
        @test Strapping.construct(Vector{UnionCell}, cell_table) == cells
    end

    @testset "#18: optional structs have stable unique columns" begin
        trips = [
            Trip(Stop(1, nothing), Stop(2, nothing)),
            Trip(Stop(2, Interval(1)), Stop(3, Interval(2))),
        ]
        rows = Strapping.deconstruct(trips)
        names = Tables.schema(rows).names
        @test length(names) == length(unique(names))
        @test Strapping.construct(Vector{Trip}, rows) == trips

        buffer = IOBuffer()
        Arrow.write(buffer, rows)
        seekstart(buffer)
        arrow_rows = Arrow.Table(buffer)
        @test Strapping.construct(Vector{Trip}, arrow_rows) == trips

        ends = TripEnd.(getfield.(trips, :finish))
        end_rows = Strapping.deconstruct(ends)
        buffer = IOBuffer()
        Arrow.write(buffer, end_rows)
        seekstart(buffer)
        @test Strapping.construct(Vector{TripEnd}, Arrow.Table(buffer)) == ends

        markerless = (
            id = [1, 1],
            stops__length = [2, 2],
            stops_code = [10, 11],
            stops_hours_value = Union{Missing,Int}[2, missing],
        )
        @test Strapping.construct(Route, markerless) ==
              Route(1, [Stop(10, Interval(2)), Stop(11, nothing)])
    end

    @testset "#19: unordered source columns" begin
        @test Strapping.construct(Person, [(name = "Bob", id = 1)]) == Person(1, "Bob")
        @test Strapping.construct(Person, [(id = 1, name = "Bob")]) == Person(1, "Bob")
    end

    @testset "#22 and #24: related-object example" begin
        club = Club(
            1,
            "chess club",
            [ClubMember(1, "John", "Smith"), ClubMember(2, "Mary", "Miller")],
        )
        rows = Strapping.deconstruct(club)
        frame = DataFrame(rows)
        @test nrow(frame) == 2
        @test frame.members_first_name == ["John", "Mary"]
        @test Strapping.construct(Club, frame) == club
        @test Strapping.construct(Vector{Club}, frame) == [club]
    end

    @testset "nullable collection states" begin
        values = [
            OptionalResults(1, nothing),
            OptionalResults(2, Point[]),
            OptionalResults(3, [Point(1, 2.5), Point(2, 3.5)]),
        ]
        rows = Strapping.deconstruct(values)
        table = Tables.columntable(rows)
        @test table.values__kind == [:nothing, :value, :value, :value]
        @test table.values__length == [0, 0, 2, 2]
        @test Strapping.construct(Vector{OptionalResults}, rows) == values
    end

    @testset "collection field conversions" begin
        value = TaggedCollection(1, [2, 3])
        rows = Strapping.deconstruct(value)
        @test Tables.columntable(rows).values == ["2", "3"]
        @test Strapping.construct(TaggedCollection, rows) == value
    end
end
