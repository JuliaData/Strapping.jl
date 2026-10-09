@testset "construction" begin
    @test Strapping.construct(Point, (x = [1], y = [2.5])) == Point(1, 2.5)
    @test Strapping.construct(Vector{Point}, (x = [1, 2], y = [2.5, 3.5])) ==
          [Point(1, 2.5), Point(2, 3.5)]
    @test Strapping.construct(Vector{Point}, (x = Int[], y = Float64[])) == Point[]

    record_type = NamedTuple{(:id, :name),Tuple{Int,String}}
    record = Strapping.construct(record_type, (name = ["Ada"], id = [7]))
    @test record == (id = 7, name = "Ada")
    @test Strapping.construct(record_type, Strapping.deconstruct(record)) == record

    # Column order does not control struct field order.
    @test Strapping.construct(Person, [(name = "Ada", id = 7)]) == Person(7, "Ada")

    mutable_point = Strapping.construct(MutablePoint, [(y = 2.5, x = 1)])
    @test (mutable_point.x, mutable_point.y) == (1, 2.5)

    customer = Strapping.construct(
        Customer,
        [(id = 1, name = "Ada", address_zip = 84101, address_city = "Salt Lake City")],
    )
    @test customer == Customer(1, "Ada", Address("Salt Lake City", 84101))

    coded = Strapping.construct(Coded, [(id = 1, code = :alpha)])
    @test coded.id == 1
    @test coded.code == Code("alpha")

    renamed = Strapping.construct(
        Renamed,
        [(coordinates_y = 2.5, record_id = 9, coordinates_x = 1)],
    )
    @test renamed == Renamed(9, Point(1, 2.5))

    ignored =
        Strapping.construct(WithIgnored, [(visible = "yes", id = 3, cache = "ignored")])
    @test ignored == WithIgnored(3, "yes", "default")

    @test Strapping.construct(OptionalScalar, [(value = nothing, name = "none")]) ==
          OptionalScalar("none", nothing)
    @test Strapping.construct(OptionalScalar, [(value = 4, name = "some")]) ==
          OptionalScalar("some", 4)
end
