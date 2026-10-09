@testset "deconstruction" begin
    points = [Point(1, 2.5), Point(2, 3.5)]
    rows = Strapping.deconstruct(points)
    @test Tables.istable(typeof(rows))
    @test Tables.rowaccess(typeof(rows))
    @test length(rows) == 2
    @test Tables.schema(rows).names == (:x, :y)
    @test Tables.schema(Strapping.deconstruct(Person(1, "Ada"))).types == (Int, String)
    @test Tables.columntable(rows) == (x = [1, 2], y = [2.5, 3.5])
    @test Strapping.construct(Vector{Point}, rows) == points

    customer = Customer(1, "Ada", Address("Salt Lake City", 84101))
    @test Tables.columntable(Strapping.deconstruct(customer)) == (
        id = [1],
        name = ["Ada"],
        address_city = ["Salt Lake City"],
        address_zip = [84101],
    )

    coded = Coded(1, Code("alpha"))
    @test Tables.schema(Strapping.deconstruct(coded)).types == (Int, Any)
    @test Tables.columntable(Strapping.deconstruct(coded)) == (id = [1], code = ["alpha"])
    @test only(Strapping.construct(Vector{Coded}, Strapping.deconstruct(coded))).code ==
          Code("alpha")

    renamed = Renamed(9, Point(1, 2.5))
    @test Tables.columntable(Strapping.deconstruct(renamed)) ==
          (record_id = [9], coordinates_x = [1], coordinates_y = [2.5])

    result = Result(10, [3.14, 3.15, 3.16])
    table = Tables.columntable(Strapping.deconstruct(result))
    @test table ==
          (id = [10, 10, 10], values__length = [3, 3, 3], values = [3.14, 3.15, 3.16])
    @test Strapping.construct(Result, table) == result
    @test Strapping.construct(Vector{Result}, table) == [result]

    empty_result = Result(11, Float64[])
    empty_table = Tables.columntable(Strapping.deconstruct(empty_result))
    @test empty_table.id == [11]
    @test empty_table.values__length == [0]
    @test isequal(empty_table.values, [missing])
    @test Strapping.construct(Result, empty_table) == empty_result

    experiment = Experiment(1, "trial", Result(7, [1.0, 2.0]))
    experiment_table = Tables.columntable(Strapping.deconstruct(experiment))
    @test experiment_table == (
        id = [1, 1],
        name = ["trial", "trial"],
        result_id = [7, 7],
        result_values__length = [2, 2],
        result_values = [1.0, 2.0],
    )
    @test Strapping.construct(Experiment, experiment_table) == experiment

    empty_rows = Strapping.deconstruct(Point[])
    @test isempty(empty_rows)
    @test Tables.schema(empty_rows).names == (:x, :y)
end
