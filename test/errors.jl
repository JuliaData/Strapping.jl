@testset "errors" begin
    @test_throws Strapping.Error Strapping.construct(Point, (x = Int[], y = Float64[]))
    @test_logs (:warn, r"additional table rows") Strapping.construct(
        Point,
        (x = [1, 2], y = [2.5, 3.5]),
    )
    @test_logs min_level = Logging.Error Strapping.construct(
        Point,
        (x = [1, 2], y = [2.5, 3.5]);
        silencewarnings = true,
    )

    @test_throws Strapping.Error Strapping.deconstruct(TwoCollections([1], [2]))
    @test_throws Strapping.Error Strapping.deconstruct(DuplicateColumns(1, 2))
    @test_throws Strapping.Error Strapping.construct(
        Vector{NoIdCollection},
        (id = [1, 1], values__length = [2, 2], values = [1, 2]),
    )
    @test_throws Strapping.Error Strapping.construct(
        Vector{Result},
        (id = [1, 2, 1], values__length = [1, 1, 1], values = [1.0, 2.0, 3.0]),
    )
end
