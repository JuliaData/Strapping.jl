using Aqua
using JET

@testset "Aqua" begin
    Aqua.test_all(
        Strapping;
        ambiguities = false,
        deps_compat = true,
        piracies = true,
        project_extras = true,
        stale_deps = true,
        unbound_args = true,
        undefined_exports = true,
    )
    Aqua.test_ambiguities(Strapping)
end

@testset "JET" begin
    report = JET.report_package(Strapping; target_modules = (Strapping,))
    @test isempty(JET.get_reports(report))
end
