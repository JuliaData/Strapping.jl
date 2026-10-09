# Strapping.jl development guide

## Purpose

Strapping maps concrete Julia structs to and from Tables.jl row tables. The
package uses StructUtils.jl for field metadata and value construction.

## Interface

Keep the user interface small and namespaced:

- `Strapping.construct`
- `Strapping.deconstruct`
- `Strapping.Style`

Do not add exports without a clear user need.

## Source layout

- `src/style.jl`: public style and errors.
- `src/plan.jl`: type-derived mapping plan.
- `src/construct.jl`: Tables rows to structs.
- `src/deconstruct.jl`: structs to a lazy Tables row table.

Keep schema discovery separate from per-row value access. Every access path,
including the flat column-table fast path, must consume the same `Plan` and
must pass the same behavior tests.

## Required validation

Run these commands from the repository root:

```sh
julia --project=. -e 'using Pkg; Pkg.test()'
julia --project=docs -e 'using Pkg; Pkg.develop(PackageSpec(path=pwd())); Pkg.instantiate()'
julia --project=docs docs/make.jl
STRAPPING_QUALITY=true julia --project=. -e 'using Pkg; Pkg.test()'
```

Add a named issue regression for every fixed mapping bug. Test both directions
when the mapping is intended to round-trip. Test Tables.jl sinks when schema or
row access changes.
