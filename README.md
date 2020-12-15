### Strapping.jl

[![CI](https://github.com/JuliaData/Strapping.jl/workflows/CI/badge.svg)](https://github.com/JuliaData/Strapping.jl/actions?query=workflow%3ACI)
[![codecov](https://codecov.io/gh/JuliaData/Strapping.jl/branch/master/graph/badge.svg)](https://codecov.io/gh/JuliaData/Strapping.jl)
[![deps](https://juliahub.com/docs/Strapping/deps.svg)](https://juliahub.com/ui/Packages/Strapping/n0fkj?t=2)
[![version](https://juliahub.com/docs/Strapping/version.svg)](https://juliahub.com/ui/Packages/Strapping/n0fkj)
[![pkgeval](https://juliahub.com/docs/Strapping/pkgeval.svg)](https://juliahub.com/ui/Packages/Strapping/n0fkj)

"Strapping" stands for **ST**ruct **R**elational M**APPING**, and provides ORM-like functionality for Julia, including:

* automatically constructing Julia structs from any [Tables.jl](https://github.com/JuliaData/Tables.jl)-compatible source (see `?Strapping.construct`)
* ability to handle complicated aggregate types, with aggregate or collection fields
* integration with the [StructTypes.jl](https://github.com/JuliaData/StructTypes.jl) package for specifying struct and struct field options
* transform any Julia struct (or vector of structs) into a 2D Tables.jl-compatible source, which can be stored in a database, file format, or other "sink" (see `?Strapping.deconstruct`)

### Documentation

[![Stable](https://img.shields.io/badge/docs-stable-blue.svg)](https://juliadata.github.io/Strapping.jl/stable)
[![Dev](https://img.shields.io/badge/docs-dev-blue.svg)](https://juliadata.github.io/Strapping.jl/dev)
