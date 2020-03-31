# SCEDC

Julia API for downloading data from the SCEDC open dataset on AWS.

[![Stable](https://img.shields.io/badge/docs-stable-blue.svg)](https://tclements.github.io/SCEDC.jl/stable)
[![Dev](https://img.shields.io/badge/docs-dev-blue.svg)](https://tclements.github.io/SCEDC.jl/dev)
[![Build Status](https://travis-ci.com/tclements/SCEDC.jl.svg?branch=master)](https://travis-ci.com/tclements/SCEDC.jl)

**SCEDC.jl** is a Julia API for the for downloading data from the SCEDC (Southern
California Earthquake Data Center) Open Dataset hosted on Amazon Web Services.

## Installation

```julia
julia> using Pkg; Pkg.add(PackageSpec(url="https://github.com/tclements/SCEDC.jl", rev="master"))
```

**Note:** This project is *only* designed to work on AWS EC2 at the moment.

## Quickstart

```julia
# download data using SCEDC on AWS
using SCEDC, Dates

startdate = Date("2016-07-01")
enddate = Date("2016-07-01")
network = "CI"
channel = "HH?"
OUTDIR = "~/data"
scedctransfer(OUTDIR, startdate, enddate=enddate, network=network,
         channel=channel)
```
