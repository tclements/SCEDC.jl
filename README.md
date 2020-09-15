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

## Quickstart

The SCEDC AWS data set has four different products:

- Continuous waveforms from 2000 - present, stored as daily mseed files and returned as `SeisIO.SeisData`
- Event catalogs going back to 1932, returned as a `DataFrame`
- Event phase picks going back to 1932, returned as `SeisIO.SeisEvent`
- Event waveforms going back to 1977, return as `SeisIO.SeisEvent`

### Continuous Data Requests
SCEDC.jl supports direct download and file streaming for continuous data requests. 

```julia
# download data using SCEDC on AWS
using SCEDC, Dates, AWSCore
aws = aws_config(region="us-west-2")
bucket = "scedc-pds"
startdate = Date("2016-07-01")
enddate = Date("2016-07-01")
network = "CI"
channel = "LH?"
OUTDIR = "~/data"

# query s3 for matching stations
filelist = s3query(aws, startdate, enddate=enddate, network=network,channel=channel)

# download mseed files to disk 
ec2download(aws,bucket,filelist,OUTDIR)

# or stream to an Array of SeisIO.SeisData 
LHs = ec2stream(aws,bucket,filelist)
```

To do parallel data transfers, use the `addprocs` function from the `Distributed` module: 
```julia
using Distributed
addprocs()
ec2download(aws,bucket,filelist,"~/paralleldir")
```

### Earthquake Catalog Requests 

SCEDC.jl queries the SCSN earthquake catalog using these possible parameters:
- minimum and maximum latitude (`minlatitude` & `maxlatitude`)
- minimum and maximum longitude (`minlongitude` & `maxlongitude`)
- minimum and maximum magnitude (`minmagnitude` & `maxmagnitude`)
- minimum and maximum depth [in km - positive is down] (`mindepth` & `maxdepth`)
- The start time/day of the event query (`starttime::TimeType`)
- The end time/day of the event query (`endtime::TimeType`)
- Event type (`eventtype`: {"eq"=>"earthquake", "qb"=> "quarry blast", 
    "sn"=>"sonic boom', "nt"=>"nuclear blast", "uk"=>"not reported","*"=>"all events"})

```julia
julia> df = catalogquery(
    aws,
    starttime = DateTime(1937,1,1),
    endtime=DateTime(1937,2,1),
    minlatitude=33.,
    maxlongitude=-116,
    minmagnitude=3.
)
6×12 DataFrame
│ Row │ ET     │ GT     │ MAG     │ M      │ LAT     │ LON      │ DEPTH   │ Q      │ EVID    │ NPH   │ NGRM  │ EVENTTIME              │
│     │ String │ String │ Float64 │ String │ Float64 │ Float64  │ Float64 │ String │ Int64   │ Int64 │ Int64 │ DateTime               │
├─────┼────────┼────────┼─────────┼────────┼─────────┼──────────┼─────────┼────────┼─────────┼───────┼───────┼────────────────────────┤
│ 1   │ eq     │ l      │ 3.05    │ l      │ 33.95   │ -116.8   │ 6.0     │ C      │ 3364067 │ 9     │ 0     │ 1937-01-09T10:41:00.64 │
│ 2   │ eq     │ l      │ 3.02    │ h      │ 34.727  │ -120.994 │ 6.0     │ D      │ 3364073 │ 7     │ 0     │ 1937-01-12T15:44:20.07 │
│ 3   │ eq     │ l      │ 3.89    │ l      │ 33.521  │ -118.137 │ 6.0     │ C      │ 3364076 │ 10    │ 0     │ 1937-01-15T18:35:46.75 │
│ 4   │ eq     │ l      │ 3.09    │ l      │ 34.124  │ -117.457 │ 11.5    │ B      │ 3364082 │ 13    │ 0     │ 1937-01-18T10:34:52.53 │
│ 5   │ eq     │ l      │ 3.78    │ l      │ 35.645  │ -118.199 │ 6.0     │ C      │ 3364084 │ 14    │ 0     │ 1937-01-19T23:57:38.42 │
│ 6   │ eq     │ l      │ 3.1     │ l      │ 33.886  │ -116.87  │ 6.0     │ C      │ 3364086 │ 8     │ 0     │ 1937-01-20T19:04:00.24 │
```

### Phase Pick Requests 
SCEDC.jl can retrieve phase picks for an earthquake given the event's ID and date. Here is an example from the 1987 Elmore Ranch earthquake with data returned as a `SeisIO.SeisEvent`

```julia
julia> phasequery(aws,628016,Date(1987,11,24))
Event 628016: SeisEvent with 70 channels

(.hdr)
    ID: 628016
   INT:  0
   LOC: 33.015 N, -115.852 E, 11.18 km
   MAG: Mw 6.6 (g 0.0°, n 70)
    OT: 1987-11-24T13:15:56.708
   SRC:                                                                                                                                   
   TYP: earthquake                                                                                                                        
  MISC: 0 items
 NOTES: 0 entries

(.source)
    ID: 
   EID: ""
    M0: 0.0
    MT: Float64[]
    DM: Float64[]
  NPOL: 0
   GAP: 0.0
   PAX: Array{Float64}(undef,0,0)
PLANES: Array{Float64}(undef,0,0)
   SRC: 
    ST: dur 0.0, rise 0.0, decay 0.0                                            
  MISC: 0 items
 NOTES: 0 entries

(.data)
SeisIO.Quake.EventTraceData with 70 channels
```

### Event Waveform Requests
SCEDC.jl can stream waveforms (with associated phase picks) from earthquakes going back to 1977. Syntax is similar to `catalogquery`:
- minimum and maximum latitude (`minlatitude` & `maxlatitude`)
- minimum and maximum longitude (`minlongitude` & `maxlongitude`)
- minimum and maximum magnitude (`minmagnitude` & `maxmagnitude`)
- minimum and maximum depth [in km - positive is down] (`mindepth` & `maxdepth`)
- The start time/day of the event query (`starttime::TimeType`)
- The end time/day of the event query (`endtime::TimeType`)
- Event type (`eventtype`: {"eq"=>"earthquake", "qb"=> "quarry blast", 
    "sn"=>"sonic boom', "nt"=>"nuclear blast", "uk"=>"not reported","*"=>"all events"})
    
Here is an example of an requesting events from 2019, returned as an `Array` of `SeisEvent` 
```julia
julia> EQ = eventstream(aws,starttime=Date(2019,1,1),endtime=Date(2019,1,2),minmagnitude=2.)
julia> EQ[1].data
SeisIO.Quake.EventTraceData with 2909 channels (3 shown)
    ID: AZ.BZN..BHE                        AZ.BZN..BHN                        AZ.BZN..BHZ                        …
  NAME: AZ.BZN..BHE                        AZ.BZN..BHN                        AZ.BZN..BHZ                        …
   LOC: 0.0 N, 0.0 E, 0.0 m                0.0 N, 0.0 E, 0.0 m                0.0 N, 0.0 E, 0.0 m                …
    FS: 40.0                               40.0                               40.0                               …
  GAIN: 1.0                                1.0                                1.0                                …
  RESP: a0 1.0, f0 1.0, 0z, 0p             a0 1.0, f0 1.0, 0z, 0p             a0 1.0, f0 1.0, 0z, 0p             …
 UNITS:                                                                                                          …
    AZ: 0.0                                0.0                                0.0                                …
   BAZ: 0.0                                0.0                                0.0                                …
  DIST: 0.0                                0.0                                0.0                                …
   PHA: 0 phases                           0 phases                           0 phases                           …
   SRC:                                                                                                          …
  MISC: 0 entries                          0 entries                          0 entries                          …
 NOTES: 0 entries                          0 entries                          0 entries                          …
     T: 2019-01-01T12:01:26 (0 gaps)       2019-01-01T12:01:22 (0 gaps)       2019-01-01T12:01:25 (0 gaps)       …
     X: +4.640e+02                         +1.005e+03                         +2.992e+03                         …
        +4.530e+02                         +1.002e+03                         +2.981e+03                         …
            ...                                ...                                ...                            …
        -2.910e+02                         +1.244e+03                         +2.671e+03                         …
        (nx = 4591)                        (nx = 5166)                        (nx = 4824)                        …
```
