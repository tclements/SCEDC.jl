module SCEDC

using Dates, DelimitedFiles, Distributed, Random
using AWS, AWSS3, CSV, DataFrames, SeisIO 
using SeisIO.Quake
@service S3 
@service Athena

# include modules
include("catalog.jl")
include("phases.jl")
include("events.jl")
include("s3.jl")
include("athena.jl")
include("ec2.jl")

end