module SCEDC

using Dates, DelimitedFiles, Distributed, Random
using AWSCore, AWSS3, AWSSDK,CSV, DataFrames, SeisIO 

# include modules
include("s3.jl")
include("athena.jl")
include("ec2.jl")

end
