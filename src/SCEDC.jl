module SCEDC

using Dates, CSV, DataFrames, AWSS3, AWSCore, AWSSDK, Random, DelimitedFiles, Distributed

# include modules
include("s3.jl")
include("athena.jl")
include("ec2.jl")

end
