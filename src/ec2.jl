export ec2download, ec2stream

"""
  ec2download(aws,filelist,OUTDIR)

Download files using pmap from S3 to EC2.
# Arguments
- `aws::AWSConfig`: AWSConfig configuration dictionary
- `bucket::String`: S3 bucket to download from.
- `filelist::Array{String}`: Filepaths to download in `bucket`.
- `OUTDIR::String`: The output directory on EC2 instance.

"""
function ec2download(
    aws::AWSConfig,bucket::String,filelist::Array{String},OUTDIR::String;
    v::Int=0
)

	# check being run on AWS
	tstart = now()
	!localhost_is_ec2() && @warn("Running locally. Run on EC2 for maximum performance.")

	println("Starting Download...      $(now())")
	println("Using $(nworkers()) cores...")

	# send files everywhere
	@eval @everywhere aws=$aws
	@eval @everywhere filelist=$filelist

	# create output files
	OUTDIR = expanduser(OUTDIR)
	outfiles = [joinpath(OUTDIR,f) for f in filelist]
	filedir = unique([dirname(f) for f in outfiles])
	for ii = 1:length(filedir)
		if !isdir(filedir[ii])
			mkpath(filedir[ii])
		end
	end

    # send outfiles everywhere
	@eval @everywhere outfiles=$outfiles
	# do transfer to ec2
    startsize = diskusage(OUTDIR)
    if v > 0
	    pmap(
            s3_file_map,
            fill(aws,length(outfiles)),
            fill(bucket,length(outfiles)),
            filelist,
            outfiles
        )
    else
        pmap(
            s3_get_file,
            fill(aws,length(outfiles)),
            fill(bucket,length(outfiles)),
            filelist,
            outfiles,
        )
    end

	println("Download Complete!        $(now())          ")
	tend = now()
	# check data in directory
	endsize = diskusage(OUTDIR)
	downloadsize = endsize - startsize
	downloadseconds = (tend - tstart).value / 1000
	println("Download took $(Dates.canonicalize(Dates.CompoundPeriod(tend - tstart)))")
	println("Download size $(formatbytes(downloadsize))")
	println("Download rate $(formatbytes(Int(round(downloadsize / downloadseconds))))/s")
	return nothing
end

"""
  ec2stream(aws,bucket,filelist)

Stream files using pmap from S3 to EC2.
# Arguments
- `aws::AWSConfig`: AWSConfig configuration dictionary
- `bucket::String`: S3 bucket to download from.
- `filelist::Array{String}`: Filepaths to stream from `bucket`.
- `demean::Bool`: Demean data after streaming.
- `detrend::Bool`: Detrend data after streaming.
- `msr::Bool`: Get multi-stage response after streaming.
- `prune::Bool`: Prune empty channels after streaming.
- `rr::Bool`: Remove instrument response after streaming.
- `taper::Bool`: Taper data after streaming.
- `ungap::Bool`: Ungap data after streaming.
- `resample::Bool`: Resample data after streaming.
- `fs::Float64`: New sampling rate.
- `rtype`: Return requested data as `SeisData` or `Array` of `SeisData`. Defaults
    to `SeisData`. Use `Array` to return an `Array` of `SeisData`.
"""
function ec2stream(
    aws::AWSConfig,bucket::String,filelist::Array{String};
    demean::Bool = false,
    detrend::Bool = false,
    msr::Bool = false,
    prune::Bool = false,
    rr::Bool = false,
    taper::Bool = false,
    ungap::Bool = false,
    unscale::Bool = false,
    resample::Bool = false,
	fs::Real = Float64(0),
    rtype = SeisData,
)

	# check being run on AWS
	!localhost_is_ec2() && @warn("Running locally. Run on EC2 for maximum performance.")

	# send files everywhere
	@eval @everywhere aws=$aws
    @eval @everywhere bucket=$bucket
	@eval @everywhere filelist=$filelist

	# do transfer to ec2
	Sarray =  pmap(
        s3_get_seed,
        fill(aws,length(filelist)),
        fill(bucket,length(filelist)),
        filelist,
        fill(demean,length(filelist)),
        fill(detrend,length(filelist)),
        fill(msr,length(filelist)),
        fill(prune,length(filelist)),
        fill(rr,length(filelist)),
        fill(taper,length(filelist)),
        fill(ungap,length(filelist)),
        fill(unscale,length(filelist)),
        fill(resample,length(filelist)),
        fill(fs,length(filelist)),
    )
    if rtype == SeisData
	    return merge(Sarray)
    end
    return Sarray

end

function s3_file_map(aws::AWSConfig,bucket::String,filein::String,fileout::String)
    s3_get_file(aws, bucket, filein, fileout)
    println("Downloading file: $filein       \r")
	return nothing
end

function s3_get_seed(
	aws::AWSConfig,bucket::String,
	filein::String,
    demean::Bool,
    detrend::Bool,
    msr::Bool,
    prune::Bool,
    rr::Bool,
    taper::Bool,
    ungap::Bool,
    unscale::Bool,
    resample::Bool,
    fs::Real,
)
    f = s3_get(aws, bucket, filein)
	S = parseseed(f)

	# remove empty channels
	if prune == true
		prune!(S)
	end

	# Get list of channels with sane instrument codes
	CC = get_seis_channels(S)

	if msr == true
		@warn("Getting response not implemented yet.")
	end

	# unscale
	if unscale == true
  		unscale!(S, chans=CC)
	end

	# Demean
	if demean == true
	  demean!(S, chans=CC)
	end

	# Taper
	if taper == true
	  taper!(S, chans=CC)
	end

	# Ungap
	if ungap == true
	  ungap!(S, chans=CC)
	end

	# resample data
	if resample == true && fs != 0
		resample!(S, chans=CC, fs=fs)
	end

	# Remove response
	# need to implement attaching response
	if rr == true
	  @warn("Removing response not implemented yet.")
	end

	return S
end

function formatbytes(bytes::Real, digits::Int=1)
	units = ["B", "KB", "MB", "GB", "TB","PB"]
	bytes = max(bytes,0)
	pow = Int(floor((bytes > 0 ? log(bytes) : 0) / log(1024)))
	pow = min(pow,length(units))
	powind = pow < length(units) ? pow + 1 : pow
	return string(round(bytes / 1024 ^ pow,digits=digits)) * units[powind]
end

function diskusage(dir)
	s = read(`du -s -b $dir`, String)
	return parse(Int, split(s)[1])
end

"""
  parseseed(f)

Convert uint8 data to SeisData.
"""
function parseseed(f::AbstractArray)
    S = SeisData()
    SeisIO.SEED.parsemseed!(S,IOBuffer(f),SeisIO.KW.nx_new,SeisIO.KW.nx_add,false,0)
    return S
end
