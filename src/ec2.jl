export ec2download, ec2stream, getXML

"""
  ec2download(bucket,filelist,OUTDIR)

Download files using pmap from S3 to EC2.
# Arguments
- `bucket::String`: S3 bucket to download from.
- `filelist::Array{String}`: Filepaths to download in `bucket`.
- `OUTDIR::String`: The output directory on EC2 instance.

# Keywords
- `v::Int=0`: Verbosity level. Set v = 1 for download progress.
- `XML::Bool=false`: Download StationXML files for request. Downloads StationXML to
    `joinpath(OUTDIR,"XML")`.

"""
function ec2download(
	aws::AWSConfig,
	bucket::String,
	filelist::Array{String},
	OUTDIR::String;
	v::Int=0,
	XML::Bool=false,
)

	# check being run on AWS
	tstart = now()
	!AWS.localhost_is_ec2() && @warn("Running locally. Run on EC2 for maximum performance.")

	println("Starting Download...      $(now())")
	println("Using $(nworkers()) cores...")

	# send files everywhere
	@eval @everywhere filelist=$filelist
	@eval @everywhere aws=$aws

	# create output files
	OUTDIR = expanduser(OUTDIR)
	outfiles = [joinpath(OUTDIR,f) for f in filelist]
	filedir = unique([dirname(f) for f in outfiles])
	for ii = 1:length(filedir)
		if !isdir(filedir[ii])
			mkpath(filedir[ii])
		end
	end

	# get XML
	if XML
		XMLDIR = joinpath(OUTDIR,"XML")
		getXML(aws,bucket,filelist,XMLDIR,v=v)
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
ec2download(a...;b...) = ec2download(global_aws_config(region="us-west-2"), a...; b...)

"""
  ec2stream(bucket,filelist)

Stream files using pmap from S3 to EC2.
# Arguments
- `bucket::String`: S3 bucket to download from.
- `filelist::Array{String}`: Filepaths to stream from `bucket`.

# Keywords
- `demean::Bool=false`: Demean data after streaming.
- `detrend::Bool=false`: Detrend data after streaming.
- `msr::Bool=false`: Get multi-stage response after streaming.
- `prune::Bool=false`: Prune empty channels after streaming.
- `rr::Bool=false`: Remove instrument response after streaming.
- `taper::Bool=false`: Taper data after streaming.
- `ungap::Bool=false`: Ungap data after streaming.
- `resample::Bool=false`: Resample data after streaming.
- `fs::Float64=0`: New sampling rate.
- `rtype`: Return requested data as `SeisData` or `Array` of `SeisData`. Defaults
    to `SeisData`. Use `Array` to return an `Array` of `SeisData`.
"""
function ec2stream(
	aws::AWSConfig,
	bucket::String,
	filelist::Array{String};
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
	!AWS.localhost_is_ec2() && @warn("Running locally. Run on EC2 for maximum performance.")

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
	    return SeisData(Sarray...)
    end
    return Sarray
end
ec2stream(a...;b...) = ec2stream(global_aws_config(region="us-west-2"), a...; b...)

"""
  getXML(bucket,filelist,XMLDIR)

Download XML files using pmap from S3 to EC2.
# Arguments
- `bucket::String`: S3 bucket to download from.
- `filelist::Array{String}`: Filepaths to download in `bucket`.
- `XMLDIR::String`: The output directory for stationXML files on EC2 instance.

# Keywords
- `v::Int=0`: Verbosity level. Set v = 1 for download progress.
- `getall::Bool=false`: Download all available StationXML files from `scedc-pds`.
"""
function getXML(
	aws::AWSConfig,
	bucket::String,
	filelist::AbstractArray,
	XMLDIR::String;
	v::Int=0,
	getall::Bool=false,
)

	if !isdir(XMLDIR)
		mkpath(XMLDIR)
	end

	# get all files if needed
	prefix = "FDSNstationXML/CI/" # change this if more nets added
	if getall
		filelist = collect(s3_list_keys(bucket,prefix))
		filelist = [replace(f,"_"=>"") for f in filelist]
		filelist = [replace(f,".xml"=>"__") for f in filelist]
	end

	basenames = basename.(filelist)
	nets = [b[1:2] for b in basenames]
	stas = [replace(b[3:7],"_"=>"") for b in basenames]

	# only get response for CI station
	ind = findall(nets .== "CI")
	if length(ind) > 0
		nets = nets[ind]
		stas = stas[ind]
	else
		@warn("No XML for request.")
	end

	# get input/output files names
	infiles = [joinpath(prefix,"$(nets[ii])_$(stas[ii]).xml") for ii = 1:length(nets)]
	infiles = unique(infiles)
	outfiles = [joinpath(XMLDIR,basename(f)) for f in infiles]

	# download files
	if v > 0
	    pmap(
			s3_file_map,
			fill(aws,length(infiles)),
			fill(bucket,length(infiles)),
			infiles,
			outfiles,
        )
    else
		pmap(
			s3_get_file,
			fill(aws,length(infiles)),
			fill(bucket,length(infiles)),
			infiles,
			outfiles,
		)
    end
	return nothing
end
getXML(a...;b...) = getXML(global_aws_config(region="us-west-2"),a...;b...)

function s3_file_map(aws::AWSConfig,bucket::String,filein::String,fileout::String)
    s3_get_file(aws,bucket, filein, fileout)
    println("Downloading file: $filein       \r")
	return nothing
end
s3_file_map(a...) = s3_file_map(global_aws_config(region="us-west-2"),a...)

function s3_get_seed(
	aws::AWSConfig,
	bucket::String,
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
    stream = S3.get_object(bucket, filein,aws_config=aws)
	S = parseseed(stream)

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
s3_get_seed(a...;b...) = s3_get_seed(global_aws_config(region="us-west-2"),a...;b...)

function formatbytes(bytes::Real, digits::Int=1)
	units = ["B", "KB", "MB", "GB", "TB","PB"]
	bytes = max(bytes,0)
	pow = Int(floor((bytes > 0 ? log(bytes) : 0) / log(1024)))
	pow = min(pow,length(units))
	powind = pow < length(units) ? pow + 1 : pow
	return string(round(bytes / 1024 ^ pow,digits=digits)) * units[powind]
end

function diskusage(dir)
	s = read(`du -k $dir`, String)
	kb = parse(Int, split(s)[1])
	return 1024 * kb
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
