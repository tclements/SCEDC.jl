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
function ec2download(aws::AWSConfig,bucket::String,filelist::Array{String},OUTDIR::String;
                     v::Int=0)

	# check being run on AWS
	tstart = now()
	LOC =  !localhost_is_ec2() && error("ec2transfer must be run on an EC2 instance. Exiting.")

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
	       pmap(s3_file_map,fill(aws,length(outfiles)),fill(bucket,length(outfiles)),filelist,outfiles)
       else
           pmap(s3_get_file,fill(aws,length(outfiles)),fill(bucket,length(outfiles)),filelist,outfiles)
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

"""
function ec2stream(aws::AWSConfig,bucket::String,filelist::Array{String})

	# check being run on AWS
	LOC =  !localhost_is_ec2() && error("ec2stream must be run on an EC2 instance. Exiting.")

	# send files everywhere
	@eval @everywhere aws=$aws
    @eval @everywhere bucket=$bucket
	@eval @everywhere filelist=$filelist

	# do transfer to ec2
	return pmap(s3_get_seed,fill(aws,length(filelist)),fill(bucket,length(filelist)),filelist)
end

function s3_file_map(aws::AWSConfig,bucket::String,filein::String,fileout::String)
    s3_get_file(aws, bucket, filein, fileout)
    println("Downloading file: $filein       \r")
end

function s3_get_seed(aws::AWSConfig,bucket::String,filein::String)
    f = s3_get(aws, bucket, filein)
    return parseseed(f)
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

function parseseed(f::AbstractArray)
    S = SeisData()
    SeisIO.SEED.parsemseed!(S,IOBuffer(f),SeisIO.KW.nx_new,SeisIO.KW.nx_add,false,0)
    return S
end
