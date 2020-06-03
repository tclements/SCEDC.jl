export ec2transfer, s3query, scedcpath

"""
  ec2transfer(aws,filelist,OUTDIR)

Transfer files using pmap from S3 to EC2.
# Arguments
- `aws::AWSConfig`: AWSConfig configuration dictionary
- `bucket::String`: S3 bucket to download from.
- `filelist::Array{String}`: Filepaths to download in bucket.
- `OUTDIR::String`: The output directory on EC2 instance.

"""
function ec2transfer(aws::AWSConfig,bucket::String,filelist::Array{String},OUTDIR::String;)

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
	pmap(s3_file_map,fill(aws,length(outfiles)),fill(bucket,length(outfiles)),filelist,outfiles)

	println("Download Complete!        $(now())          ")
	tend = now()
	# check data in directory
	endsize = diskusage(OUTDIR)
	downloadsize = (endsize - startsize) / 1024
	downloadseconds = (tend - tstart).value / 1000
	println("Download took $(Dates.canonicalize(Dates.CompoundPeriod(tend - tstart)))")
	if downloadsize / downloadseconds > 1024
		downloadsize /= 1024
		println("Download rate $(downloadsize / downloadseconds) GB/s")
	else
		println("Download rate $(downloadsize / downloadseconds) MB/s")
	end
	return nothing
end

function diskusage(dir)
	s = read(`du -s $dir`, String)
	return parse(Int, split(s)[1])
end


function df_subset(df::DataFrame,col::String,colsymbol::Symbol)
        col = regex_helper(col)
        ind = findall(occursin.(col,df[!,colsymbol]))
        df = df[ind,:]
	return df
end

function df_subset(df::DataFrame,col::Nothing,colsymbol::Symbol)
	 return df
end

function regex_helper(reg::String)
    if reg == '*'
        # pass for all
    elseif occursin('?',reg)
        reg = replace(reg, '?' => '.')
        reg = Regex(reg)
    elseif occursin('*',reg)
        if reg[end] == '*'
                reg = '^' * strip(reg,'*')
        elseif reg[1] == '*'
                reg = strip(reg,'*') * '$'
        end
            reg = Regex(reg)
    end
    return reg
end

function s3_file_map(aws::AWSConfig,bucket::String,filein::String,fileout::String)
    s3_get_file(aws, bucket, filein, fileout)
    println("Downloading file: $filein       \r")
end

function filenamecleaner(filename::String)
	filename = strip(filename,'b')
	return replace(filename,"'"=>"")
end

function indexpath(d::Date)
    days = (d - Date(Year(d))).value + 1
    n = ndigits(days)
    outstring = "continuous_waveforms/index/csv/year="
    outstring *= string(Year(d).value) * "/year_doy="
    outstring *= string(Year(d).value) * '_' * ('0' ^ (3 - n)) * string(days)
    outstring *= "/index.csv"
    return outstring
end

"""
  scedcpath(filename)

Convert filename to scedc-pds path.
"""
function scedcpath(filename::String)
    year = filename[14:17]
    day = filename[18:20]
    return "continuous_waveforms/" * year * '/' * year * '_' * day * '/' * filename
end

"""
  s3query(aws,OUTDIR,startdate)

Use S3 to query SCEDC-pds database.

# Arguments
- `aws::AWSConfig`: AWSConfig configuration dictionary
- `OUTDIR::String`: The output directory.
- `startdate::Date`: The start day of the download.
- `enddate::Date`: The (optional) end day of the download.
- `network::String`: Network to download from. If network = "*" or is unspecified,
                       data is downloaded from all available networks.
- `station::String`: Station to download, e.g. "RFO". If station = "*" or is unspecified,
                       data is downloaded from all available stations.
- `channel::String`: Channels to download, e.g. "HH*". If channel = "*" or is unspecified,
                       data is downloaded from all available channels.
- `location::String`: Locations to download, e.g. "00". If channel = "*" or is unspecified,
                       data is downloaded from all available locations. NOTE: most files do
                       not have a location.
- `minlatitude::Float64`: Minimum latitude in data search.
- `maxlatitude::Float64`: Maximum latitude in data search.
- `minlongitude::Float64`: Minimum longitude in data search.
- `maxlongitude::Float64`: Maximum longitude in data search.
"""
function s3query(aws::AWSConfig,
                  startdate::Date;
			  	  enddate::Union{Date,Nothing}=nothing,
                  network::Union{String,Nothing}=nothing,
                  station::Union{String,Nothing}=nothing,
                  location::Union{String,Nothing}=nothing,
                  channel::Union{String,Nothing}=nothing,
                  minlatitude::Union{Float64,Nothing}=nothing,
                  maxlatitude::Union{Float64,Nothing}=nothing,
                  minlongitude::Union{Float64,Nothing}=nothing,
                  maxlongitude::Union{Float64,Nothing}=nothing)

	LOC =  !localhost_is_ec2() && error("scedctransfer must be run on an EC2 instance. Exiting.")
	@eval @everywhere aws=$aws
    firstdate = Date(2000,1,1)

	if isnothing(enddate)
		enddate = startdate
	end

    # check dates
    if startdate > now()
        throw(ArgumentError("Date must be earlier than today. Aborting download."))
    end

    if enddate < firstdate
        throw(ArgumentError("End date must be later than $firstdate. Aborting download."))
    end

    # download index for each day
	date_range = startdate:Day(1):enddate
	paths = indexpath.(date_range)
    N = length(date_range)
    params = [network,station,location,channel,minlatitude,minlatitude,minlongitude,maxlongitude]
    @everywhere paths=$paths
    dfs = pmap(getCSV,fill(aws,N),paths,[params for ii=1:N])
    dfs = vcat(dfs...)
    return scedcpath.(dfs[:ms_filename])
end

function getCSV(aws,path, params::AbstractArray)
    filedf = CSV.read(IOBuffer(s3_get(aws,"scedc-pds",path)))

    # subset dataframe
    # filter by lat/lon
    network,station,location,channel,minlatitude,maxlatitude,minlongitude,maxlongitude = params
    if !isnothing(minlatitude)
        filedf = filedf[filedf[:lat] .> minlatitude,:]
    end

    if !isnothing(maxlatitude)
        filedf = filedf[filedf[:lat] .< maxlatitude,:]
    end

    if !isnothing(minlongitude)
        filedf = filedf[filedf[:lon] .> minlongitude,:]
    end

    if !isnothing(maxlongitude)
        filedf = filedf[filedf[:lon] .< maxlongitude,:]
    end

    # filter stations
    filedf = df_subset(filedf,network,:net)
    filedf = df_subset(filedf,station,:sta)
    filedf = df_subset(filedf,channel,:seedchan)
    filedf = df_subset(filedf,location,:loc)
    return filedf
end
