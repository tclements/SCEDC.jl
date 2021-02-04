export phasequery
ETYPE_MAPPING = Dict(
    "eq"=>"earthquake", 
    "qb"=> "quarry blast", 
    "sn"=> "sonic boom", 
    "nt"=> "nuclear blast", 
    "uk"=> "not reported",
)

MAGTYPE_MAPPING = Dict( 
    "b"=> "Mb", 
    "e"=> "Me", 
    "l"=> "ML",
    "s"=> "MS", 
    "c"=> "Mc",
    "n"=> "", 
    "w"=> "Mw", 
    "h"=> "Mh", 
    "d"=> "Md", 
    "lr"=> "Mlr",
)

POLARITY_MAPPING = Dict(
    "c."=> 'U',
    ".c"=> 'U', 
    "u."=> 'U', 
    "d."=> 'D', 
    ".d"=> 'D',
    "r."=> ' ', 
    ".."=> ' ',
    ".."=> ' ',
    ""  => ' ', 
)

function read_phase(phasestream::AbstractArray)
    pstr = String(phasestream)
    parr = split(pstr,"\n")
    parr = strip.(parr[1:end-1])
    event_header = String(parr[1])
    parr = parr[2:end]
    N = length(parr)

    # get event info from first line of phase file 
    evid, etype, origin_time, eqmag, eqloc = parse_event_header(event_header,N)

    # need to wrap this 
    df = phasestring2df(parr)
    EQ = parse_phases(df)
    
    # make SeisHdr
    hdr = SeisHdr(id=evid,loc=eqloc,mag=eqmag,ot=origin_time,typ=etype)
    return SeisEvent(hdr=hdr,data=EQ)
end

function parse_event_header(header::String,N::Int)
    dateformater = Dates.DateFormat("y/m/d,H:M:S.s")
    evid, etype, geo, origin_time, lat, lon, depth, mag, magtype = split(header)
    evid = String(evid)
    lat = parse(Float64,lat)
    lon = parse(Float64,lon)
    depth = parse(Float64,depth)
    mag = parse(Float32,mag)
    origin_time = DateTime(origin_time,dateformater)
    etype = ETYPE_MAPPING[String(etype)]
    
    # make magnitude 
    eqmag = EQMag(val=mag,scale=MAGTYPE_MAPPING[magtype],src="SCSN",nst=N)

    # make location 
    eqloc = EQLoc(lat=lat,lon=lon,dep=depth,nst=N)
    return evid, etype, origin_time, eqmag, eqloc
end

"""
    phasequery(eventid,eventtime)

Use S3 to query SCEDC-pds phase arrivals for eventid == `eventid`.

# Arguments
- `eventid::Int`: SCSN event ID.
- `eventtime::TimeType`: Time of event. 

Returns a `SeisIO.Quake.SeisEvent`
"""
function phasequery(aws::AWSConfig,eventid::Int,eventtime::TimeType)
    daynum = lpad(dayofyear(eventtime),3,'0')
    yr = year(eventtime)
    query = "event_phases/$yr/$(yr)_$daynum/$eventid.phase"
    phasestream = Array{UInt8}(undef,0)
    try
        phasestream = s3_get(aws,"scedc-pds",query)
    catch e
        throw(e,"Event $eventid on $eventtime could not be found.")
    end

    return read_phase(phasestream)
end
phasequery(a...) = phasequery(global_aws_config(region="us-west-2"),a...)
phasequery(d::Dict,a...) = phasequery(global_aws_config(region=d[:region]),a...)

function phasestring2df(phasestring::AbstractArray)
    ns = [:NET,:STA,:CHAN,:LOC,:LAT,:LON,:ELEV,:PHASE,:MOTION,:ONSET,:QUAL,:DIST,:OFFSET]
    df = DataFrame(permutedims(String.(hcat(split.(phasestring)...))),ns)
    df[!,[:LAT,:LON,:ELEV,:QUAL,:DIST,:OFFSET]] .= parse.(Float64,df[!,[:LAT,:LON,:ELEV,:QUAL,:DIST,:OFFSET]])
    df[!,:LOC] .= replace.(df[!,:LOC],"--"=>"00")
    df[!,:POL] = map(x -> POLARITY_MAPPING[x], df[!,:MOTION])
    return df
end

function parse_phases(df::DataFrame)
    # construct EventChannel
    N = size(unique(df,[:NET,:STA,:CHAN]),1)
    EQ = EventTraceData(N)
    for (ii,group) in enumerate(groupby(df,[:NET,:STA,:CHAN]))
        EQ[ii] = df2phacat(group)
    end
    return EQ
end

function df2phacat(df::AbstractDataFrame)
    N = size(df,1)
    if N == 0 
        throw(ErrorException("Empty DataFrame."))
    end
    id = join(df[1,[:NET,:STA,:LOC,:CHAN]],'.')
    loc = GeoLoc(lat=df[1,:LAT],lon=df[1,:LON],el=df[1,:ELEV])
    dist = df[1,:DIST]
    phacat = Dict(df[ii,:PHASE]=>SeisPha(d=df[ii,:DIST],tt=df[ii,:OFFSET],pol=df[ii,:POL]) for ii = 1:N)
    return EventChannel(id=id,loc=loc,dist=dist,pha=phacat)
end