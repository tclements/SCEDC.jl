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
    "c"=> 'U', 
    "u"=> 'U', 
    "d"=> 'D', 
    "r"=> ' ', 
    "."=> ' ',
)

function read_phase(phase)
    pstr = String(phase)
    parr = split(pstr,"\n")
    parr = strip.(parr[1:end-1])
    event_header = String(parr[1])
    parr = parr[2:end]
    N = length(parr)

    # get event info from first line of phase file 
    evid, etype, origin_time, eqmag, eqloc = parse_event_header(event_header,N)

    # make EventChannels
    EQ = EventTraceData(N)
    for ii = 1:N
        EQ[ii] = parse_phase(String(parr[ii]))
    end
    
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

function parse_phase(phase_string::String)
    net, sta, chan, loc, lat, lon, elev, phase, motion, onset, quality, dist, offset = split(phase_string)
    net, sta, chan, loc, phase, motion, onset, quality = String.([net, sta, chan, loc, phase, motion, onset, quality])
    polarity = POLARITY_MAPPING[motion[1:1]]
    if polarity == ""
        polarity = POLARITY_MAPPING[motion[2:2]]
    end
    lat, lon, elev, quality, dist, offset = parse.(Float64,[lat, lon, elev, quality, dist, offset])

    # construct EventChannel
    id = join([net,sta,loc,chan],".")
    loc = GeoLoc(lat=lat,lon=lon,el=elev)
    pha = SeisPha(d=dist,tt=offset,pol=polarity)
    phacat = Dict(phase => pha)
    return EventChannel(id=id,loc=loc,dist=dist,pha=phacat)
end

"""
    phasequery(aws,eventid,eventtime)

Use S3 to query SCEDC-pds phase arrivals for eventid == `eventid`.

# Arguments
- `aws::AWSConfig`: AWSConfig configuration dictionary
- `eventid::Int`: SCSN event ID.
- `eventtime::TimeType`: Time of event. 

Returns a `SeisIO.Quake.SeisEvent`
"""
function phasequery(aws::AWSConfig,eventid::Int,eventtime::TimeType)
    daynum = lpad(dayofyear(eventtime),3,'0')
    yr = year(eventtime)
    query = "event_phases/$yr/$(yr)_$daynum/$eventid.phase"

    if s3_exists(aws,"scedc-pds",query)
        phasestream = s3_get(aws,"scedc-pds",query)
    else
        throw(ErrorException("Event $eventid on $eventtime could not be found."))
    end

    return read_phase(phasestream)
end
