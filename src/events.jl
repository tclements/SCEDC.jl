export eventpaths, eventstream
import SeisIO: merge,merge!

"""
     eventpaths(aws)

Use S3 to query SCEDC-pds event paths. Returns array of event S3 paths.

# Arguments
- `aws::AWSConfig`: AWSConfig configuration dictionary

# Keywords
- `starttime::TimeType`: The start time/day of the event query.
- `endtime::TimeType`: The end time/day of the event query.
- `minlatitude::Real`: Minimum latitude in event query.
- `maxlatitude::Real`: Maximum latitude in event query.
- `minlongitude::Real`: Minimum longitude in event query.
- `maxlongitude::Real`: Maximum longitude in event query.
- `minmagnitude::Real`: Minimum magnitude in event query.
- `maxmagnitude::Real`: Maximum magnitude in event query.
- `mindepth::Real`: Minimum event depth in km.
- `maxdepth::Real`: Maximum event depth in km.
- `eventtype::String`: Event type: {"eq"=>"earthquake", "qb"=> "quarry blast", 
"sn"=>"sonic boom', "nt"=>"nuclear blast", "uk"=>"not reported","*"=>"all events"}
"""
function eventpaths(
    aws::AWSConfig;
    starttime::TimeType=Date(1977,1,1),
    endtime::TimeType=Today(),
    minlatitude::Real=-90,
    maxlatitude::Real=90,
    minlongitude::Real=-180,
    maxlongitude::Real=180,
    minmagnitude::Real=-10,
    maxmagnitude::Real=10,
    mindepth::Real=-10,
    maxdepth::Real=100,
    eventtype::String="*",
)
    @assert starttime >= Date(1977,1,1) "Starttime must be greater or equal to $(Date(1977,1,1))"
    # quert possible events from catalog 
    eventdf = catalogquery(
        aws,
        starttime=starttime,
        endtime=endtime,
        minlatitude=minlatitude,
        maxlatitude=maxlatitude,
        minlongitude=minlongitude,
        maxlongitude=maxlongitude,
        minmagnitude=minmagnitude,
        mindepth=mindepth,
        maxdepth=maxdepth,
        eventtype=eventtype
    )

    N = size(eventdf,1)
    epaths = ["event_waveforms/$(year(row.EVENTTIME))/$(year(row.EVENTTIME))_$(lpad(dayofyear(row.EVENTTIME),3,'0'))/$(row.EVID).ms" for row in eachrow(eventdf)]
    ppaths = ["event_phases/$(year(row.EVENTTIME))/$(year(row.EVENTTIME))_$(lpad(dayofyear(row.EVENTTIME),3,'0'))/$(row.EVID).phase" for row in eachrow(eventdf)]
    paths = [epaths[ii] for ii = 1:length(epaths) if (s3_exists(aws,"scedc-pds",epaths[ii])) & (s3_exists(aws,"scedc-pds",ppaths[ii]))] 

    # check that events actually exist
    if isempty(paths)
        throw(ErrorException("No events for query."))
    end 
    return paths
end

function eventdownload()
end

function eventstream(aws::AWSConfig;
    starttime::TimeType=Date(1977,1,1),
    endtime::TimeType=Today(),
    minlatitude::Real=-90,
    maxlatitude::Real=90,
    minlongitude::Real=-180,
    maxlongitude::Real=180,
    minmagnitude::Real=-10,
    maxmagnitude::Real=10,
    mindepth::Real=-10,
    maxdepth::Real=100,
    eventtype::String="*",
)
    s3paths = eventpaths(
        aws,
        starttime=starttime,
        endtime=endtime,
        minlatitude=minlatitude,
        maxlatitude=maxlatitude,
        minlongitude=minlongitude,
        maxlongitude=maxlongitude,
        minmagnitude=minmagnitude,
        mindepth=mindepth,
        maxdepth=maxdepth,
        eventtype=eventtype
    )

    # grab event id and date from s3path
    evid = [parse(Int,replace(basename(s),".ms"=>"")) for s in s3paths]
    s3dates = date_yyyyddd.([basename(dirname(s)) for s in s3paths])
    s3phases = [phasequery(aws,evid[ii],s3dates[ii]) for ii = 1:length(evid)]
    s3events = ec2stream(aws,"scedc-pds",s3paths,rtype=Array)

    for ii = 1:length(s3events)
        s3phases[ii] = merge(s3phases[ii],s3events[ii])
    end
    return s3phases
end

function date_yyyyddd(yearday::String)
    @assert occursin(r"[1-2][0-9][0-9][0-9]_[0-3][0-6][0-9]",yearday)
    y,d = split(yearday,"_")
    yint = parse(Int,y)
    dint = parse(Int,d)
    @assert dint <= 366 "Input day must be less than or equal to 366"
    return Date(yint) + Day(dint-1)
end

function merge!(EC::EventChannel,SC::SeisChannel)
    if EC.id != SC.id 
        throw(ErrorException("EventChannel ID $(EC.id) and SeisChannel ID $(SC.id) do not match."))
    end
    for f in SeisIO.datafields
        fval = getfield(SC,f)
        if !isempty(fval)
            setfield!(EC,f,fval)
        end
    end
    return nothing
end
merge!(SC::SeisChannel,EC::EventChannel,) = merge!(EC,SC)
merge(EC::EventChannel,SC::SeisChannel) = (U = deepcopy(EC);merge!(U,SC);return U)
merge(SC::SeisChannel,EC::EventChannel) = merge(EC,SC)

function merge(SE::SeisEvent,SD::SeisData)
    newEvent = SeisEvent()
    newEvent.hdr = SE.hdr
    newEvent.source = SE.source
    newEvent.data = merge(SE.data,SD)
    return newEvent
end
merge(SD::SeisData,SE::SeisEvent) = merge(SE,SD)

function merge(ETD::EventTraceData,SD::SeisData)
    N = length(SD.id)
    ETDdict = Dict(ETD.id[ii]=>ii for ii = 1:length(ETD.id))
    ETDkeys = keys(ETDdict)
    newETD = EventTraceData(N)
    for ii = 1:N
        if in(SD.id[ii],ETDkeys)
            ind = ETDdict[SD.id[ii]]
            newETD[ii] = merge(SD[ii],ETD[ind])
        else
            newETD[ii] = EventTraceData(SD[ii])[1] # need to make EventChannel(SeisChannel())
        end
    end
    return newETD 
end
merge(SD::SeisData,ETD::EventTraceData) = merge(ETD,SD)
