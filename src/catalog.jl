export catalogquery

"""
    read_catalog(catalog)

Read a SCEDC-format listing of catalog data from the Southern California Earthquake Data
Center. For a format description, see http://www.data.scec.org/ftp/catalogs/SCEC_DC.

Catalog maybe a filepath or an `IOStream`. 
"""
function read_catalog(catalog)
    cols = [
        "YYYY/MM/DD",
        "HH:mm:SS.ss",
        "ET",
        "GT",
        "MAG",
        "M",
        "LAT",
        "LON",
        "DEPTH",
        "Q",
        "EVID",
        "NPH",
        "NGRM",
    ]
    types = [
        String,
        String,
        String,
        String,
        Float64,
        String,
        Float64,
        Float64,
        Float64,
        String,
        Int,
        Int,
        Int,
    ]

    # read into dataframe
    df = CSV.File(
        catalog,
        header=cols,
        datarow=11,
        types=types,
        comment="#",
        delim=' ',
        ignorerepeated=true,
        silencewarnings=true,
    ) |> DataFrame
    dropmissing!(df)

    # format dates 
    dateformater = Dates.DateFormat("y/m/dTH:M:S.s")
    df[!,"EVENTTIME"] = DateTime.(parse_hms.(df[!,"YYYY/MM/DD"],df[!,"HH:mm:SS.ss"]),dateformater)
    select!(df,Not([Symbol("YYYY/MM/DD"),Symbol("HH:mm:SS.ss")]))
    return df
end
        
"""
    catalogquery()

Use S3 to query SCEDC-pds event catalog. Returns a DataFrame of events.

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
function catalogquery(aws::AWSConfig;
    starttime::TimeType=today(),
    endtime::TimeType=today(),
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
    all_types = ["eq","qb","sn","nt","uk","*"]
    # do filein
    @assert starttime <= endtime "Starttime ($starttime) must be less than endtime ($endtime)."
    @assert minlatitude >= -90 "Minimum latitude must be greater than -90"
    @assert maxlatitude <= 90 "Maximum latitude must be less than 180"
    @assert minlongitude >= -180 "Minimum longitude must be greater than -180"
    @assert maxlongitude <= 180 "Maximum longitude must be less than 180"
    @assert minmagnitude >= -10 "Minimum magnitude must be greater than -10"
    @assert maxmagnitude <= 10 "Maximum magnitude must be less than 10"
    @assert in(lowercase(eventtype), all_types) "eventtype '$eventtype' must be one of $all_types"

    # get each year 
    years = year(starttime):year(endtime)
    alldf = DataFrame()
    for yr in years
        yrstream = s3_get(aws,"scedc-pds","earthquake_catalogs/SCEC_DC/$yr.catalog")
        df = read_catalog(yrstream)
        filter!(row -> starttime <= row.EVENTTIME <= endtime, df)
        filter!(row -> minlatitude <= row.LAT <= maxlatitude,df)
        filter!(row -> minlongitude <= row.LON <= maxlongitude,df)
        filter!(row -> minmagnitude <= row.MAG <= maxmagnitude,df)
        filter!(row -> mindepth <= row.DEPTH <= maxdepth,df)
        if eventtype != "*"
            filter!(row -> row.ET == eventtype, df)
        end
        if !isempty(df)
            append!(alldf,df)
        end
    end

    if isempty(alldf)
        throw(DomainError("No events for query."))
    end
    return alldf   
end
catalogquery(a...;b...) = catalogquery(global_aws_config(region="us-west-2"), a...; b...)
catalogquery(d::Dict, a...;b...) = catalogquery(global_aws_config(region=d[:region]), a...; b...)

function parse_hms(d::String,hms::String)
    # check that date matches HH:MM:SS.s syntax 
    r = r"^[0-2][0-3]:[0-5][0-9]:[0:5][0-9].[0-9][0-9]"
    if occursin(r,hms)
        return d * "T" * hms
    end

    # string does not match 
    strm=match(r"(?<hour>\d+):(?<minute>\d+):(?<second>\d+).(?<dec>\d+)",hms)
    dec = parse(Int,strm[:dec])
    h = parse(Int,strm[:hour])
    m = parse(Int,strm[:minute])
    s = parse(Int,strm[:second])

    if s > 59 
        m +=  s ÷ 60 
        s = s % 60
    end

    if m > 59 
        h += m ÷ 60 
        m = m % 60 
    end

    if h > 23
        newday = h ÷ 24
        h = h % 24 
        dateformater = DateFormat("yyyy/mm/dd")
        d = Dates.format(Date(d,dateformater)+ Day(newday),dateformater)
    end

    h = lpad(string(h),2,'0')
    m = lpad(string(m),2,'0')
    s = lpad(string(s),2,'0')
    return d * "T$h:$m:$s.$dec"
end