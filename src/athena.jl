export athenasetup, athenaquery
"""
  athenasetup(aws,bucket)

Create AWS Athena database and table for SQL queries.

# Arguments
- `aws::AWSConfig`: AWSConfig configuration dictionary
- `bucket::String`: Name of AWS bucket to store query results
"""
function athenasetup(aws::AWSConfig,bucket::String;
                    database::String="scedcindex",
                    table::String="scedc_parquet",
                    clean::Bool=true)

    # make sure a bucket exists for queries
    if bucket ∉ s3_list_buckets(aws)
        s3_create_bucket(aws,bucket)
    end

    # create database
    println("Creating database '$database' ",now())
    AWSSDK.Athena.start_query_execution(aws,QueryString="create database $database",
        ResultConfiguration=["OutputLocation" => "s3://$bucket/queries/"],
        ClientRequestToken=randstring(32))

    # string to create table
    tablestr = """CREATE EXTERNAL TABLE IF NOT EXISTS $table ( `ms_filename` string,""" *
               """ `net` string, `sta` string, `seedchan` string, `location` string,""" *
               """ `lat` double, `lon` double, `sample_rate` double ) PARTITIONED BY""" *
               """ ( year int, year_doy string ) STORED AS PARQUET LOCATION""" *
               """ 's3://scedc-pds/continuous_waveforms/index/parquet/'""" *
               """ TBLPROPERTIES ("parquet.compress"="SNAPPY");"""

    # create table
    println("Creating table '$table' ",now())
    AWSSDK.Athena.start_query_execution(aws,QueryString=tablestr,
       ResultConfiguration=["OutputLocation" => "s3://$bucket/queries/"],
       ClientRequestToken=randstring(32),
       QueryExecutionContext = ["Database" => database])

    # update partitions
    println("Repairing table '$table' ",now())
    output = AWSSDK.Athena.start_query_execution(aws,QueryString="MSCK REPAIR TABLE $table",
           ResultConfiguration=["OutputLocation" => "s3://$bucket/queries/"],
           ClientRequestToken=randstring(32),
           QueryExecutionContext = ["Database" => database])

    # wait until done
    finished = false
    while !finished
        queryresult = AWSSDK.Athena.get_query_execution(aws,output)
        if queryresult["QueryExecutionDetail"]["Status"] == "SUCCEEDED"
            finished = true
            println("REPAIR table $table finished ",now())
        else
            sleep(10)
        end
    end
   return nothing
end

"""
  athenaquery(aws,bucket,query)

Use AWS Athena to query SCEDC-pds database.

Possible queries use these parameters:
- `year::String`: Year data was recorded, in YYYY format.
- `year_doy::String`: Year and day data was recorded, in YYYY_DDD format.
- `ms_filename::String`: Name of miniSEED file, e.g. SBCPSLOBHZ01_2020153.ms
- `net::String`: Network identifier, e.g. CI, AZ, etc..
- `seedchan::String`: SEED channel, e.g. BHZ, HHN, LHE, etc..
- `location::String`: Station location, e.g. 00, 01, 00, etc..
- `lat::double`: Station latitude, e.g. 34.5
- `lon::double`: Station longitude, e.g. -117
- `sample_rate::double`: Station sampling rate, in Hz

Logical operators available for queries include:
    - `Logical Operators`: `and`, `or`, `not`
    - `Comparison Operators`: `<`, `>`, `<=`, `>=`, `=`, `!=`
    - `Range Operator`: `between`
See https://docs.aws.amazon.com/athena/latest/ug/presto-functions.html for possible operators.


# Arguments
- `aws::AWSConfig`: AWSConfig configuration dictionary
- `bucket::String`: Name of AWS bucket to store query results
- `query::String`: SQL query for SCEDC-pds database.

# Returns
Array of filepaths matching query in scedc-pds bucket.

# Example queries
1. Query all "HH[E,N,Z]" channels
query = "seedchan LIKE 'HH%'"

2. Query data from time range
query = "year_doy > '2016_150' and year_doy<'2016_157'"

3. Query lat/lon boundaries
query =  "lat between 34.00 and 34.50 and lon between -117 and -116"

4. Compound queries
query = "year = 2020 and seedchan LIKE 'BH%'"

"""
function athenaquery(aws::AWSConfig,bucket::String, query::String;
                    database::String="scedcindex",
                    table::String="scedc_parquet",
                    clean::Bool=true)

    # make sure a bucket exists for queries
    if bucket ∉ s3_list_buckets(aws)
        s3_create_bucket(aws,bucket)
    end

    # query the table
    output = AWSSDK.Athena.start_query_execution(aws,
        QueryString="select ms_filename from $table where $query order by ms_filename ",
        ResultConfiguration=["OutputLocation" => "s3://$bucket/queries/"],
        ClientRequestToken=randstring(32),
        QueryExecutionContext = ["Database" => database])

    # check when query is finished
    finished = false
    while !finished
         queryresult = AWSSDK.Athena.get_query_execution(aws,output)
         if queryresult["QueryExecutionDetail"]["Status"]["State"] == "SUCCEEDED"
             finished = true
             println("QUERY '$query' finished ",now())
         else
             sleep(1)
         end
     end

    # read query results
    querypath = "queries/"*output["QueryExecutionId"] * ".csv"
    tmpfile = tempname()
    s3_get_file(aws,bucket,querypath,tmpfile)
    filelist = readdlm(tmpfile,String)
    filelist = filelist[2:end,1]
    if length(filelist) != 0
        println("$(length(filelist)) matching QUERIES ",now())
    else
        println("No matching QUERIES ",now())
    end

    if clean
        s3_delete(aws,bucket,querypath)
        s3_delete(aws,bucket,querypath*".metadata")
    end
    return scedcpath.(filelist)
end
