export athenasetup, scedcquery 
"""
  athenasetup(aws,bucket)

Create AWS Athena database and table for SQL queries.

# Arguments
`aws::AWSConfig`: AWSConfig configuration dictionary
`bucket::String`: Name of AWS bucket to store query results
"""
function athenasetup(aws::AWSConfig,bucket::String;
                    database::String="scedcindex",
                    table::String="scedc_parquet",
                    clean::Bool=true)

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
  scedcquery(aws,bucket,query)

Use AWS Athena to query SCEDC-pds database.

# Arguments
`aws::AWSConfig`: AWSConfig configuration dictionary
`bucket::String`: Name of AWS bucket to store query results
`query::String`: SQL query for SCEDC-pds database.

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
query = "seedchan LIKE 'BH%' and year_doy > '2016_150' and year_doy<'2016_157'"

"""
function scedcquery(aws::AWSConfig,bucket::String, query::String;
                    database::String="scedcindex",
                    table::String="scedc_parquet",
                    clean::Bool=true)

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

"""
  scedcpath(filename)

Convert filename to scedc-pds path.
"""
function scedcpath(filename::String)
    year = filename[14:17]
    day = filename[18:20]
    return "continuous_waveforms/" * year * '/' * year * '_' * day * '/' * filename
end
