import sys
from pyspark.context import SparkContext
from pyspark.sql import functions as sqlFunc
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from datetime import date, timedelta, datetime

# %%

def read_source_data(glue_context, source_db, source_table):
    source_df = (glue_context
                        .create_data_frame
                        .from_catalog(database   = source_db, 
                                      table_name = source_table)
                        )
    
    return DynamicFrame.fromDF(source_df, 
                                glue_context, 
                                "source_dyf")

def spark_aggregate(glue_context, parent_frame, groups, aggs, transformation_ctx):
    aggs_funcs          = [getattr(sqlFunc, func)(column) for column, func in aggs]
    aggs_col_func_names = [func + '_' + column for column, func in aggs]

    result_df = (
            parent_frame.toDF()
                        .groupBy(*groups)
                        .agg(*aggs_funcs)
            if len(groups) > 0
            else parent_frame.toDF()
                            .agg(*aggs_funcs)
    )

    for c in range(len(aggs_col_func_names)):
        result_df = result_df.withColumnRenamed(result_df.columns[c + len(groups)], aggs_col_func_names[c])
    
    return DynamicFrame.fromDF(result_df, 
                                glue_context, 
                                transformation_ctx)

def apply_mappings(frame):
    return ApplyMapping.apply(
                frame=frame,
                mappings=[
                    ("policy_id",       "long",   "policy_id",      "long"),
                    ("expiry_date",     "date",   "expiry_date",    "date"),
                    ("location_name",   "string", "location_name",  "string"),
                    ("state_code",      "string", "state_code",     "string"),
                    ("region_name",     "string", "region_name",    "string"),
                    ("insured_value",   "double", "insured_value",  "double"),
                    ("business_type",   "string", "business_type",  "string"),
                    ("flood",           "string", "flood",          "string"),
                    ("file_name",       "string", "file_name",      "string"),
                    ("year_month_day",  "string", "year_month_day", "string"),
                    ("year",            "string", "year",           "string"),
                    ("month",           "string", "month",          "string"),
                    ("day",             "string", "day",            "string"),
                ]
            )

def write_to_table(glue_context, target_dyf, target_db, target_table, target_path, partitions):
    try:
        s3sink = glue_context.getSink(
                    connection_type="s3",
                    path=target_path,
                    partitionKeys=partitions,
                    compression="snappy",
                    enableUpdateCatalog=True,
                    updateBehavior="UPDATE_IN_DATABASE"
        )

        s3sink.setCatalogInfo(
                    catalogDatabase =target_db,
                    catalogTableName=target_table
        )

        s3sink.setFormat("glueparquet", useGlueParquetWriter=True)

        s3sink.writeFrame(target_dyf)
    except Exception as ex:
        ValueError (f'**** Error saving into database. \\n{ex}')


# %%

def run_etl(glue_context, spark, start_date, days_ago, bucket_name, catalog, source_db, target_db, source_table, target_table, partitions):
    
    target_path = f"s3://{bucket_name}/{catalog}/{target_db}/{target_table}/"
    
    if start_date == 'cron' or not start_date:
        start_date = (date.today() - timedelta(days=days_ago)).strftime('%Y%m%d')
    
    part_year  = start_date[:4]
    part_month = start_date[4:6]
    part_day   = start_date[6:]
    
    print(part_day, part_month, part_year)
    
    target_exists = target_table in [tb.name for tb in spark.catalog.listTables(dbName=target_db)]

    source_dyf = read_source_data(glue_context, source_db, source_table)

    if target_exists:
        src_dyf = source_dyf.filter(
                                f=lambda x:     x[partitions[0]] == part_year
                                            and x[partitions[1]] == part_month
                                            and x[partitions[2]] == part_day
                            )
    else:
        ymd_dyf = spark_aggregate(
                            glue_context = glue_context,
                            parent_frame = source_dyf,
                            groups       = [],
                            aggs         = [['year_month_day', 'first']],
                            transformation_ctx='ymd_dyf'
        )
        
        print(ymd_dyf.toDF().show())
        
        src_dyf = source_dyf.join(
                                frame2=ymd_dyf,
                                paths1=['year_month_day'],
                                paths2=['first_year_month_day']
        )

    src_dyf = apply_mappings(src_dyf)

    src_df = src_dyf.toDF()
    qtty_src = src_df.count()

    if not target_exists:
        target_df = src_dyf.toDF()
        qtty_before = target_df.count()
    else:
        target_df = (glue_context
                            .create_dynamic_frame
                            .from_catalog(database   = target_db,
                                          table_name = target_table
                            )
                    ).toDF()
        qtty_before = target_df.count()

        # filter for deletions
        not_match_df = (target_df.alias('target')
                                .join(  src_df.alias('src'),
                                        sqlFunc.col("target.policy_id") == sqlFunc.col("src.policy_id"),
                                        'left')
                                .where('src.policy_id is null') #or target.expiry_date > src.expiry_date')
                                .select('target.*')
        )

        target_df = not_match_df.union(src_df)
        
    target_dyf = DynamicFrame.fromDF(target_df, glue_context, 'target_dyf')
    qtty_after = target_df.count()

    
    if target_exists:
        glue_context.purge_table(
                                target_db,
                                target_table,
                                {"retentionPeriod": 0,
                                "manifestFilePath": target_path + "manifest/"},
                                "target_dyf"
        )
    
    write_to_table(glue_context,
                   target_dyf, 
                   target_db, 
                   target_table, 
                   target_path, 
                   partitions
    )

    print('Start date  :', start_date)
    print('Source db   :', source_db)
    print('Qtty before :', qtty_before)
    print('Qtty source :', qtty_src)
    print('Qtty after  :', qtty_after)

# %%

args = getResolvedOptions(sys.argv, ["JOB_NAME", "start_date"])
t1 = datetime.now()
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# %%
try:
        run_etl(glue_context=glueContext,
                spark=spark,
                start_date=args['start_date'], 
                days_ago=1, 
                bucket_name='jamil-datalake-dev', 
                catalog="glue-catalog",
                source_db='insurance_db', 
                target_db="insurance_prd", 
                source_table="policy", 
                target_table="policy_prod",
                partitions=["year", "month", "day"]
        )
except Exception as ex:
        print('Erro: ', ex)
        
print('Elapsed time: ', datetime.now() - t1)

job.commit()
