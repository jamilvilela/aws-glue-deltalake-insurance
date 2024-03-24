import sys
import boto3
import pyspark.sql.functions as F
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from delta import DeltaTable
from datetime import date, datetime, timedelta
from dateutil.parser import parse
from re import sub




# %%

def to_snake_case(text):
    return ('_'.join(sub('([A-Z][a-z]+)', r' \1', 
                        sub('([A-Z]+)', r' \1', 
                            text.replace('-', ' ')))
                    .split())
                .lower())


def dedup_keys_str(primary_keys: list) -> str:
    condition_list = [f'target.{key} = delta.{key}' for key in primary_keys]
    dedup_str = ' AND '.join(condition_list)
    return dedup_str


def table_exists(spark, database, table) -> bool:
    exist = spark.sql(f"select * from {database}.{table} limit 1")
    return bool(exist.count() > 0)


def delta_table_exists(spark, path) -> bool:
    exist = False
    try:
        DeltaTable.forPath(spark, path)
        exist = True
    except:
        exist = False
    return exist


def is_valid_date(date_ymd) -> bool:
    is_valid = False
    if date_ymd:
        try:
            parse(timestr=date_ymd, yearfirst=True, dayfirst=True)
            is_valid = True
        except:
            is_valid = False
    return is_valid


def get_s3_bucket_objects(bucket_name, prefix, start_date=None, final_date=None, file_format='csv') -> list:
    s3 = boto3.client('s3')
    file_list = s3.list_objects_v2(Bucket=bucket_name.removeprefix("s3://").removesuffix('/'),
                                    Prefix=prefix.removeprefix(bucket_name))

    if 'Contents' in file_list:
        content = file_list['Contents']
    else:
        return []

    if 'Key' in content[0]:
        if start_date is None:
            start_date = '2001-01-01'

        if final_date is None:
            final_date = '2099-12-31'

        start_date = date.fromisoformat(start_date).strftime('%Y%m%d')
        final_date = date.fromisoformat(final_date).strftime('%Y%m%d')

        obj_list = []
        for obj in content:
            obj = obj['Key']

            if obj.endswith(f".{file_format}"):

                if start_date <= obj[-12:-4] <= final_date:
                    obj_list.append(bucket_name + obj)

        obj_list.sort()

    else:
        return []

    return obj_list


def s3_bucket_exists(bucket_name) -> bool:
    s3 = boto3.client('s3')
    obj_list = [obj['Name'] for obj in s3.list_buckets()['Buckets']]
    return bucket_name in obj_list


def read_source(spark, path, schema):
    source_df = (spark.read
                    .format("csv")
                    .schema(schema)
                    .option("header", "true")
                    .load(path)
                    )
    return source_df


def transform(glueContext, data_frame):
    dyf = DynamicFrame.fromDF(data_frame, 
                                glueContext, 
                                "dyf")

    mappings = [('operation', 'string', 'operation', 'char(1)'),
                ('policy_id', 'string', 'policy_id', 'bigint'),
                ('expiry_date', 'string', 'expiry_date', 'date'),
                ('location_name', 'string', 'location_name', 'string'),
                ('state_code', 'string', 'state_code', 'string'),
                ('region_name', 'string', 'region_name', 'string'),
                ('insured_value', 'string', 'insured_value', 'double'),
                ('business_type', 'string', 'business_type', 'string'),
                ('earthquake', 'string', 'earth_quake', 'char(1)'),
                ('flood', 'string', 'flood', 'char(1)')]
    dyf = dyf.apply_mapping(mappings)
    data_frame = dyf.toDF()

    target_df = (data_frame
                        .withColumn('file_name',       F.input_file_name())
                        .withColumn('year_month_day',  F.expr("substring(file_name, length(file_name) -11, 8)"))
                        .withColumn('year',            F.expr("substring(year_month_day, 1, 4)"))
                        .withColumn('month',           F.expr("substring(year_month_day, 5, 2)"))
                        .withColumn('day',             F.expr("substring(year_month_day, 7, 2)"))
                        .drop('operation')
                    )
    target_df = target_df.orderBy(F.asc('policy_id'), F.desc('year_month_day'))
    target_df = target_df.dropDuplicates(['policy_id'])
    return target_df


def historical_load(target_df, path):
    try:
        (target_df.write
                    .format('delta')
                    .mode('overwrite')
                    .partitionBy(['year', 'month', 'day'])
                    .option("overwriteSchema", "true")
                    .option("path", path)
                    .save()
            )
    except:
        raise ValueError(f"**** Error saving into the bucket {path}")


def delta_load(spark, delta_df, primary_keys, path):
    try:
        target_df = DeltaTable.forPath(spark, path)
    except:
        raise ValueError('**** Target S3 target folder has not found.')

    try:
        (target_df.alias('target')
            .merge(source=delta_df.alias('delta'),
                condition=F.expr(dedup_keys_str(primary_keys)))
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            ).execute()
    except Exception as ex:
        raise ValueError(f"**** Error upserting into bucket {path}. \\n {ex}")


def get_schema():
    schema_fields = [
        ('operation', 'string'),
        ('policy_id', 'string'),
        ('expiry_date', 'string'),
        ('location_name', 'string'),
        ('state_code', 'string'),
        ('region_name', 'string'),
        ('insured_value', 'string'),
        ('business_type', 'string'),
        ('earthquake', 'string'),
        ('flood', 'string')
    ]

    schema = StructType(
                [StructField(field_name, StringType(), True) 
                for field_name, _ in schema_fields]
    )

    return schema

def main(args, spark, glueContext) -> None:
    reprocess_all = (args['reprocess_all'] == 'True')
    environment   = args['environment']
    file_format   = args['file_format']
    s3_bucket     = args['s3_bucket']
    start_date    = args['start_date']
    final_date    = args['final_date']
    days_ago      = 1

    ingestion    = 'raw-data'
    catalog      = 'glue-catalog'
    database     = 'insurance_db'
    table_name   = 'policy'
    primary_keys = ['policy_id']

    prefix_full_load = ingestion + '/' + database + '/' + table_name + '/full-load/'
    prefix_cdc_load  = ingestion + '/' + database + '/' + table_name + '/cdc-load/'

    source_path_full = s3_bucket + prefix_full_load
    source_path_cdc  = s3_bucket + prefix_cdc_load

    target_path = s3_bucket + catalog + '/' + database + '/' + table_name + '/'
       
    if not s3_bucket_exists(s3_bucket.removeprefix("s3://").removesuffix('/')):
        raise ValueError('**** Bucket name is invalid.')

    if (start_date != 'cron' and not is_valid_date(start_date)) or (
            final_date != 'cron' and not is_valid_date(final_date)):
        raise ValueError('**** Start or final date is invalid.')

    if reprocess_all not in [True, False]:
        raise ValueError('**** The parameter reprocess_all must be boolean: (True or False).')

    if environment not in ['dev', 'prd']:
        raise ValueError('**** The parameter environment must be [dev or prd].')

    if reprocess_all:
        start_date = '2001-01-01'
        final_date = date.today().strftime('%Y-%m-%d')

        # TODO: to backup delta table
        # to delete delta delta table
    else:
        if start_date == 'cron':
            start_date = (date.today() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
        if final_date == 'cron':
            final_date = start_date
            
    if delta_table_exists(spark, target_path):
        source_path = source_path_cdc
        qtty_before = spark.read.format('delta').load(target_path).count()
    else:
        source_path = source_path_full
        qtty_before = 0
        
    file_list = get_s3_bucket_objects(bucket_name=s3_bucket,
                                        prefix=source_path,
                                        start_date=start_date,
                                        final_date=final_date,
                                        file_format=file_format
                                        )

    if not file_list:
        print('There is not files for loading.')
        qtty_src = 0
    else:
        src_df = read_source(spark,
                             file_list, 
                             get_schema()
                            )
        qtty_src = src_df.count()
        
        final_df = transform(glueContext, src_df)

        if delta_table_exists(spark, target_path):
            print(' >>> Delta loading')
            delta_load(spark, 
                       final_df, 
                       primary_keys, 
                       target_path
                    )
        else:
            print(' >>> Historiccal loading')
            historical_load(final_df, 
                            target_path
                            )

    delta_df = (spark.read
                    .format('delta')
                    .load(target_path)
                )
    qtty_after = delta_df.count()

    print('Start date  : ', start_date)
    print('Final date  : ', final_date)
    print('Source path :', source_path)
    print('Target path :', target_path)
    print('Qtty in DB  :', qtty_before)
    print('Qtty in file:', qtty_src)
    print('Qtty after  :', qtty_after)



# %%

args = getResolvedOptions(sys.argv, 
                              ['JOB_NAME', 
                              'reprocess_all', 
                              'file_format', 
                              'environment', 
                              's3_bucket',
                              'start_date', 
                              'final_date']
                              )

t1 = datetime.now()
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

try:
    main(args, spark, glueContext)
except Exception as ex:
    print('Error: ', ex)

print('Elapsed time: ', datetime.now() - t1)
job.commit()
