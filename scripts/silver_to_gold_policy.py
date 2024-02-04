import sys
from pyspark.context import SparkContext
from pyspark.sql import functions as sqlFunc
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from datetime import date, timedelta


class SilverToGoldPolicy:
    def __init__(self, args):
        self.glue_context = self.create_glue_context()
        self.spark = self.glue_context.spark_session
        self.job = self.create_glue_job(self.glue_context, args["JOB_NAME"], args)

    @staticmethod
    def create_glue_context():
        return GlueContext(SparkContext.getOrCreate())

    @staticmethod
    def create_glue_job(glue_context, job_name, args):
        job = Job(glue_context)
        job.init(job_name, args)
        
        return job

    def read_source_data(self, source_db, source_table):
        source_df = (self.glue_context
                            .create_data_frame
                            .from_catalog(database  =source_db, 
                                          table_name=source_table)
                            )
        
        return DynamicFrame.fromDF(source_df, 
                                   self.glue_context, 
                                   "source_dyf")

    def spark_aggregate(self, parent_frame, groups, aggs, transformation_ctx):
        aggs_funcs = [getattr(sqlFunc, func)(column) for column, func in aggs]
        
        result_df = (
                parent_frame.toDF()
                            .groupBy(*groups)
                            .agg(*aggs_funcs)
                if len(groups) > 0
                else parent_frame.toDF()
                                .agg(*aggs_funcs)
        )
        
        cols = result_df.columns
        aggs_col_names = [func + '_' + column for column, func in aggs]

        for c in range(len(aggs_col_names)):
            result_df = result_df.withColumnRenamed(cols[c], aggs_col_names[c])
            
        return DynamicFrame.fromDF(result_df, 
                                   self.glue_context, 
                                   transformation_ctx)

    @staticmethod
    def apply_mappings(frame):
        return ApplyMapping.apply(
                    frame=frame,
                    mappings=[
                        ("policy_id", "long", "policy_id", "long"),
                        ("expiry_date", "date", "expiry_date", "date"),
                        ("location_name", "string", "location_name", "string"),
                        ("state_code", "string", "state_code", "string"),
                        ("region_name", "string", "region_name", "string"),
                        ("insured_value", "double", "insured_value", "double"),
                        ("business_type", "string", "business_type", "string"),
                        ("flood", "string", "flood", "string"),
                        ("file_name", "string", "file_name", "string"),
                        ("year_month_day", "string", "year_month_day", "string"),
                        ("year", "string", "year", "string"),
                        ("month", "string", "month", "string"),
                        ("day", "string", "day", "string"),
                    ]
                )

    def write_to_table(self, target_dyf, target_db, target_table, target_path, partitions):
        additional_options = {
            "enableUpdateCatalog": True,
            "updateBehavior": "UPDATE_IN_DATABASE"
        }

        s3sink = self.glue_context.getSink(
                        connection_type="s3",
                        path=target_path,
                        partitionKeys=partitions,
                        compression="snappy",
                        enableUpdateCatalog=True,
                        updateBehavior="UPDATE_IN_DATABASE"
        )

        s3sink.setCatalogInfo(
                        catalogDatabase=target_db,
                        catalogTableName=target_table
        )

        s3sink.setFormat("glueparquet", useGlueParquetWriter=True)

        s3sink.writeFrame(target_dyf)
        

    def run_etl(self, start_date, days_ago, bucket_name, catalog, source_db, target_db, source_table, target_table, partitions):
        
        target_path = f"s3://{bucket_name}/{catalog}/{target_db}/{target_table}/"
        
        if start_date == 'cron' or not start_date:
            start_date = (date.today() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
        
        part_year = start_date[:4]
        part_month = start_date[5:7]
        part_day = start_date[8:]

        target_exists = target_table in [tb.name for tb in self.spark.catalog.listTables(dbName=target_db)]

        source_dyf = self.read_source_data(source_db, source_table)

        if target_exists:
            src_dyf = source_dyf.filter(
                f=lambda x:     x[partitions[0]] == part_year
                            and x[partitions[1]] == part_month
                            and x[partitions[2]] == part_day
            )
        else:
            ymd_dyf = self.spark_aggregate(
                                parent_frame=source_dyf,
                                groups=['year', 'month', 'day'],
                                aggs=[['year_month_day', 'first']],
                                transformation_ctx='ymd_dyf'
            )

            src_dyf = source_dyf.join(frame2=ymd_dyf,
                                      paths1=['year', 'month', 'day'],
                                      paths2=['year', 'month', 'day']
                                      )

        src_dyf = self.apply_mappings(src_dyf)

        src_df = src_dyf.toDF()

        if not target_exists:
            target_df = src_df
        else:
            target_df = (self.glue_context
                                .create_dynamic_frame
                                .from_catalog(database=target_db,
                                              table_name=target_table
                              )
            ).toDF()

            # filter for deletions
            not_match_df = (target_df.alias('target')
                                     .join(src_df.alias('src'),
                                           sqlFunc.col("target.policy_id") == sqlFunc.col("src.policy_id"),
                                            'left')
                                     .where('src.policy_id is null or target.expiry_date > src.expiry_date')
                                     .select('target.*')
            )

            target_df = not_match_df.union(src_df)

        target_dyf = DynamicFrame.fromDF(target_df, self.glue_context, 'target_dyf')

        # delete existing data
        if target_dyf.toDF().count() > 0:
            self.glue_context.purge_table(target_db,
                                          target_table,
                                          {"retentionPeriod": 0,
                                           "manifestFilePath": target_path + "manifest/"},
                                          'target_dyf'
                                          )

        self.write_to_table(target_dyf, 
                            target_db, 
                            target_table, 
                            target_path, 
                            partitions)

        self.job.commit()


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "start_date"])
    etl = SilverToGoldPolicy(args)
    etl.run_etl(start_date=args['start_date'], 
                days_ago=1, 
                bucket_name='jamil-datalake-dev', 
                catalog="glue-catalog",
                source_db='insurance_db', 
                target_db="insurance_prd", 
                source_table="policy", 
                target_table="policy_prod",
                partitions=["year", "month", "day"])


if __name__ == "__main__":
    main()
