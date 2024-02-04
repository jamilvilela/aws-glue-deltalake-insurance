CREATE EXTERNAL TABLE insurance_db.policy
LOCATION 's3://jamil-datalake-dev/glue-catalog/insurance_db/policy/'
TBLPROPERTIES ('table_type' = 'DELTA')

