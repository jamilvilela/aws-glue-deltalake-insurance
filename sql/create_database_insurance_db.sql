CREATE DATABASE IF NOT EXISTS insurance_db
COMMENT 'Insurance database product on AWS Athena'
LOCATION 's3://jamil-datalake-dev/glue-catalog/insurance_db/'
WITH DBPROPERTIES ('owner'='lake-admin', 'description'='Data Lake admin');