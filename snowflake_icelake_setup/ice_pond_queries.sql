CREATE SCHEMA external_storage_integrations;

CREATE STORAGE INTEGRATION ice_swamp_reader
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::{account}:role/{role}'
  STORAGE_ALLOWED_LOCATIONS = ('*')
  STORAGE_AWS_EXTERNAL_ID = '0000'


CREATE CATALOG INTEGRATION GLUE_CATALOG_READER
  CATALOG_SOURCE = GLUE
  CATALOG_NAMESPACE = 'raw_data'
  TABLE_FORMAT = ICEBERG
  GLUE_AWS_ROLE_ARN = 'arn:aws:iam::{account}:role/{role}'
  GLUE_CATALOG_ID = '{catalog_id}'
  GLUE_REGION = 'us-east-1'
  ENABLED = TRUE;

CREATE OR REPLACE EXTERNAL VOLUME ice_swamp_volume
   STORAGE_LOCATIONS =
      (
         (
            NAME = '{bucket}'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://bucket/'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::{account}:role/{role}'
            STORAGE_AWS_EXTERNAL_ID = '{EXTERNAL_ID}'
         )
      );

SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('ICE_SWAMP_VOLUME');

CREATE ICEBERG TABLE src_ice_sessions
  EXTERNAL_VOLUME='ICE_SWAMP_VOLUME'
  CATALOG='GLUE_CATALOG_READER'
  CATALOG_TABLE_NAME='sessions';

CREATE ICEBERG TABLE src_ice_sessions_fct
  EXTERNAL_VOLUME='ICE_SWAMP_VOLUME'
  CATALOG='GLUE_CATALOG_READER'
  CATALOG_TABLE_NAME='sessions';

CREATE ICEBERG TABLE src_ice_pageviews_fct
  EXTERNAL_VOLUME='ICE_SWAMP_VOLUME'
  CATALOG='GLUE_CATALOG_READER'
  CATALOG_TABLE_NAME='pageviews';

CREATE ICEBERG TABLE stg_ice_users_dim
  EXTERNAL_VOLUME='ICE_SWAMP_VOLUME'
  CATALOG='GLUE_CATALOG_READER'
  CATALOG_TABLE_NAME='stg_ice_users_dim';