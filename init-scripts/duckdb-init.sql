

INSTALL httpfs ;
LOAD httpfs ;

SET s3_endpoint='minio:9000'

-- becouse default https and change them
SET s3_url_style='path'

SET s3_use_ssl=false

SET s3_access_key_id={{env_var('MINIO_ROOT_USER')}}

SET s3_secret_access_key={{env_var('MINIO_ROOT_PASSWORD')}}

