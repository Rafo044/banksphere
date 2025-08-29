import boto3
import polars as ps
import io

accounts = ps.read_csv("/home/rafael/Documents/banksphere/minio/data/Accounts.csv")
bucket_name = "bronz"
object_name = "accounts"


minio = boto3.resource(    "s3",
     endpoint_url="https://localhost:9000",
     aws_access_key_id="minioadmin",
     aws_secret_accesss_key="minioadmin"
)

if not minio.Bucket(bucket_name) in minio.buckets.all():
    minio.create_bucket(Bucket = bucket_name)

buffer = io.BytesIO()
accounts.write_parquet(buffer)
buffer.seek(0)


minio.put_object(Bucket=bucket_name, Key=object_name, Body=buffer.getvalue())