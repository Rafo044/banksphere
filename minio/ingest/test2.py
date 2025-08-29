import polars as ps
import io
from minio import Minio
from minio.error import S3Error


accounts = ps.read_csv("/home/rafael/Documents/banksphere/minio/data/Accounts.csv")
bucket_name = "bronz"
object_name = "accounts"

def main():
    client = Minio(
        endpoint = "localhost:9000",
        access_key = "minioadmin",
        secret_key = "minioadmin",
        secure = False
    )

    buffer = io.BytesIO()
    accounts.write_parquet(buffer)
    buffer.seek(0)

    
    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)
        print("Created bucket", bucket_name)
    else:
        print("Bucket", bucket_name, "already exists")

   
    client.put_object(
        bucket_name,
        object_name + ".parquet",
        data=buffer,
        length=buffer.getbuffer().nbytes
    )
    print(
        object_name, "successfully uploaded as object",
        object_name, "to bucket", bucket_name,
    )

if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print("error occurred.", exc)
