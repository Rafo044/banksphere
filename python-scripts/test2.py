import polars as ps
import io
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
import os

load_dotenv()


current_dir = os.getcwd()

parent_dir = os.path.abspath(os.path.join(current_dir, ".."))

data_dir = os.path.join(parent_dir, "data/")

extensions = (".csv", ".parquet", ".xlsx")

files_dict = {}

for files in os.listdir(data_dir):
    if files.endswith(extensions):
        object_name = os.path.splitext(files)[0]  
        full_path = os.path.join(parent_dir, files)
        files_dict[object_name] = full_path


file = ps.read_csv(full_path)
bucket_name = "bronz"


def main(bucket_name,object_name,full_path):
    client = Minio(
        endpoint = ENDPOINT,
        access_key = ACCESS_KEY,
        secret_key = SECRET_KEY,
        secure = False
    )
    file = ps.read_csv(full_path)

    buffer = io.BytesIO()
    file.write_parquet(buffer)
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
        for object_name,full_path in files_dict.items():
            main(bucket_name,object_name,full_path)
    except S3Error as exc:
        print("error occurred.", exc)
