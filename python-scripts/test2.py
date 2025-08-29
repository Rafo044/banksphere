import polars as ps
import io
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
import os

load_dotenv()
ENDPOINT = os.getenv("ENDPOINT")
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
extensions = (".csv", ".parquet", ".xlsx")

current_dir = os.getcwd()
parent_dir = os.path.abspath(os.path.join(current_dir, ".."))
data_dir = os.path.join(parent_dir, "data")


files_dict = {}


for files in os.listdir(data_dir):
    if files.endswith(extensions):
        object_name = os.path.splitext(files)[0]  
        full_path = os.path.join(parent_dir, files)
        files_dict[object_name] = full_path



bucket_name = "bronz"


def main(bucket_name,object_name,full_path):
    client = Minio(
        endpoint = ENDPOINT,
        access_key = ACCESS_KEY,
        secret_key = SECRET_KEY,
        secure = False
    )
    if full_path.endswith(".csv"):
        file = ps.read_csv(full_path)
    elif full_path.endswith(".parquet"):
        file = ps.read_parquet(full_path)
    elif full_path.endswith(".xlsx"):
        file = ps.read_excel(full_path)


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
    for object_name, full_path in files_dict.items():
        try:
            main(bucket_name, object_name, full_path)
        except S3Error as exc:
            print(f"Error uploading {object_name}: {exc}")

