import pymongo
import requests
import boto3
import os
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv

# .env 파일에서 환경변수 로드
load_dotenv()

# 환경변수에서 설정 가져오기
mongo_uri = os.getenv('MONGO_URI')
mongo_db_name = os.getenv('MONGO_DB_NAME')
mongo_collection_name = os.getenv('MONGO_COLLECTION_NAME')

aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_REGION')

bucket_name = os.getenv('S3_BUCKET_NAME')
s3_directory = os.getenv('S3_DIRECTORY_NAME')

# MongoDB 연결 설정
mongo_client = pymongo.MongoClient(mongo_uri)
db = mongo_client[mongo_db_name]
collection = db[mongo_collection_name]

# AWS S3 연결 설정
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region
)


def download_image(url, local_path):
    try:
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(local_path, 'wb') as file:
                for chunk in response.iter_content(1024):
                    file.write(chunk)
            return True
        else:
            print(f"Failed to download image from {url}")
            return False
    except Exception as e:
        print(f"Error downloading image from {url}: {e}")
        return False


def upload_to_s3(local_path, bucket_name, s3_path):
    try:
        s3_client.upload_file(local_path, bucket_name, s3_path)
        print(f"Successfully uploaded {s3_path} to {bucket_name}")
        return True
    except FileNotFoundError:
        print(f"The file was not found: {local_path}")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False


def main():
    # MongoDB에서 이미지 URL과 _id 가져오기
    img_docs = collection.find({}, {"_id": 1, "img_url": 1})

    for document in img_docs:
        img_url = document["img_url"]
        object_id = str(document["_id"])

        # 로컬 파일 경로 설정
        local_filename = object_id
        local_path = os.path.join("/tmp", local_filename)

        # 이미지 다운로드
        if download_image(img_url, local_path):
            # S3 경로 설정
            s3_path = f"{s3_directory}/{local_filename}"

            # S3에 업로드
            if upload_to_s3(local_path, bucket_name, s3_path):
                # 로컬 파일 삭제
                os.remove(local_path)


if __name__ == "__main__":
    main()
