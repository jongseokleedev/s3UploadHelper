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

# 이미지 URL 필드명 설정
DOCUMENT_FIELD = 'imgurl'

missing_field_count = 0
download_failed_count = 0


def download_image(url, local_path):
    try:
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(local_path, 'wb') as file:
                for chunk in response.iter_content(1024):
                    file.write(chunk)
            return True, None
        else:
            return False, f"Failed to download image from {url} with status code {response.status_code}"
    except Exception as e:
        return False, f"Error downloading image from {url}: {e}"


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
    global missing_field_count, download_failed_count
    # MongoDB에서 이미지 URL과 _id 가져오기
    img_docs = collection.find({}, {"_id": 1, DOCUMENT_FIELD: 1, "name": 1})

    no_imgurl_file_name = f'no_{DOCUMENT_FIELD}.txt'
    download_failed_file_name = 'download_failed.txt'

    with open(no_imgurl_file_name, 'w') as no_imgurl_file, open(download_failed_file_name, 'w') as download_failed_file:
        for document in img_docs:
            if DOCUMENT_FIELD not in document:
                no_imgurl_file.write(f"_id: {document['_id']}, name: {document.get('name', 'N/A')}\n")
                print(f"Document with _id {document['_id']} is missing '{DOCUMENT_FIELD}' field.")
                missing_field_count += 1
                continue

            img_url = document[DOCUMENT_FIELD]
            object_id = str(document["_id"])
            name = document.get("name", "N/A")

            # 로컬 파일 경로 설정 (사용자 홈 디렉토리의 'downloads' 폴더에 저장)
            local_filename = object_id
            local_path = os.path.join(os.path.expanduser('~'), 'downloads', local_filename)

            # 이미지 다운로드
            success, error_message = download_image(img_url, local_path)
            if not success:
                download_failed_file.write(f"_id: {document['_id']}, name: {name}, error: {error_message}\n")
                print(
                    f"Failed to download image for document with _id {document['_id']} and name {name}. Error: {error_message}")
                download_failed_count += 1
                continue

            # S3 경로 설정
            s3_path = f"{s3_directory}/{local_filename}"

            # S3에 업로드
            if upload_to_s3(local_path, bucket_name, s3_path):
                # 로컬 파일 삭제
                os.remove(local_path)

        # 총 카운트 기록
        no_imgurl_file.write(f"\nTotal documents missing '{DOCUMENT_FIELD}' field: {missing_field_count}\n")
        download_failed_file.write(f"\nTotal documents failed to download: {download_failed_count}\n")

    print(f"Total documents missing '{DOCUMENT_FIELD}' field: {missing_field_count}")
    print(f"Total documents failed to download: {download_failed_count}")


if __name__ == "__main__":
    main()
