import pymongo
import requests
import boto3
import os
from urllib.parse import urlparse
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

# 가져올 URL 필드명 설정
DOCUMENT_FIELD = 'outsideimgurl'

# 다운로드 및 로그 디렉토리 설정
project_dir = os.path.dirname(os.path.abspath(__file__))
downloads_dir = os.path.join(project_dir, 'downloads')
logs_dir = os.path.join(project_dir, 'log')

# 디렉토리가 없는 경우 생성
os.makedirs(downloads_dir, exist_ok=True)
os.makedirs(logs_dir, exist_ok=True)

# 카운트 설정
missing_field_count = 0
download_failed_count = 0
no_extension_count = 0
upload_success_count = 0


def clear_directory(dir):
    """디렉토리의 모든 파일을 삭제"""
    for filename in os.listdir(dir):
        file_path = os.path.join(dir, filename)
        if os.path.isfile(file_path):
            os.unlink(file_path)


def get_file_extension(url):
    """URL에서 파일 확장자를 추출"""
    parsed_url = urlparse(url)
    _, extension = os.path.splitext(parsed_url.path)
    return extension


def download_image(url, local_path):
    """이미지 다운로드"""
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
    """S3에 파일 업로드"""
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
    global missing_field_count, download_failed_count, no_extension_count, upload_success_count

    # 로그 디렉토리 초기화
    clear_directory(logs_dir)
    clear_directory(downloads_dir)

    # MongoDB에서 이미지 URL과 _id 가져오기
    doc_fields = collection.find({}, {"_id": 1, DOCUMENT_FIELD: 1, "name": 1})

    missing_field_path = os.path.join(logs_dir, f'no_{DOCUMENT_FIELD}.txt')
    download_failed_file_path = os.path.join(logs_dir, 'download_failed.txt')
    no_extension_file_path = os.path.join(logs_dir, 'no_extension.txt')
    default_extension = '.jpg'

    with open(missing_field_path, 'w') as missing_field_file, \
            open(download_failed_file_path, 'w') as download_failed_file, \
            open(no_extension_file_path, 'w') as no_extension_file:

        for document in doc_fields:
            if DOCUMENT_FIELD not in document:
                missing_field_file.write(f"_id: {document['_id']}, name: {document.get('name', 'N/A')}\n")
                print(f"Document with _id {document['_id']} is missing '{DOCUMENT_FIELD}' field.")
                missing_field_count += 1
                continue

            url = document[DOCUMENT_FIELD]
            object_id = str(document["_id"])
            name = document.get("name", "N/A")

            # URL에서 파일 확장자 추출
            extension = get_file_extension(url)
            if not extension:
                # 기본 확장자 설정
                extension = default_extension
                no_extension_file.write(f"_id: {document['_id']}, name: {name}\n")
                print(f"Defaulting extension to .jpg for URL: {url}")
                no_extension_count += 1
            # .do나 .html등의 링크로 받는 경우르 핸들링하기위해 이미지 확장자가 아닌 경우 Default 확장자로 설정
            elif extension not in ['.jpg', '.png', '.jpeg']:
                # 기본 확장자 설정
                extension = default_extension
                no_extension_file.write(f"_id: {document['_id']}, name: {name}\n")
                print(f"Extension {extension} is not .jpg or .png or .jpeg, defaulting to .jpg for URL: {url}")
                no_extension_count += 1

            # 로컬 파일 경로 설정
            local_filename = f"{object_id}{extension}"
            local_path = os.path.join(downloads_dir, local_filename)

            # 이미지 다운로드
            success, error_message = download_image(url, local_path)
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
                upload_success_count += 1
            # 로컬 파일 삭제
            # os.remove(local_path)

        # 총 카운트 기록
        missing_field_file.write(f"\nTotal documents missing '{DOCUMENT_FIELD}' field: {missing_field_count}\n")
        download_failed_file.write(f"\nTotal documents failed to download: {download_failed_count}\n")
        no_extension_file.write(f"\nTotal documents with no file extension: {no_extension_count}\n")

    print(f"Total documents missing '{DOCUMENT_FIELD}' field: {missing_field_count}")
    print(f"Total documents failed to download: {download_failed_count}")
    print(f"Total documents with no file extension: {no_extension_count}")
    print(f"Total documents successfully uploaded to S3: {upload_success_count}")


if __name__ == "__main__":
    main()
