import pymongo
import requests
import boto3
import os
from urllib.parse import urlparse
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv
from PIL import Image
from io import BytesIO

# .env 파일에서 환경변수 로드
load_dotenv()

# 환경변수에서 값 로드
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
DOCUMENT_FIELD = 'outsideimgurl'

# 다운로드 및 로그 디렉토리 설정
project_dir = os.path.dirname(os.path.abspath(__file__))
downloads_dir = os.path.join(project_dir, 'downloads')
logs_dir = os.path.join(project_dir, 'log')

# 디렉토리가 없는 경우 생성
os.makedirs(downloads_dir, exist_ok=True)
os.makedirs(logs_dir, exist_ok=True)

# 카운트 및 로그 파일 경로 설정
missing_field_count = 0
download_failed_count = 0
no_extension_count = 0
upload_success_count = 0
image_processing_failed_count = 0

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

def download_image(url):
    """이미지 다운로드"""
    try:
        # 브라우저와 유사하도록 헤더설정
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        }
        response = requests.get(url, headers=headers,timeout=5, verify=False)
        if response.status_code == 200:
            return BytesIO(response.content), None
        else:
            return None, f"Failed to download image from {url} with status code {response.status_code}"
    except Exception as e:
        return None, f"Error downloading image from {url}: {e}"

def process_image(image_stream, max_size_mb=1):
    """이미지 변환 및 리사이징"""
    try:
        with Image.open(image_stream) as img:
            # 변환할 이미지 포맷 설정
            img_format = 'WEBP'
            img_stream = BytesIO()

            # 이미지 저장 및 품질 조정
            img.save(img_stream, format=img_format, quality=85)  # quality=85는 기본 품질, 필요에 따라 조정 가능
            img_stream.seek(0)

            # 1MB 이하로 조정
            while img_stream.getbuffer().nbytes > max_size_mb * 1024 * 1024:
                img_stream.seek(0)
                img = Image.open(img_stream)
                width, height = img.size
                img = img.resize((width // 2, height // 2), Image.LANCZOS)
                img_stream = BytesIO()
                img.save(img_stream, format=img_format, quality=85)

            img_stream.seek(0)
            return img_stream, None
    except Exception as e:
        return None, f"Error processing image: {e}"

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
    global missing_field_count, download_failed_count, no_extension_count, upload_success_count, image_processing_failed_count

    # 로그 디렉토리 초기화
    clear_directory(logs_dir)
    clear_directory(downloads_dir)

    # MongoDB에서 이미지 URL과 _id 가져오기
    doc_fields = list(collection.find({}, {"_id": 1, DOCUMENT_FIELD: 1, "name": 1}))
    total_docs = len(doc_fields)

    missing_field_path = os.path.join(logs_dir, f'no_{DOCUMENT_FIELD}.txt')
    download_failed_file_path = os.path.join(logs_dir, 'download_failed.txt')
    no_extension_file_path = os.path.join(logs_dir, 'no_extension.txt')
    image_processing_failed_file_path = os.path.join(logs_dir, 'image_processing_failed.txt')
    summary_file_path = os.path.join(logs_dir, 'summary.txt')

    with open(missing_field_path, 'w') as missing_field_file, \
            open(download_failed_file_path, 'w') as download_failed_file, \
            open(no_extension_file_path, 'w') as no_extension_file, \
            open(image_processing_failed_file_path, 'w') as image_processing_failed_file, \
            open(summary_file_path, 'w') as summary_file:

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
                extension = '.jpg'
                no_extension_file.write(f"_id: {document['_id']}, name: {name}\n")
                print(f"Defaulting extension to .jpg for URL: {url}")
                no_extension_count += 1
            elif extension not in ['.jpg', '.png']:
                # 기본 확장자 설정
                extension = '.jpg'
                no_extension_file.write(f"_id: {document['_id']}, name: {name}\n")
                print(f"Extension {extension} is not .jpg or .png, defaulting to .jpg for URL: {url}")
                no_extension_count += 1

            # 로컬 파일 경로 설정
            local_filename = f"{object_id}.webp"
            local_path = os.path.join(downloads_dir, local_filename)

            # 이미지 다운로드 및 처리
            image_stream, error_message = download_image(url)
            if not image_stream:
                download_failed_file.write(f"_id: {document['_id']}, name: {name}, error: {error_message}\n")
                print(f"Failed to download image for document with _id {document['_id']} and name {name}. Error: {error_message}")
                download_failed_count += 1
                continue

            processed_image_stream, error_message = process_image(image_stream)
            if not processed_image_stream:
                image_processing_failed_file.write(f"_id: {document['_id']}, name: {name}, error: {error_message}\n")
                print(f"Failed to process image for document with _id {document['_id']} and name {name}. Error: {error_message}")
                image_processing_failed_count += 1
                continue

            # 로컬 파일 저장
            with open(local_path, 'wb') as f:
                f.write(processed_image_stream.getvalue())

            # S3 경로 설정
            s3_path = f"{s3_directory}/{local_filename}"

            # S3에 업로드
            if upload_to_s3(local_path, bucket_name, s3_path):
                upload_success_count += 1
                # 로컬 파일 삭제
                os.remove(local_path)

        # 총 카운트 기록
        missing_field_file.write(f"\nTotal documents missing '{DOCUMENT_FIELD}' field: {missing_field_count}\n")
        download_failed_file.write(f"\nTotal documents failed to download: {download_failed_count}\n")
        no_extension_file.write(f"\nTotal documents with no or invalid file extension: {no_extension_count}\n")
        image_processing_failed_file.write(f"\nTotal documents failed image processing: {image_processing_failed_count}\n")

        # Summary 기록
        summary_file.write(f"Total documents checked: {total_docs}\n")
        summary_file.write(f"Total documents missing '{DOCUMENT_FIELD}' field: {missing_field_count}\n")
        summary_file.write(f"Total documents failed to download: {download_failed_count}\n")
        summary_file.write(f"Total documents with no or invalid file extension: {no_extension_count}\n")
        summary_file.write(f"Total documents failed image processing: {image_processing_failed_count}\n")
        summary_file.write(f"Total documents successfully uploaded to S3: {upload_success_count}\n")

        print(f"Total documents checked: {total_docs}")
        print(f"Total documents missing '{DOCUMENT_FIELD}' field: {missing_field_count}")
        print(f"Total documents failed to download: {download_failed_count}")
        print(f"Total documents with no or invalid file extension: {no_extension_count}")
        print(f"Total documents failed image processing: {image_processing_failed_count}")
        print(f"Total documents successfully uploaded to S3: {upload_success_count}")

if __name__ == "__main__":
    main()
