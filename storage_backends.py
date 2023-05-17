# Standard Library
import logging
import os
import traceback
from io import BytesIO
from typing import Tuple, Union

# Django Library
from django.conf import settings
from django.core.files.storage import FileSystemStorage

# Third Party Library
import boto3
from botocore.exceptions import ClientError
from storages.backends.s3boto3 import S3Boto3Storage

# Own Library
from wakefit.celery import app
from wakefit.utils.celery import MyCeleryTask

# Local Library
from .utils.mail_helpers import send_error_mail, send_error_mail_adv

logger = logging.getLogger(__name__)


class StaticStorage(S3Boto3Storage):
    location = 'static'
    default_acl = 'public-read'


class PublicMediaStorage(S3Boto3Storage):
    location = ''  # attachments
    default_acl = 'public-read'
    file_overwrite = False


class PrivateMediaStorage(S3Boto3Storage):
    location = 'private'
    default_acl = 'private'
    file_overwrite = False
    custom_domain = False


def get_storage(storage_type):
    if settings.USE_S3:
        storage_class = {'private': PrivateMediaStorage, 'public': PublicMediaStorage}[storage_type]
        return storage_class()
    return FileSystemStorage()


@app.task(base=MyCeleryTask)
def upload_file_s3(
    file_name: str, file_name_s3: str, to_delete: bool = False, bucket: str = None, extra_args: dict = None
):
    """
    Upload a file to S3

    Args:
      file_name (str): The name of the file to upload.
      file_name_s3 (str): The name of the file in S3.
      to_delete (bool): If True, the file will be deleted after upload. Defaults to False
      bucket (str): The name of the bucket to upload to.
      extra_args (dict): A dictionary of parameters to pass to the client operation.

    Returns:
      The response is a dictionary with the following keys:
    """
    myf = '[upload_file_s3] '

    if not settings.USE_S3:
        return False
    # Upload the file
    if bucket is None:
        bucket = settings.AWS_STORAGE_BUCKET_NAME
    try:
        s3_client = boto3.client(
            's3', aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY
        )
    except Exception:
        send_error_mail_adv(
            f'{myf} boto3.client', f'Error File Name: {file_name_s3}', traceback.format_exc(), apply_async=True
        )
        return False
    try:
        response = s3_client.upload_file(file_name, bucket, file_name_s3, ExtraArgs=extra_args)
        logger.info(f's3 upload response is: {response}')
    except ClientError:
        send_error_mail_adv(
            f'{myf} upload_file ClientError',
            f'Error File Name: {file_name_s3}',
            traceback.format_exc(),
            apply_async=True,
        )
        return False
    except Exception:
        send_error_mail_adv(
            f'{myf} upload_file base exception',
            f'Error File Name: {file_name_s3}',
            traceback.format_exc(),
            apply_async=True,
        )
        return False
    if to_delete:
        os.remove(file_name)
    return True


def get_s3_file_url(file_name_s3: str, bucket: str = None) -> str:
    """
    It takes in a bucket name, a key name and an expiration time and returns a presigned URL

    Args:
      file_name_s3 (str): The name of the file in the bucket.
      bucket (str): The name of the bucket where the file is located.

    Returns:
      A presigned URL for the object.
    """
    if bucket is None:
        bucket = settings.AWS_STORAGE_BUCKET_NAME
    if file_name_s3 is None:
        return ''
    return f'https://{bucket}.s3.amazonaws.com/{file_name_s3}'


def get_presigned_url_from_boto3(bucket: str, key: str, expires_in=7200):
    """
    It takes in a bucket name, a key name and an expiration time and returns a presigned URL

    Args:
      bucket (str): The name of the bucket where the file is located.
      key (str): The name of the file in the bucket.
      expires_in: The number of seconds the presigned url is valid for. Defaults to 7200

    Returns:
      A presigned URL for the object.
    """
    if bucket is None or key is None:
        return False

    s3_client = boto3.client(
        's3',
        'ap-south-1',
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
    )
    params = {'Bucket': bucket, 'Key': key}
    return s3_client.generate_presigned_url('get_object', Params=params, ExpiresIn=expires_in)


def get_file_from_s3(file_name_s3: str, out_put_path: str = '/var/www/others/others.pdf', bucket=None):
    """
    It downloads a file from S3, and if it fails, it sends an email to the admin

    Args:
      file_name_s3 (str): The name of the file in S3.
      out_put_path (str): The path where you want to save the file. Defaults to /var/www/others/others.pdf
      bucket: The name of the bucket you want to upload to.

    Returns:
      A boolean value.
    """
    if not settings.USE_S3:
        return False

    if bucket is None:
        bucket = settings.AWS_STORAGE_BUCKET_NAME
    try:
        s3_client = boto3.client(
            's3', aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY
        )
    except Exception as e:
        send_error_mail.apply_async(
            args=['Download S3 Error', f'Error File Name: {file_name_s3}', str(e), str(traceback.format_exc())]
        )
        return False
    try:
        s3_client.download_file(bucket, file_name_s3, out_put_path)
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            print(f'{file_name_s3}, file not found in s3')
            return False
        if e.response['Error']['Code'] == 'InvalidObjectState':
            print(f'{file_name_s3}, file found, but not supported object state')
            return False
        send_error_mail.apply_async(
            args=['Download S3 Error', f'Error File Name: {file_name_s3}', str(e), str(traceback.format_exc())]
        )
    except Exception as e:
        send_error_mail.apply_async(
            args=['Download S3 Error', f'Error File Name: {file_name_s3}', str(e), str(traceback.format_exc())]
        )
        return False
    return True


def get_fileobj_from_s3(file_name_s3: str, bucket=None) -> Tuple[bool, Union[str, BytesIO]]:
    """
    It takes a file name and a bucket name, and returns a tuple of a boolean and either a string or a BytesIO object

    Args:
      file_name_s3 (str): The name of the file in S3.
      bucket: The name of the bucket to download from.

    Returns:
      A tuple of a boolean and a string or a BytesIO object.
    """
    if not settings.USE_S3:
        return False, 'S3 is disabled'

    if bucket is None:
        bucket = settings.AWS_STORAGE_BUCKET_NAME
    try:
        s3_client = boto3.client(
            's3', aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY
        )
    except Exception as e:
        send_error_mail.apply_async(
            args=['Download Obj S3 Error', f'Error S3 File Name: {file_name_s3}', str(e), str(traceback.format_exc())]
        )
        return False, 'Issue with s3 client'
    try:
        data = BytesIO()
        s3_client.download_fileobj(bucket, file_name_s3, data)
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            print(f'{file_name_s3}, file not found in s3')
            return False, 'File not found in s3'
        if e.response['Error']['Code'] == 'InvalidObjectState':
            print(f'{file_name_s3}, file found, but not supported object state')
            return False, 'File found, but not supported object state'
        send_error_mail.apply_async(
            args=['Download S3 Error', f'Error File Name: {file_name_s3}', str(e), str(traceback.format_exc())]
        )
    except Exception as e:
        send_error_mail.apply_async(
            args=['Download S3 Error', f'Error File Name: {file_name_s3}', str(e), str(traceback.format_exc())]
        )
        return False, 'Issue with s3 client getting file'
    return True, data


def delete_file_from_s3(file_name: str, bucket: str = None):
    """
    It deletes a file from S3

    Args:
      file_name (str): The name of the file you want to delete.
      bucket (str): The name of the bucket you want to delete the file from.

    Returns:
      A boolean value.
    """
    if not settings.USE_S3:
        return False

    if bucket is None:
        bucket = settings.AWS_STORAGE_BUCKET_NAME
    try:
        s3_client = boto3.client(
            's3', aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY
        )
    except Exception as e:
        send_error_mail.apply_async(
            args=['Delete S3 Error', f'Error with File: {file_name}', str(e), traceback.format_exc()]
        )
        return False
    try:
        s3_client.delete_object(Bucket=bucket, Key=file_name)
    except Exception as e:
        send_error_mail.apply_async(
            args=['Delete S3 Error', f'Error with File: {file_name}', str(e), traceback.format_exc()]
        )
        return False
    return True


def create_s3_file_from_binary_data(
    binary_data: Union[str, bytes], content_type: str, s3_filename: str, s3_bucket: str = None
):
    """
    Create a file in S3 bucket from binary data

    Args:
      binary_data (Union[str, bytes]): The binary data you want to save into the S3 file.
      content_type (str): The content type of the file.
      s3_filename (str): The name of the file you want to create in S3.
      s3_bucket (str): The name of the bucket you want to upload to.

    Returns:
      True or False
    """
    if not settings.USE_S3:
        return False

    if not s3_bucket:
        s3_bucket = settings.AWS_STORAGE_BUCKET_NAME

    try:
        s3_client = boto3.client(
            's3', aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY
        )
    except Exception as e:
        exec_str = traceback.format_exc()
        send_error_mail.apply_async(
            args=['AWS S3 File Creation Error | client()', f's3 filename: {s3_filename}', str(e), exec_str]
        )
        return False

    try:
        s3_client.put_object(Body=binary_data, Bucket=s3_bucket, Key=s3_filename, ContentType=content_type)
    except Exception as e:
        exec_str = traceback.format_exc()
        send_error_mail.apply_async(
            args=['AWS S3 File Creation Error | put_object()', f's3 filename: {s3_filename}', str(e), exec_str]
        )
        return False

    return True


def move_all_files_to_different_folder_s3(
    source_bucket_name: str,
    source_folder_path: str,
    destination_bucket_name: str,
    destination_folder_path: str,
    to_delete: bool,
):

    """
    Move files to different folder in s3 bucket
    Args:
        source bucket name, source folder path, destination bucket name, destination folder path
    Returns:
      True or False
    """
    if not settings.USE_S3:
        return False
    try:
        s3_client = boto3.client(
            's3', aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY
        )
    except Exception as e:
        exec_str = traceback.format_exc()
        send_error_mail.apply_async(
            args=['AWS S3 File Movement Error| client()', f's3 source bucket: {source_bucket_name}', str(e), exec_str]
        )
        return False
    # List all objects in the source folder
    response = s3_client.list_objects_v2(Bucket=source_bucket_name, Prefix=source_folder_path)

    # Move each object to the destination folder
    for obj in response.get('Contents', []):
        # Construct the new key name by replacing the source folder with the destination folder
        new_key = obj['Key'].replace(source_folder_path, destination_folder_path, 1)

        # Copy the object to the new location
        s3_client.copy_object(
            Bucket=destination_bucket_name, CopySource={'Bucket': source_bucket_name, 'Key': obj['Key']}, Key=new_key
        )
        if to_delete:
            # Delete the object from the source location
            s3_client.delete_object(Bucket=source_bucket_name, Key=obj['Key'])

    return True


def get_excel_file_object_from_s3(bucket_name: str, file_with_location: str):
    if not settings.USE_S3:
        return False, ''
    try:
        s3_client = boto3.client(
            's3', aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY
        )
    except Exception as e:
        exec_str = traceback.format_exc()
        send_error_mail.apply_async(
            args=['S3 Excel file obj| client()', f'bucket file: {bucket_name} {file_with_location}', str(e), exec_str]
        )
        return False, ''
    obj = s3_client.get_object(Bucket=bucket_name, Key=file_with_location)

    # Create in-memory file object from S3 object data
    file_v = BytesIO(obj['Body'].read())
    return True, file_v


def move_specific_file_to_folder(
    bucket_name: str, file_w_folder: str, destination_folder: str, to_delete_file: bool, dest_file_name=None
):
    """
    Args:
        bucket_name: str
        file_w_folder: '/folder_name/file_name'
        destination_folder: '/dest_folder_name/
    """
    if not settings.USE_S3:
        return False
    try:
        s3_client = boto3.client(
            's3', aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY
        )
    except Exception as e:
        exec_str = traceback.format_exc()
        send_error_mail.apply_async(
            args=['Move specfic file S3', f'bucket file: {bucket_name} {file_w_folder}', str(e), exec_str]
        )
        return False
    # get file name from file_w_folder
    # folder_name = os.path.dirname(file_w_folder)
    if not dest_file_name:
        file_name = os.path.basename(file_w_folder)
    else:
        file_name = dest_file_name
    # Copy the file to the new folder
    s3_client.copy_object(
        Bucket=bucket_name, CopySource=f'{bucket_name}/{file_w_folder}', Key=destination_folder + file_name
    )

    if to_delete_file:
        # Delete the original file from the old folder
        s3_client.delete_object(Bucket=bucket_name, Key=file_w_folder)
    return True
