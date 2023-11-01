import boto3


def upload_to_s3(local_file_path, s3_bucket_name, s3_object_key, verbose=False):
    try:
        s3_client = boto3.client('s3')
        s3_client.upload_file(local_file_path, s3_bucket_name, s3_object_key)
        if verbose:
            print("Uploaded '{}' to S3 bucket '{}' with key '{}'.".format(local_file_path, s3_bucket_name, s3_object_key))
    except Exception as e:
        print("Error uploading '{}' to S3: {}".format(local_file_path, str(e)))


def download_from_s3(s3_bucket_name, s3_object_key, local_file_path, verbose=False):
    try:
        s3_client = boto3.client('s3')
        s3_client.download_file(s3_bucket_name, s3_object_key, local_file_path)
        if verbose:
            print("Downloaded '{}' from S3 bucket '{}' with key '{}'.".format(local_file_path, s3_bucket_name, s3_object_key))
    except Exception as e:
        print("Error downloading '{}' from S3: {}".format(local_file_path, str(e)))
