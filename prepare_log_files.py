import boto3
import os
import gzip
from os import walk
import shutil


def download_and_unzip(logger):
    bucket_name = os.getenv("bucket_name")
    download_directory = os.getenv("download_directory")

    account_nr = os.getenv("account_nr")    # provide env - plain account number of your LB
    bucket_az = os.getenv("bucket_AZ")      # provide env in format eu-west-1
    logs_date = os.getenv("logs_date")      # provide env in format YYYY/MM/DD
    prefix = os.path.join("AWSLogs", account_nr, "elasticloadbalancing", bucket_az, logs_date)

    s3_resource = boto3.resource('s3')
    my_bucket = s3_resource.Bucket(bucket_name)

    for my_bucket_object in my_bucket.objects.filter(Prefix=prefix):
        logger.debug(my_bucket_object)
        path, filename = os.path.split(my_bucket_object.key)
        absolute_path = os.path.join(download_directory,filename)
        my_bucket.download_file(my_bucket_object.key, absolute_path)

    logger.info("All files downloaded")
    unzip_files()


def unzip_files(logger):
    downloaded_gz_files_directory = os.getenv("download_directory")
    destination_location = os.getenv("logs_location")

    _, _, filenames = next(walk(downloaded_gz_files_directory))

    for filename in filenames:
        abs_path_file_in = os.path.join(downloaded_gz_files_directory, filename)
        # cutting last three chars (.gz)
        abs_path_file_out = os.path.join(destination_location,filename)[:-3]

        with gzip.open(abs_path_file_in, 'rb') as f_in:
            with open(abs_path_file_out, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
    logger.info("All files unzipped")
