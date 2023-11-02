#!/usr/bin/python3

import os
import sys
import argparse
import datetime
import re
import fnmatch
import boto3


def parse_duration_string(duration_string):
    total_seconds = 0
    duration_regex = re.compile(r'(\d+)([hHmMdDwWyY])')
    duration_mapping = {
        'h': 3600,
        'm': 60,
        'D': 86400,
        'M': 2592000,
        'Y': 31536000
    }

    matches = duration_regex.findall(duration_string)
    for number, unit in matches:
        total_seconds += int(number) * duration_mapping[unit]

    return total_seconds


def format_size(size_bytes, human_readable=True):
    if human_readable:
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return "{:.1f} {}".format(size_bytes, unit)
            size_bytes /= 1024.0
    else:
        return str(size_bytes) + ' B'


def format_seconds(seconds, human_readable=True):
    if human_readable:
        if seconds < 1:
            return "{:.2f} ms".format(seconds * 1000)
        elif seconds < 60:
            return "{:.2f} seconds".format(seconds)
        elif seconds < 3600:
            minutes = seconds / 60
            return "{:.2f} minutes".format(minutes)
        elif seconds < 86400:
            hours = seconds / 3600
            return "{:.2f} hours".format(hours)
        elif seconds < 31536000:
            days = seconds / 86400
            return "{:.2f} days".format(days)
        else:
            years = seconds / 31536000
            if years >= 2:
                return "{:.0f} years".format(years)
            else:
                return "1 year"
    else:
        return str(seconds) + ' seconds'

def matches_regex(file_path, regex_pattern):
    try:
        return re.search(regex_pattern, file_path)
    except re.error:
        print("Error: Invalid regex pattern.")
        sys.exit(1)

def delete_old_files(cutoff_duration):
    cutoff_date = datetime.datetime.now() - datetime.timedelta(seconds=cutoff_duration)
    files_matched = 0
    files_deleted = 0
    total_size_deleted = 0  # Variable to keep track of total size of deleted files
    directories_scanned = 0
    if args.regex_pattern:
        regex_pattern = re.compile(args.regex_pattern)
    else:
        regex_pattern = None

    for root, dirs, files in os.walk(args.directory):
        # Increment directories count for each unique directory visited
        directories_scanned += 1
        if args.very_verbose:
            print("Checking folder: '{}'".format(root))

        for file_name in files:
            if (not args.regex_pattern or matches_regex(os.path.join(root, file_name), regex_pattern)) and fnmatch.fnmatch(file_name, args.glob_pattern) and not os.path.islink(os.path.join(root, file_name)):
                file_path = os.path.join(root, file_name)
                file_modified_time = os.path.getmtime(file_path)
                modified_date = datetime.datetime.fromtimestamp(file_modified_time)
                if modified_date < cutoff_date:
                    files_matched += 1
                    file_size = os.path.getsize(file_path)
                    total_size_deleted += file_size  # Add the size of deleted file to the total
                    age_seconds = (datetime.datetime.now() - modified_date).total_seconds()
                    if args.s3_bucket and args.backup_to_s3:
                        # Preserve the directory structure on S3
                        local_file_path = os.path.normpath(file_path)
                        if args.verbose:
                            print("Uploading: {} (Size: {}, Age: {})".format(file_path, format_size(file_size, args.human_readable), format_seconds(age_seconds, args.human_readable)))
                        upload_to_s3(local_file_path, args.s3_bucket, local_file_path, verbose=args.verbose)
                    if args.destroy:
                        os.remove(file_path)
                        files_deleted += 1
                        if args.verbose:
                            print("Deleted: {} (Size: {}, Age: {})".format(file_path, format_size(file_size, args.human_readable), format_seconds(age_seconds, args.human_readable)))
                    elif args.verbose:
                        print("Matched: {} (Size: {}, Age: {})".format(file_path, format_size(file_size, args.human_readable), format_seconds(age_seconds, args.human_readable)))

        sys.stdout.write("\rFiles matched: {} - Files deleted: {} - Directories scanned: {}".format(files_matched, files_deleted, directories_scanned))
        sys.stdout.flush()

        if not args.recursive:
            break

    return files_matched, files_deleted, directories_scanned, total_size_deleted  # Return the total size of deleted files


def restore_files_from_s3(bucket_name, prefix, restore_folder):
    s3 = boto3.client('s3')

    # remove Linux path characters from prefix
    # NOTE: This will need to be changed if running on Windows
    _prefix = prefix.lstrip('.')
    _prefix = _prefix.lstrip('/')

    # List objects in the specified bucket and prefix
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=_prefix)

    # Iterate through the objects and restore them
    for obj in response.get('Contents', []):
        s3_key = obj['Key']
        rel_local_file_path = s3_key.replace(_prefix, '').lstrip('/')
        local_file_path = os.path.join(restore_folder, rel_local_file_path)

        # Download the file from S3 to the local restore folder
        s3.download_file(bucket_name, s3_key, local_file_path)

        # Get the original Last Modified timestamp from the S3 object's user metadata
        response = s3.head_object(Bucket=bucket_name, Key=s3_key)
        original_last_modified = response['Metadata'].get('last-modified')

        if original_last_modified:
            # Convert the original Last Modified timestamp to a timestamp (float) value
            original_last_modified_timestamp = datetime.datetime.strptime(original_last_modified,
                                                                          '%Y-%m-%dT%H:%M:%SZ').timestamp()

            # Set the local file's Last Modified timestamp to the original Last Modified timestamp
            os.utime(local_file_path, (original_last_modified_timestamp, original_last_modified_timestamp))

        print(f"Restored file: {s3_key} to {local_file_path} with original LastModified: {original_last_modified}")


def upload_to_s3(local_file_path, s3_bucket_name, s3_object_key, verbose=False):
    try:
        # Get the original LastModified timestamp of the local file
        original_last_modified_timestamp = os.path.getmtime(local_file_path)

        # Convert the LastModified timestamp to a formatted string
        original_last_modified = datetime.datetime.fromtimestamp(original_last_modified_timestamp).strftime(
            '%Y-%m-%dT%H:%M:%SZ')

        # Create an S3 client
        s3_client = boto3.client('s3')

        # Upload the file to S3 and set the original LastModified in user metadata
        s3_client.upload_file(local_file_path, s3_bucket_name, s3_object_key,
                              ExtraArgs={'Metadata': {'last-modified': original_last_modified}})

        # Set the user metadata for the uploaded object
        s3_client.put_object(Bucket=s3_bucket_name, Key=s3_object_key,
                             Metadata={'last-modified': original_last_modified})

        if verbose:
            msg = "Uploaded '{}' to S3 bucket '{}' with key '{}' and original LastModified: {}"
            print(msg.format(local_file_path, s3_bucket_name, s3_object_key, original_last_modified))

    except Exception as e:
        print("Error uploading '{}' to S3: {}".format(local_file_path, str(e)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Delete files older than a specified duration in the given directory matching a glob pattern.")
    parser.add_argument("directory", help="Path to the directory to search for files.")
    parser.add_argument("glob_pattern", help="Glob pattern to match files (e.g., '*.txt').")
    parser.add_argument("cutoff_duration", help="Time duration string specifying the cutoff date (e.g., '1Y3M' for 1 year and 3 months).")
    parser.add_argument("-r", "--recursive", action="store_true", help="Recursively search for files in sub-directories.")
    parser.add_argument("--destroy", action="store_true", help="Actually delete files. Without this flag, it will only pretend to delete.")
    parser.add_argument("-v", "--verbose", action="store_true", help="Print detailed information about matched or deleted files.")
    parser.add_argument("-H", "--human-readable", action="store_true", help="Print sizes and ages in human-readable format.")
    parser.add_argument("--s3-bucket", help="S3 bucket name to back up the files.")
    parser.add_argument("--restore-from-s3", action="store_true", help="Restore files from S3.")
    parser.add_argument("--backup-to-s3", action="store_true", help="Backup files to S3. If destroying files, they will be backed up before deletion.")
    parser.add_argument("--pretend", action="store_true", help="Pretend to delete or move files. This is the default behavior if --destroy is not specified.")
    parser.add_argument("-R", "--regex-pattern", help="Filter matches using a regex pattern.")
    parser.add_argument("-V", "--very-verbose", action="store_true",
                        help="Print very detailed information including each folder being checked.")

    args = parser.parse_args()

    cutoff_duration = parse_duration_string(args.cutoff_duration)
    if args.destroy and args.restore_from_s3:
        answer = input("Are you sure you want to destroy files and restore from S3 at the same time? (y/N): ")
        if answer.lower() == 'n':
            print("Aborting...")
            exit()
    if args.restore_from_s3 or args.backup_to_s3:
        # import here so user doesn't need boto3 installed if they don't use S3
        if not args.s3_bucket:
            raise ValueError("Error: S3 bucket name is required for restoration. Use the '--s3-bucket' argument.")
    if not os.path.exists(args.directory):
        raise ValueError("Error: The directory '{}' does not exist.".format(args.directory))
    elif not args.restore_from_s3:
        abs_directory_path = os.path.normpath(os.path.abspath(args.directory))
        print("Scanning {} for files matching the pattern '{}'...".format(abs_directory_path, args.glob_pattern))
        files_matched, num_deleted_files, directories_scanned, total_size_deleted = delete_old_files(cutoff_duration)
        match_msg = "\nFound {} files matching '{}'".format(files_matched, args.glob_pattern)
        if args.regex_pattern:
            match_msg += " and regex pattern '{}'".format(args.regex_pattern)
        match_msg += " in {} directories with a total size of {}".format(directories_scanned, format_size(total_size_deleted, args.human_readable))
        print(match_msg)
        if args.destroy:
            print("Deleted {} files, total size: {}.".format(num_deleted_files, format_size(total_size_deleted, args.human_readable)))
        elif args.verbose and files_matched > num_deleted_files:
            print("Use --destroy to delete {} matched files, total size: {}.".format(files_matched - num_deleted_files, format_size(total_size_deleted, args.human_readable)))
    elif args.restore_from_s3:
        restore_files_from_s3(args.s3_bucket, args.directory, args.directory)
