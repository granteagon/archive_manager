#!/usr/bin/python3

import os
import sys
import argparse
import datetime
import re
import fnmatch


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


def delete_old_files(cutoff_duration):
    cutoff_date = datetime.datetime.now() - datetime.timedelta(seconds=cutoff_duration)
    files_matched = 0
    files_deleted = 0
    total_size_deleted = 0  # Variable to keep track of total size of deleted files
    directories_scanned = 0

    for root, dirs, files in os.walk(args.directory):
        # Increment directories count for each unique directory visited
        directories_scanned += 1

        for file_name in files:
            if fnmatch.fnmatch(file_name, args.glob_pattern) and not os.path.islink(os.path.join(root, file_name)):
                file_path = os.path.join(root, file_name)
                file_modified_time = os.path.getmtime(file_path)
                modified_date = datetime.datetime.fromtimestamp(file_modified_time)
                if modified_date < cutoff_date:
                    files_matched += 1
                    file_size = os.path.getsize(file_path)
                    total_size_deleted += file_size  # Add the size of deleted file to the total
                    age_seconds = (datetime.datetime.now() - modified_date).total_seconds()
                    if args.s3_bucket:
                        # Preserve the directory structure on S3
                        local_file_path = os.path.normpath(file_path)
                        if args.restore_from_s3:
                            if args.verbose:
                                print("Downloading: {} (Size: {}, Age: {})".format(file_path, format_size(file_size, args.human_readable), format_seconds(age_seconds, args.human_readable)))
                            download_from_s3(args.s3_bucket, local_file_path, local_file_path, verbose=args.verbose)
                        elif args.backup_to_s3:
                            if args.verbose:
                                print("Uploading: {} (Size: {}, Age: {})".format(file_path, format_size(file_size, args.human_readable), format_seconds(age_seconds, args.human_readable)))
                            upload_to_s3(local_file_path, args.s3_bucket, local_file_path, verbose=args.verbose)
                    if args.destroy and not args.restore_from_s3:
                        os.remove(file_path)
                        files_deleted += 1
                        if args.verbose:
                            print("Deleted: {} (Size: {}, Age: {})".format(file_path, format_size(file_size, args.human_readable), format_seconds(age_seconds, args.human_readable)))
                    elif args.verbose:
                        print("Matched: {} (Size: {}, Age: {})".format(file_path, format_size(file_size, args.human_readable), format_seconds(age_seconds, args.human_readable)))

        # Print progress report if verbose mode is not enabled
        if not args.verbose:
            sys.stdout.write("\rFiles matched: {} - Files deleted: {} - Directories scanned: {}".format(files_matched, files_deleted, directories_scanned))
            sys.stdout.flush()

    return files_matched, files_deleted, directories_scanned, total_size_deleted  # Return the total size of deleted files

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

    args = parser.parse_args()

    cutoff_duration = parse_duration_string(args.cutoff_duration)
    if args.restore_from_s3 or args.backup_to_s3:
        # import here so user doesn't need boto3 installed if they don't use S3
        from s3 import upload_to_s3, download_from_s3
        if not args.s3_bucket:
            print("Error: S3 bucket name is required for restoration. Use the '--s3-bucket' argument.")
    if not os.path.exists(args.directory):
        print("Error: The directory '{}' does not exist.".format(args.directory))
    else:
        abs_directory_path = os.path.normpath(os.path.abspath(args.directory))
        print("Scanning {} for files matching the pattern '{}'...".format(abs_directory_path, args.glob_pattern))
        files_matched, num_deleted_files, directories_scanned, total_size_deleted = delete_old_files(cutoff_duration)
        print("\nFound {} files matching '{}' in {} for a total size of {}".format(files_matched, args.glob_pattern, directories_scanned, format_size(total_size_deleted, args.human_readable)))

        if args.destroy:
            print("Deleted {} files, total size: {}.".format(num_deleted_files, format_size(total_size_deleted, args.human_readable)))
        elif args.verbose and files_matched > num_deleted_files:
            print("Use --destroy to delete {} matched files, total size: {}.".format(files_matched - num_deleted_files, format_size(total_size_deleted, args.human_readable)))
