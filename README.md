# archive_manager
First aid for your bloated disk. Scan, clean, backup, and restore files matching a pattern or age.

This script will help you find and delete files matching a pattern or age and optionally backup &amp; restore to AWS S3.  This is a work in progress, but it's functional.  It was built to run on a drive that is at 100% capacity, so it won't attempt to store any information on the disk it's cleaning.

More features are coming, so be sure to check out the roadmap and drop me a note if you have any suggestions.

## Installation

Pre-requisite: Make sure you install awscli and configure your credentials if you plan on using s3.

1. Clone the repo
2. Link the script to your path
```bash
$ ln -s /path/to/archive_manager.py /usr/local/bin/archive_manager
```
3. Make the script executable
```bash
$ chmod +x /path/to/archive_manager.py
```
4. If you plan on using s3, install boto3, the only requirement.
```bash
$ pip3 install boto3
```

## Example Usage
Find all files matching a pattern and see how much disk space they take up
```bash
$ archive_manager /folder/to/backup '*.jp*g' 2Y -R '*/wedding/2023/*'
```

Delete JPEG files older than 2 years
```bash
$ archive_manager /folder/to/backup '*.jp*g' 2Y --destroy
```

Delete files older than 2 month that match a regex and backup to s3 but don't delete
```bash
$ archive_manager ./big_directory '*.zip' 2M --bucket my-bucket --backup
```

Restore files from s3 to a local directory
```bash
$ archive_manager /folder/to/backup '*.jp*g' 2Y --bucket my-bucket --restore
```

## Roadmap
1. Add tests and refactor into clean code
2. Create Pipy package
3. Add asyncio to speed up s3 operations
4. Ability to create/upack tar ball chunks of files to reduce s3 operations
5. Support for Iceberg storage.

## Contributing
Pull requests are very welcome!
Please note: I had to write this for python 3.4.  That will change soon, but for now, just understand that's why I'm using the old string formatting.

## License
[MIT](./LICENSE)


