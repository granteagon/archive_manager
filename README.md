# archive_manager
Simple Python script to delete files matching a pattern or age with optional s3 backup &amp; restore.

## Usage
Just download the file and run it with Python 3 on a linux-y shell.  It will prompt you for the rest.

## Example
```bash
# delete all pictures older than 2 years
$ python3 archive_manager.py ./my-bloated-img-directory '*.jpg' 2Y 
```

