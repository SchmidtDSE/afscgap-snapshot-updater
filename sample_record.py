import io
import json
import os
import sys

import boto3
import fastavro

NUM_ARGS = 2
USAGE_STR = 'python sample_record.py [bucket] [path]'


def main():
    if len(sys.argv) != NUM_ARGS + 1:
        print(USAGE_STR)
        sys.exit(1)

    access_key = os.environ['AWS_ACCESS_KEY']
    access_secret = os.environ['AWS_ACCESS_SECRET']

    s3_client = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=access_secret
    )

    bucket = sys.argv[1]
    full_loc = sys.argv[2]
    target_buffer = io.BytesIO()

    s3_client.download_fileobj(bucket, full_loc, target_buffer)
    target_buffer.seek(0)
    result = list(fastavro.reader(target_buffer))

    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
