import io
import itertools
import os
import sys

import boto3
import fastavro
import toolz.itertoolz

KEY_SCHEMA = {
    'doc': 'Key to an observation flat file.',
    'name': 'Key',
    'namespace': 'edu.dse.afscgap',
    'type': 'record',
    'fields': [
        {'name': 'year', 'type': 'int'},
        {'name': 'survey', 'type': 'string'},
        {'name': 'haul', 'type': 'long'}
    ]
}

NUM_ARGS = 1
USAGE_STR = 'python write_main_index.py [bucket]'


def main():
    if len(sys.argv) != NUM_ARGS + 1:
        print(USAGE_STR)
        sys.exit(1)

    bucket = sys.argv[1]

    access_key = os.environ['AWS_ACCESS_KEY']
    access_secret = os.environ['AWS_ACCESS_SECRET']

    s3_client = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=access_secret
    )

    def make_haul_metadata_record(path):
        filename_with_path = path.split('/')[-1]
        filename = filename_with_path.split('.')[0]
        components = filename.split('_')
        return {
            'year': int(components[0]),
            'survey': components[1],
            'haul': int(components[2])
        }

    paginator = s3_client.get_paginator('list_objects_v2')
    iterator = paginator.paginate(Bucket=bucket, Prefix='joined/')
    pages = filter(lambda x: 'Contents' in x, iterator)
    contents = map(lambda x: x['Contents'], pages)
    contents_flat = itertools.chain(*contents)
    keys = map(lambda x: x['Key'], contents_flat)
    metadata_records = map(make_haul_metadata_record, keys)

    write_buffer = io.BytesIO()
    fastavro.writer(
        write_buffer,
        KEY_SCHEMA,
        metadata_records
    )
    write_buffer.seek(0)

    s3_client = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=access_secret
    )
    output_loc = 'index/main.avro'
    s3_client.upload_fileobj(write_buffer, bucket, output_loc)


if __name__ == '__main__':
    main()
