import io
import itertools
import os
import sys

import boto3
import fastavro
import toolz.itertoolz

REQUIRES_ROUNDING = {
    'latitude_dd_start',
    'longitude_dd_start',
    'latitude_dd_end',
    'longitude_dd_end',
    'bottom_temperature_c',
    'surface_temperature_c',
    'depth_m',
    'distance_fished_km',
    'duration_hr',
    'net_width_m',
    'net_height_m',
    'area_swept_km2',
    'cpue_kgkm2',
    'cpue_nokm2',
    'weight_kg',
}

INDEX_SCHEMA = {
    'doc': 'Index from a value to an observations flat file.',
    'name': 'Index',
    'namespace': 'edu.dse.afscgap',
    'type': 'record',
    'fields': [
        {'name': 'value', 'type': ['string', 'long', 'double', 'null']},
        {'name': 'keys', 'type': {
            'type': 'array',
            'items': {
                'name': 'Key',
                'type': 'record',
                'fields':[
                    {'name': 'year', 'type': 'int'},
                    {'name': 'survey', 'type': 'string'},
                    {'name': 'haul', 'type': 'long'}
                ]
            }
        }}
    ]
}

REQUIRES_DATE_ROUND = {'date_time'}

NUM_ARGS = 2
USAGE_STR = 'python combine_shards.py [bucket] [key]'


def main():
    if len(sys.argv) != NUM_ARGS + 1:
        print(USAGE_STR)
        sys.exit(1)

    bucket = sys.argv[1]
    key = sys.argv[2]

    filename = key + '.txt'
    loc = os.path.join('index_shards', filename)
    with open(loc) as f:
        batches = [int(x.strip()) for x in f]

    access_key = os.environ['AWS_ACCESS_KEY']
    access_secret = os.environ['AWS_ACCESS_SECRET']

    s3_client = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=access_secret
    )

    def get_avro(full_loc):
        target_buffer = io.BytesIO()
        s3_client.download_fileobj(bucket, full_loc, target_buffer)
        target_buffer.seek(0)
        return list(fastavro.reader(target_buffer))

    def normalize_record(target):
        value = target['value']
        if value is not None:
            if key in REQUIRES_ROUNDING:
                target['value'] = '%.2f' % value
            elif key in REQUIRES_DATE_ROUND:
                target['value'] = value.split('T')[0]

        return target

    batch_locs = map(lambda x: 'index_sharded/%s_%d.avro' % (key, x), batches)
    shards = map(get_avro, batch_locs)
    combined = itertools.chain(*shards)
    normalized = map(normalize_record, combined)

    write_buffer = io.BytesIO()
    fastavro.writer(
        write_buffer,
        INDEX_SCHEMA,
        normalized
    )
    write_buffer.seek(0)

    s3_client = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=access_secret
    )
    output_loc = 'index/%s.avro' % key
    s3_client.upload_fileobj(write_buffer, bucket, output_loc)


if __name__ == '__main__':
    main()
