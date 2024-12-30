import io
import itertools
import os
import sys

import boto3
import coiled
import dask
import dask.bag
import fastavro

USAGE_STR = 'python render_flat.py [bucket] [keys]'
NUM_ARGS = 2

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

REQUIRES_DATE_ROUND = {'date_time'}

REQUIRES_FLAT = {
    'performance',
    'cruise',
    'cruisejoin',
    'hauljoin',
    'haul'
}

IGNORE_ZEROS = {
    'species_code',
    'scientific_name',
    'common_name'
}



def process_file(bucket, year, survey, haul, key):

    import io
    import os

    import botocore
    import boto3
    import fastavro

    access_key = os.environ['AWS_ACCESS_KEY']
    access_secret = os.environ['AWS_ACCESS_SECRET']

    s3_client = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=access_secret
    )

    def get_avro(full_loc):
        target_buffer = io.BytesIO()
        try:
            s3_client.download_fileobj(bucket, full_loc, target_buffer)
            target_buffer.seek(0)
            return list(fastavro.reader(target_buffer))
        except botocore.exceptions.ClientError:
            return None

    def generate_index_record(record):
        value = record[key]
        key_pieces = [year, survey, haul]
        key_pieces_str = map(lambda x: str(x), key_pieces)
        key_output = '\t'.join(key_pieces_str)
        return {
            'value': value,
            'keys': set([key_output])
        }

    template_vals = (year, survey, haul)
    flat_loc = 'joined/%d_%s_%d.avro' % template_vals
    flat_records = get_avro(flat_loc)

    def is_non_zero(target):
        def is_field_non_zero(field):
            value = target[field]
            return (value is not None) and (value > 0)

        fields = ['cpue_kgkm2', 'cpue_nokm2', 'weight_kg', 'count']
        flags = map(is_field_non_zero, fields)
        flags_positive = filter(lambda x: x is True, flags)
        num_flags_positive = sum(map(lambda x: 1, flags_positive))
        return num_flags_positive > 0

    if key in IGNORE_ZEROS:
        flat_records_allowed = filter(is_non_zero, flat_records)
    else:
        flat_records_allowed = flat_records

    index_records = map(generate_index_record, flat_records_allowed)

    return list(index_records)


def build_output_record(target):
    def process_key(key_str):
        key_pieces = key_str.split('\t')
        year = int(key_pieces[0])
        survey = key_pieces[1]
        haul = int(key_pieces[2])
        return {'year': year, 'survey': survey, 'haul': haul}

    return {
        'value': target['value'],
        'keys': [process_key(x) for x in target['keys']]
    }


def get_observations_meta(bucket):
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
            'path': path,
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
    return map(make_haul_metadata_record, keys)


def write_sample(key, bucket, sample):
    import io
    import os
    import random

    import boto3
    import fastavro

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

    sample_realized = list(sample)
    if len(sample_realized) == 0:
        return None

    batch = random.randint(0, 1000000)

    access_key = os.environ.get('AWS_ACCESS_KEY', '')
    access_secret = os.environ.get('AWS_ACCESS_SECRET', '')

    target_buffer = io.BytesIO()
    fastavro.writer(
        target_buffer,
        INDEX_SCHEMA,
        sample_realized
    )
    target_buffer.seek(0)

    s3_client = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=access_secret
    )
    output_loc = 'index_sharded/%s_%d.avro' % (key, batch)
    s3_client.upload_fileobj(target_buffer, bucket, output_loc)
    return batch


def assign_batch(target):
    import random
    return {'batch': random.randint(0, 100), 'target': [target]}


def main():
    if len(sys.argv) != NUM_ARGS + 1:
        print(USAGE_STR)
        sys.exit(1)

    bucket = sys.argv[1]
    keys = sys.argv[2].split(',')
    hauls_meta = list(get_observations_meta(bucket))

    access_key = os.environ.get('AWS_ACCESS_KEY', '')
    access_secret = os.environ.get('AWS_ACCESS_SECRET', '')
    cluster = coiled.Cluster(
        name='DseProcessAfscgap',
        n_workers=100,
        worker_vm_types=['m7a.medium'],
        scheduler_vm_types=['m7a.medium'],
        environ={
            'AWS_ACCESS_KEY': access_key,
            'AWS_ACCESS_SECRET': access_secret
        }
    )
    client = cluster.get_client()

    def execute_for_key(key):
        hauls_meta_realized = dask.bag.from_sequence(hauls_meta)
        index_records_nest = hauls_meta_realized.map(
            lambda x: process_file(
                bucket,
                x['year'],
                x['survey'],
                x['haul'],
                key
            )
        )
        index_records = index_records_nest.flatten()

        def key_record(target):
            if key in REQUIRES_ROUNDING:
                if target['value'] is None:
                    return target['value']
                else:
                    return '%.2f' % target['value']
            elif key in REQUIRES_DATE_ROUND:
                if target['value'] is None:
                    return target['value']
                else:
                    return target['value'].split('T')[0]
            else:
                return target['value']

        def combine_records(a, b):
            return {'value': a['value'], 'keys': a['keys'].union(b['keys'])}

        if key in REQUIRES_FLAT:
            index_records_output = index_records.map(build_output_record)
        else:
            index_records_grouped_nest = index_records.foldby(
                key=key_record,
                binop=combine_records
            )
            index_records_grouped = index_records_grouped_nest.map(lambda x: x[1])
            index_records_output = index_records_grouped.map(build_output_record)

        repartitioned = index_records_output.repartition(npartitions=20)
        incidies_future = repartitioned.map_partitions(
            lambda x: write_sample(key, bucket, x)
        )

        indicies_all = incidies_future.compute(scheduler=client)
        indicies = filter(lambda x: x is not None, indicies_all)
        indicies_strs = list(map(lambda x: str(x), indicies))
        assert len(indicies_strs) == len(set(indicies_strs))

        loc = os.path.join('index_shards', key + '.txt')
        with open(loc, 'w') as f:
            f.write('\n'.join(indicies_strs))

    for key in keys:
        print('Executing for %s...' % key)
        execute_for_key(key)

    cluster.close(force_shutdown=True)


if __name__ == '__main__':
    main()
