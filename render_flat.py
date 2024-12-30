import csv
import io
import itertools
import functools
import os
import sys

import boto3
import coiled
import fastavro

USAGE_STR = 'python render_flat.py [bucket] [filenames]'
NUM_ARGS = 2


OBSERVATION_SCHEMA = {
    'doc': 'Description of an observation joined across haul, catch, species.',
    'name': 'Observation',
    'namespace': 'edu.dse.afscgap',
    'type': 'record',
    'fields': [
        {'name': 'year', 'type': ['int', 'null']},
        {'name': 'srvy', 'type': ['string', 'null']},
        {'name': 'survey', 'type': ['string', 'null']},
        {'name': 'survey_name', 'type': ['string', 'null']},
        {'name': 'survey_definition_id', 'type': ['long', 'null']},
        {'name': 'cruise', 'type': ['long', 'null']},
        {'name': 'cruisejoin', 'type': ['long', 'null']},
        {'name': 'hauljoin', 'type': ['long', 'null']},
        {'name': 'haul', 'type': ['long', 'null']},
        {'name': 'stratum', 'type': ['long', 'null']},
        {'name': 'station', 'type': ['string', 'null']},
        {'name': 'vessel_id', 'type': ['long', 'null']},
        {'name': 'vessel_name', 'type': ['string', 'null']},
        {'name': 'date_time', 'type': ['string', 'null']},
        {'name': 'latitude_dd_start', 'type': ['double', 'null']},
        {'name': 'longitude_dd_start', 'type': ['double', 'null']},
        {'name': 'latitude_dd_end', 'type': ['double', 'null']},
        {'name': 'longitude_dd_end', 'type': ['double', 'null']},
        {'name': 'bottom_temperature_c', 'type': ['double', 'null']},
        {'name': 'surface_temperature_c', 'type': ['double', 'null']},
        {'name': 'depth_m', 'type': ['double', 'null']},
        {'name': 'distance_fished_km', 'type': ['double', 'null']},
        {'name': 'duration_hr', 'type': ['double', 'null']},
        {'name': 'net_width_m', 'type': ['double', 'null']},
        {'name': 'net_height_m', 'type': ['double', 'null']},
        {'name': 'area_swept_km2', 'type': ['double', 'null']},
        {'name': 'performance', 'type': ['float', 'null']},
        {'name': 'species_code', 'type': ['long', 'null']},
        {'name': 'cpue_kgkm2', 'type': ['double', 'null']},
        {'name': 'cpue_nokm2', 'type': ['double', 'null']},
        {'name': 'count', 'type': ['long', 'null']},
        {'name': 'weight_kg', 'type': ['double', 'null']},
        {'name': 'taxon_confidence', 'type': ['string', 'null']},
        {'name': 'scientific_name', 'type': ['string', 'null']},
        {'name': 'common_name', 'type': ['string', 'null']},
        {'name': 'id_rank', 'type': ['string', 'null']},
        {'name': 'worms', 'type': ['long', 'null']},
        {'name': 'itis', 'type': ['long', 'null']},
        {'name': 'complete', 'type': ['boolean', 'null']}
    ]
}


def process_haul(bucket, year, survey, haul, species_by_code):

    import copy
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

    def append_catch_haul(catch_record, haul_record):
        catch_record.update(haul_record)
        return catch_record

    def append_species(target):
        species_code = target['species_code']

        if species_code not in species_by_code:
            target['complete'] = False
            return target

        species_record = species_by_code[species_code]
        target.update(species_record)
        return target

    def complete_record(target):
        keys = map(lambda x: x['name'], OBSERVATION_SCHEMA['fields'])
        keys_realized = list(keys)
        values = map(lambda x: target.get(x, None), keys_realized)
        return dict(zip(keys_realized, values))

    def convert_to_avro(records):
        records_complete = map(complete_record, records)
        target_buffer = io.BytesIO()
        fastavro.writer(target_buffer, OBSERVATION_SCHEMA, records_complete)
        target_buffer.seek(0)
        return target_buffer

    def mark_incomplete(target):
        target['complete'] = False
        return target

    def mark_complete(target):
        target['complete'] = True
        return target

    def make_zero_record(species, haul_record):
        haul_copy = copy.deepcopy(haul_record)
        haul_copy['species_code'] = species['species_code']
        haul_copy['cpue_kgkm2'] = 0
        haul_copy['cpue_nokm2'] = 0
        haul_copy['count'] = 0
        haul_copy['weight_kg'] = 0
        haul_copy['taxon_confidence'] = None
        haul_copy['scientific_name'] = species['scientific_name']
        haul_copy['common_name'] = species['common_name']
        haul_copy['id_rank'] = species['id_rank']
        haul_copy['worms'] = species['worms']
        haul_copy['itis'] = species['itis']
        haul_copy['complete'] = True
        return haul_copy

    template_vals = (year, survey, haul)
    haul_loc = 'haul/%d_%s_%d.avro' % template_vals
    haul_records = get_avro(haul_loc)
    assert len(haul_records) == 1
    haul_record = haul_records[0]

    catch_loc = 'catch/%d.avro' % haul
    catch_records = get_avro(catch_loc)

    if catch_records is None:
        catch_records_out = map(mark_incomplete, haul_records)
    else:
        catch_no_species = map(
            lambda x: append_catch_haul(x, haul_record),
            catch_records
        )
        catch_with_species = map(append_species, catch_no_species)
        catch_records_out = map(mark_complete, catch_with_species)

    catch_records_out_realized = list(catch_records_out)
    species_codes_found = set(map(
        lambda x: x.get('species_code', None),
        catch_records_out_realized
    ))
    species_codes_all = set(species_by_code.keys())
    speices_codes_missing = species_codes_all - species_codes_found
    speices_missing = map(lambda x: species_by_code[x], speices_codes_missing)
    catch_records_zero = map(
        lambda x: make_zero_record(x, haul_record),
        speices_missing
    )

    catch_records_all = itertools.chain(
        catch_records_out_realized,
        catch_records_zero
    )
    catch_with_species_avro = convert_to_avro(catch_records_all)
    output_loc = 'joined/%d_%s_%d.avro' % template_vals
    s3_client.upload_fileobj(catch_with_species_avro, bucket, output_loc)

    outputs_dicts = map(
        lambda x: {
            'complete': 1 if x['complete'] else 0,
            'incomplete': 0 if x['complete'] else 1,
            'zero': 1 if x.get('count', 0) == 0 else 0
        },
        catch_records_out_realized
    )
    output_dict = functools.reduce(
        lambda a, b: {
            'complete': a['complete'] + b['complete'],
            'incomplete': a['incomplete'] + b['incomplete'],
            'zero': a['zero'] + b['zero']
        },
        outputs_dicts
    )
    output_dict['loc'] = output_loc
    return output_dict


def get_hauls_meta(bucket):
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
    iterator = paginator.paginate(Bucket=bucket, Prefix='haul/')
    pages = filter(lambda x: 'Contents' in x, iterator)
    contents = map(lambda x: x['Contents'], pages)
    contents_flat = itertools.chain(*contents)
    keys = map(lambda x: x['Key'], contents_flat)
    return map(make_haul_metadata_record, keys)


def get_all_species(bucket):
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

    paginator = s3_client.get_paginator('list_objects_v2')
    iterator = paginator.paginate(Bucket=bucket, Prefix='species/')
    pages = filter(lambda x: 'Contents' in x, iterator)
    contents = map(lambda x: x['Contents'], pages)
    contents_flat = itertools.chain(*contents)
    keys = map(lambda x: x['Key'], contents_flat)
    records_nest = map(get_avro, keys)
    records_flat = itertools.chain(*records_nest)
    records_tuples = map(lambda x: (x['species_code'], x), records_flat)
    return dict(records_tuples)


def main():
    if len(sys.argv) != NUM_ARGS + 1:
        print(USAGE_STR)
        sys.exit(1)

    bucket = sys.argv[1]
    file_paths_loc = sys.argv[2]
    hauls_meta = get_hauls_meta(bucket)

    cluster = coiled.Cluster(
        name='DseProcessAfscgap',
        n_workers=10,
        worker_vm_types=['m7a.medium'],
        scheduler_vm_types=['m7a.medium'],
        environ={
            'AWS_ACCESS_KEY': os.environ.get('AWS_ACCESS_KEY', ''),
            'AWS_ACCESS_SECRET': os.environ.get('AWS_ACCESS_SECRET', ''),
            'SOURCE_DATA_LOC': os.environ.get('SOURCE_DATA_LOC', '')
        }
    )
    cluster.adapt(minimum=10, maximum=500)
    client = cluster.get_client()

    hauls_meta_realized = list(hauls_meta)
    species_by_code = get_all_species(bucket)

    written_paths_future = client.map(
        lambda x: process_haul(
            bucket,
            x['year'],
            x['survey'],
            x['haul'],
            species_by_code
        ),
        hauls_meta_realized
    )
    written_paths = map(lambda x: x.result(), written_paths_future)

    with open(file_paths_loc, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'loc',
            'complete',
            'incomplete',
            'zero'
        ])
        writer.writeheader()
        writer.writerows(written_paths)

    cluster.close(force_shutdown=True)


if __name__ == '__main__':
    main()
