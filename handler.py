from run_cumulus_task import run_cumulus_task
import granule_metadata_extractor.processing as mdx
import granule_metadata_extractor.src as src
from cumulus_process import Process, s3
from re import match
import os
import types
import boto3
import re


# simple task that returns the event


def extract_netcdf_metadata(ds_short_name, version, access_url, netcdf_file, netcdf_vars, output_folder='/tmp',
                            format='netCDF-4'):
    """
    Function to extract metadata from netCDF files
    :param ds_short_name: collection shortname
    :param version: version
    :param access_url: The access URL to the granule
    :param netcdf_file: Path to netCDF file
    :param netcdf_vars: netCDF variables
    :return:
    """
    regex = netcdf_vars.get('regex')
    switcher = {
        "goesrpltcrs": mdx.ExtractGeosrpltcrsMetadata,
        "gpmwrflpvex": mdx.ExtractLpvexwrfMetadata,
        "gpmseafluxicepop": mdx.ExtractGpmseafluxicepopMetadata,
        "uiucsndimpacts": mdx.ExtractUiucsndimpactsMetadata,
        "gpmvn": mdx.ExtractGpmvnMetadata,
        "noaasndimpacts": mdx.ExtractNoaasndimpactsMetadata,
        "kakqimpacts": mdx.ExtractKakqimpactsMetadata,
        "kccximpacts": mdx.ExtractKccximpactsMetadata,
        "kbgmimpacts": mdx.ExtractKbgmimpactsMetadata,
        "kboximpacts": mdx.ExtractKboximpactsMetadata,
        "kbufimpacts": mdx.ExtractKbufimpactsMetadata,
        "sbusndimpacts": mdx.ExtractSbusndimpactsMetadata,
        "nexeastimpacts": mdx.ExtractNexeastimpactsMetadata,
        "nexmidwstimpacts": mdx.ExtractNexmidwstimpactsMetadata
    }

    time_variable_key = netcdf_vars.get('time_var_key')
    lon_variable_key = netcdf_vars.get('lon_var_key')
    lat_variable_key = netcdf_vars.get('lat_var_key')
    time_units = netcdf_vars.get('time_units', 'units')

    if match(regex, os.path.basename(netcdf_file)):
        metadata = switcher.get(ds_short_name, src.ExtractNetCDFMetadata)(netcdf_file)
        format = 'netCDF-3' if '.nc' in netcdf_file else format
        data = metadata.get_metadata(ds_short_name=ds_short_name, time_variable_key=time_variable_key,
                                     lon_variable_key=lon_variable_key, lat_variable_key=lat_variable_key,
                                     time_units=time_units, format=format, version=version)

        data['OnlineAccessURL'] = access_url
        echo10xml = src.GenerateEcho10XML(data)
        echo10xml.generate_echo10_xml_file(output_folder=output_folder)


def extract_csv_metadata(ds_short_name, version, access_url, csv_file, csv_vars={}, output_folder='/tmp', format='CSV'):
    """
    Function to extract metadata from netCDF files
    :param ds_short_name: collection shortname
    :param version: version
    :param access_url: The access URL to the granule
    :param netcdf_file: Path to netCDF file
    :param netcdf_vars: CSV variables
    :return:
    """

    metadata = src.ExtractCSVMetadata(csv_file)

    time_position = csv_vars.get('time_row_position', 0)
    lon_postion = csv_vars.get('lon_row_position', 15)
    lat_postion = csv_vars.get('lat_row_position', 16)
    time_units = csv_vars.get('time_units', 'hours')
    regex = csv_vars.get('regex', '.*')

    if match(regex, os.path.basename(csv_file)):
        # def get_metadata(self, ds_short_name, time_postion=0, time_units="hours", lon_postion=15, date_format='%Y-%m-%dT%H:%M:%SZ',
        # lat_postion=16, format='CSV', version='1')
        data = metadata.get_metadata(ds_short_name=ds_short_name, time_position=time_position,
                                     time_units=time_units, lon_postion=lon_postion, lat_postion=lat_postion,
                                     format=format, version='0.9')

        data['OnlineAccessURL'] = access_url
        echo10xml = src.GenerateEcho10XML(data)
        echo10xml.generate_echo10_xml_file(output_folder=output_folder)


def extract_ascii_metadata(ds_short_name, version, access_url, ascii_file, ascii_vars={}, output_folder='/tmp',
                           format='ASCII'):
    """
    Function to extract metadata from ASCII files
    :param ds_short_name: collection shortname
    :param version: version
    :param access_url: The access URL to the granule
    :param netcdf_file: Path to netCDF file
    :param netcdf_vars: CSV variables
    :return:
    """

    switcher = {
        "gpmodmlpvex": mdx.ExtractGpmodmlpvexMetadata,
        "gpmjwlpvex": mdx.ExtractGpmjwlpvexMetadata,
        "gpmparawifld": mdx.ExtractGpmparawifldMetadata,
        "gpmpipicepop": mdx.ExtractGpmpipicepopASCIIMetadata,
        "namdrop_raw": mdx.ExtractNamdrop_rawMetadata,
        "gripstorm": mdx.ExtractGripstormASCIIMetadata,
        "relampagolma": mdx.ExtractLmarelampagoMetadata,
        "goesrpltavirisng": mdx.ExtractGoesrpltavirisngMetadata,
        "gpmvanlpvex": mdx.ExtractGpmvanlpvexMetadata,
        "gpmikalpvex": mdx.ExtractGpmikalpvexMetadata,
        "gpmkorlpvex": mdx.ExtractGpmkorlpvexMetadata,
        "gpmkerlpvex": mdx.ExtractGpmkerlpvexMetadata,
        "gpmkumlpvex": mdx.ExtractGpmkumlpvexMetadata,
        "gpm2dc3vp": mdx.ExtractGpm2dc3vpMetadata,
        "gpmlipiphx": mdx.ExtractGpmlipiphxASCIIMetadata,
        "misrepimpacts": mdx.ExtractMisrepimpactsMetadata,
        "2dimpacts": mdx.Extract2dimpactsMetadata
    }

    regex = ascii_vars.get('regex', '.*')

    if match(regex, os.path.basename(ascii_file)):
        metadata = switcher.get(ds_short_name, default_switch)(ascii_file)
        data = metadata.get_metadata(ds_short_name=ds_short_name, format=format, version=version)
        data['OnlineAccessURL'] = access_url
        echo10xml = src.GenerateEcho10XML(data)
        echo10xml.generate_echo10_xml_file(output_folder=output_folder)


def extract_kml_metadata(ds_short_name, version, access_url, kml_file, ascii_vars={}, output_folder='/tmp',
                         format='KML'):
    """
    Function to extract metadata from KML files
    :param ds_short_name: collection shortname
    :param version: version
    :param access_url: The access URL to the granule
    :param netcdf_file: Path to netCDF file
    :param netcdf_vars: CSV variables
    :return:
    """

    switcher = {
        "gpmsatpaifld": mdx.ExtractGpmsatpaifldMetadata,
        "gripstorm": mdx.ExtractGripstormKMLMetadata

    }


    regex = ascii_vars.get('regex', '.*')

    if match(regex, os.path.basename(kml_file)):
        metadata = switcher.get(ds_short_name, default_switch)(kml_file)
        data = metadata.get_metadata(ds_short_name=ds_short_name, format=format, version=version)
        data['OnlineAccessURL'] = access_url
        echo10xml = src.GenerateEcho10XML(data)
        echo10xml.generate_echo10_xml_file(output_folder=output_folder)


def extract_browse_metadata(ds_short_name, version, access_url, browse_file, browse_vars={},
                            output_folder='/tmp', format='BROWSE'):
    """
    Function to extract metadata from Browse files
    :param format:
    :param ds_short_name: collection shortname
    :param version: version
    :param access_url: The access URL to the granule
    :param browse_file: Path to Browse file
    :param browse_vars: Browse variables
    :return:
    """

    switcher = {
        "er2edop": mdx.ExtractEr2edopMetadata,
        "dc8lase": mdx.ExtractDc8laseMetadata,
        "gpmsatpaifld": mdx.ExtractGpmsatpaifldPNGMetadata,
        "gpmpipicepop": mdx.ExtractGpmpipicepopPNGMetadata,
        "aces1am": mdx.ExtractAces1amMetadata,
        "tcspecmwf": mdx.ExtractTcspecmwfMetadata,
        "er2mir": mdx.ExtractEr2mirMetadata,
        "dc8ammr": mdx.ExtractDc8ammrMetadata,
        "goesrpltmisrep": mdx.ExtractGoesrpltmisrepMetadata,
        "gpmlipiphx": mdx.ExtractGpmlipiphxPNGMetadata
    }

    format_template = {
        "er2edop": "GIF",
        "dc8lase": "GIF",
        "gpmsatpaifld": "PNG",
        "gpmpipicepop": "PNG",
        "aces1am": "MATLAB",
        "tcspecmwf": "GRIB",
        "er2mir": "GIF",
        "dc8ammr": "GIF",
        "goesrpltmisrep": "PNG",
        "gpmlipiphx": "PNG"
    }

    regex = browse_vars.get('regex', '.*')
    if match(regex, os.path.basename(browse_file)):
        metadata = switcher.get(ds_short_name, default_switch)(browse_file)
        data = metadata.get_metadata(ds_short_name=ds_short_name, format=format_template.get(ds_short_name, format), version=version)
        data['OnlineAccessURL'] = access_url
        echo10xml = src.GenerateEcho10XML(data)
        echo10xml.generate_echo10_xml_file(output_folder=output_folder)


def extract_avi_metadata(ds_short_name, version, access_url, browse_file, browse_vars={},
                         output_folder='/tmp', format='AVI'):
    """
    Function to extract metadata from Browse files
    :param format:
    :param ds_short_name: collection shortname
    :param version: version
    :param access_url: The access URL to the granule
    :param browse_file: Path to Browse file
    :param browse_vars: Browse variables
    :return:
    """

    switcher = {
        "gpmpipicepop": mdx.ExtractGpmpipicepopAVIMetadata
    }

    regex = browse_vars.get('regex', '.*')

    if match(regex, os.path.basename(browse_file)):
        metadata = switcher.get(ds_short_name, default_switch)(browse_file)
        data = metadata.get_metadata(ds_short_name=ds_short_name, format=format, version=version)
        data['OnlineAccessURL'] = access_url
        echo10xml = src.GenerateEcho10XML(data)
        echo10xml.generate_echo10_xml_file(output_folder=output_folder)


def upload_file(self, filename):
    info = self.get_publish_info(filename)
    if info is None:
        return filename
    try:
        uri = None
        if info.get('s3', None) is not None:
            uri = s3.upload(filename, info['s3'], extra={})
        return uri
    except Exception as e:
        self.logger.error("Error uploading file %s: %s" % (os.path.basename(os.path.basename(filename)), str(e)))


def get_file_staging_directory(config, ):
    """
    Comute staging files
    :return:
    """
    # TODO Implement filestaging function
    return "Tobe implemented"


def extract_metadata(file_path, config, output_folder):
    """

    :param netcdf_file:
    :return:
    """
    collection = config.get('collection')
    buckets = config.get('buckets')
    protected_bucket = buckets.get('protected').get('name')
    ds_short_name = collection.get('name')
    version = collection.get('version')
    metadata_extractor_vars = collection.get('meta', {}).get('metadata_extractor', [])
    access_url = os.path.join(config.get('distribution_endpoint'), protected_bucket, config['fileStagingDir'],
                              os.path.basename(file_path))
    processing_switcher = {
        "netcdf": extract_netcdf_metadata,
        "csv": extract_csv_metadata,
        "ascii": extract_ascii_metadata,
        "browse": extract_browse_metadata,
        "kml": extract_kml_metadata,
        "avi": extract_avi_metadata
    }

    for metadata_extractor_var in metadata_extractor_vars:
        processing_switcher.get(metadata_extractor_var.get('module'),
                                default_switch)(ds_short_name, version, access_url, file_path, metadata_extractor_var,
                                                output_folder)


def get_bucket(filename, files, buckets):
    """
    Extract the bucket from the files
    :param filename: Granule file name
    :param files: list of collection files
    :param buckets: Object holding buckets info
    :return: Bucket object
    """
    bucket_type = "public"
    for file in files:
        if match(file.get('regex', '*.'), filename):
            bucket_type = file['bucket']
            break
    return buckets[bucket_type]

def exclude_fetch():
    """
    This function is to exclude fetching the granules from
    specific shortnames

    :return:
    """
    return ["tcspecmwf", "gpmwrflpvex", "relampagolma", "goesrpltavirisng", "gpmvanlpvex", "gpmikalpvex", "gpmkorlpvex", "gpmkerlpvex", "gpmkumlpvex",
            "gpmseafluxicepop","kakqimpacts","kccximpacts","kbgmimpacts","kboximpacts","kbufimpacts"]


def mutate_input(output_folder, input_file):
    """
    Point the input to local folder instead of S3
    :param input_file: The input file from S3
    :return: path to the file in local machine
    """
    filename = os.path.basename(input_file)

    return [f"{output_folder.rstrip('/')}/{filename}"]


def task(event, context, temp_path=None):
    """

    :param event:
    :param context:
    :return:
    """
    print(event)
    input = event.get('input', [])
    config = event.get('config')
    collection = config.get('collection')
    collection_name = collection.get('name')
    config['fileStagingDir'] = config.get('fileStagingDir', f"{collection_name}__{collection['version']}").strip('/')
    key = 'input_key'
    buckets = config['buckets']
    config['input_keys'] = {key: r'^.*.(nc|tsv|txt|gif|tar|zip|png|kml|dat|gz|pdf|docx|kmz|xlsx)$'}
    process = Process(input=input, config=config, path=temp_path)
    # Replace the upload_file function
    process.upload_file = types.MethodType(upload_file, process)
    excluded = collection_name in exclude_fetch()
    if excluded:
        output = {'input_key': mutate_input(process.path, input[0])}
        s3_client = boto3.resource('s3')
        source_bucket = config.get('buckets').get('internal').get('name')
        copy_source = {
            'Bucket': source_bucket,
            'Key': re.search(f'^s3://{source_bucket}/(.*)', input[0])[1]
        }
        bucket = s3_client.Bucket(config.get('buckets').get('protected').get('name'))
        bucket.copy(copy_source, f"{config['fileStagingDir']}/{re.search('(.*)/(.*)$', input[0])[2]}")
    else:
        output = process.fetch_all()
    files_sizes = {}
    for output_file_path in output.get(key):
        extract_metadata(file_path=output_file_path, config=config, output_folder=process.path)
        generated_files = [output_file_path + ".cmr.xml"] if excluded else [output_file_path, output_file_path + ".cmr.xml"]
        for generated_file in generated_files:
            files_sizes[generated_file.split('/')[-1]] = os.path.getsize(generated_file)
        process.output += generated_files
    uploaded_files = process.upload_output_files()
    granule_data = {}
    for uploaded_file in uploaded_files:
        if uploaded_file is None or not uploaded_file.startswith('s3'):
            continue
        filename = uploaded_file.split('/')[-1]
        granule_id = filename.split('.cmr.xml')[0]
        if granule_id not in granule_data.keys():
            granule_data[granule_id] = {'granuleId': granule_id , 'files': []}
        granule_data[granule_id]['files'].append(
            {
                "path": config['fileStagingDir'],
                "url_path": config.get('url_path', config['fileStagingDir']),
                "bucket": get_bucket(filename, collection.get('files', []), buckets)['name'],
                "fileName": filename, # Cumulus changed the key name to be camelCase
                "filename": uploaded_file, # We still need to provide some custom steps with
                # this key holding the object URI
                "size": files_sizes[filename]
        }
    )
      
    final_output = list(granule_data.values())

    process.clean_all()

    return {"granules": final_output, "input": uploaded_files}


# handler that is provided to aws lambda


def default_switch(self, *args):
    """
    This is a default switch to pass
    *args is an arbitrary argument
    :return:
    """
    pass


def handler(event, context):
    return run_cumulus_task(task, event, context)


if __name__ == '__main__':
    event = {
   "input":[
      "s3://ghrcsbxw-internal/file-staging/ghrcsbxw/2dimpacts__1/impacts_2dvd_sn25_raintotalhour.txt"
   ],
   "config":{
      "files_config":[
         {
            "bucket":"public",
            "regex":"^impacts_2dvd_(.*).*\\.cmr.xml$",
            "sampleFileName":"impacts_2dvd_sn37_raindsd.txt.cmr.xml"
         },
         {
            "bucket":"protected",
            "regex":"^impacts_2dvd_(.*).*(txt)$",
            "sampleFileName":"impacts_2dvd_sn37_raindsd.txt"
         }
      ],
      "buckets":{
         "protected":{
            "type":"protected",
            "name":"ghrcsbxw-protected"
         },
         "internal":{
            "type":"internal",
            "name":"ghrcsbxw-internal"
         },
         "private":{
            "type":"private",
            "name":"ghrcsbxw-private"
         },
         "public":{
            "type":"public",
            "name":"ghrcsbxw-public"
         }
      },
      "collection":{
         "name":"2dimpacts",
         "version":"1",
         "dataType":"2dimpacts",
         "process":"metadataextractor",
         "url_path":"2dimpacts__1",
         "duplicateHandling":"replace",
         "granuleId":"^impacts_2dvd_.*\\.(txt)$",
         "granuleIdExtraction":"^((impacts_2dvd_).*)",
         "reportToEms": True,
         "sampleFileName":"impacts_2dvd_sn37_raindsd.txt",
         "meta":{
            "provider_path":"2dimpacts/fieldCampaigns/impacts/2DVD/data/",
            "hyrax_processing":"false",
            "metadata_extractor":[
               {
                  "regex":"^(.*).*\\.(txt)$",
                  "module":"ascii"
               }
            ]
         },
         "files":[
            {
               "bucket":"public",
               "regex":"^impacts_2dvd_(.*).*\\.cmr.xml$",
               "sampleFileName":"impacts_2dvd_sn37_raindsd.txt.cmr.xml"
            },
            {
               "bucket":"protected",
               "regex":"^impacts_2dvd_(.*).*(txt)$",
               "sampleFileName":"impacts_2dvd_sn37_raindsd.txt"
            }
         ]
      },
   "distribution_endpoint":"https://y90y21dcf1.execute-api.us-west-2.amazonaws.com/dev/"
   }
}

    context = []
    task(event, context)
