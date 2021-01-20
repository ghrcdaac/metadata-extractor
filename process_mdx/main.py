import granule_metadata_extractor.processing as mdx
import granule_metadata_extractor.src as src
from cumulus_process import Process, s3
from re import match
import os
import boto3
import re


class MDX(Process):
    """
    Class to extract spatial and temporal metadata
    """
    @staticmethod
    def generate_xml_data(data, access_url,output_folder, granule_new_name = None):
        """

        """
        if granule_new_name:
            access_url = access_url.replace(os.path.basename(access_url), os.path.basename(granule_new_name))
        data['OnlineAccessURL'] = access_url
        echo10xml = src.GenerateEcho10XML(data)
        echo10xml.generate_echo10_xml_file(output_folder=output_folder)
        return data


    def extract_netcdf_metadata(self, ds_short_name, version, access_url, netcdf_file, netcdf_vars,
                                output_folder='/tmp', format='netCDF-4'):
        """
        Function to extract metadata from netCDF files
        :param ds_short_name: collection shortname
        :param version: version
        :param access_url: The access URL to the granule
        :param netcdf_file: Path to netCDF file
        :param netcdf_vars: netCDF variables
        :param output_folder: Location to output created echo10xml file
        :param format: data type of input file
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
            "nexmidwstimpacts": mdx.ExtractNexmidwstimpactsMetadata,
            "ncsusndimpacts": mdx.ExtractNcsusndimpactsMetadata,
            "goesimpacts": mdx.ExtractGoesimpactsMetadata,
            "amsua15sp": mdx.ExtractAMSUAMetadata,
            "npolimpacts": mdx.ExtractNpolimpactsMetadata,
            "sbuceilimpacts": mdx.ExtractSbuceilimpactsMetadata,
            "sbulidarimpacts": mdx.ExtractSbulidarimpactsMetadata,
            "sbukasprimpacts": mdx.ExtractSbukasprimpactsMetadata,
            "sbumrr2impacts": mdx.ExtractSbumrr2impactsMetadata,
            "sbumetimpacts": mdx.ExtractSbumetimpactsNetCDFMetadata,
            "rss1tpwnv7r01": mdx.ExtractRssClimatologyMetadata,
            "rss1windnv7r01": mdx.ExtractRssClimatologyMetadata,
            "sbuparsimpacts": mdx.ExtractSbuparsimpactsMetadata,
            "cosmirimpacts": mdx.ExtractCosmirimpactsMetadata,
            "cplimpacts": mdx.ExtractCplimpactsMetadata,
            "parprbimpacts": mdx.ExtractParprbimpactsMetadata,
            "isslis_v1_nrt": mdx.ExtractIsslisv1Metadata,
            "isslis_v1_nqc": mdx.ExtractIsslisv1Metadata,
            "isslisg_v1_nrt": mdx.ExtractIsslisgv1Metadata,
            "isslisg_v1_nqc": mdx.ExtractIsslisgv1Metadata,
            "seaflux": mdx.ExtractSeafluxMetadata,
            "asosimpacts": mdx.ExtractAsosimpactsMetadata,
            "wrfimpacts": mdx.ExtractWrfimpactsMetadata,
            "hs3shis": mdx.ExtractHs3shisMetadata,
            "hiwrapimpacts": mdx.ExtractHiwrapimpactsMetadata,
            "gpmwacrc3vp": mdx.ExtractGpmwacrc3vpMetadata,
            "crsimpacts": mdx.ExtractCrsimpactsMetadata
        }

        time_variable_key = netcdf_vars.get('time_var_key')
        lon_variable_key = netcdf_vars.get('lon_var_key')
        lat_variable_key = netcdf_vars.get('lat_var_key')
        time_units = netcdf_vars.get('time_units', 'units')

        if match(regex, os.path.basename(netcdf_file)):
            metadata = switcher.get(ds_short_name, src.ExtractNetCDFMetadata)(netcdf_file)
            format = 'netCDF-3' if '.nc4' not in netcdf_file else format
            data = metadata.get_metadata(ds_short_name=ds_short_name,
                                         time_variable_key=time_variable_key,
                                         lon_variable_key=lon_variable_key,
                                         lat_variable_key=lat_variable_key,
                                         time_units=time_units, format=format, version=version)
            return MDX.generate_xml_data(data=data, access_url=access_url, output_folder=output_folder)
        return {}

    def extract_csv_metadata(self, ds_short_name, version, access_url, csv_file, csv_vars={},
                             output_folder='/tmp', format='CSV'):
        """
        Function to extract metadata from netCDF files
        :param ds_short_name: collection shortname
        :param version: version
        :param access_url: The access URL to the granule
        :param csv_file: Path to csv file
        :param csv_vars: Variables within csv file
        :param output_folder: Location to output created echo10xml file
        :param format: data type of input file
        :return:
        """

        metadata = src.ExtractCSVMetadata(csv_file)

        time_position = csv_vars.get('time_row_position', 0)
        lon_postion = csv_vars.get('lon_row_position', 15)
        lat_postion = csv_vars.get('lat_row_position', 16)
        time_units = csv_vars.get('time_units', 'hours')
        regex = csv_vars.get('regex', '.*')

        if match(regex, os.path.basename(csv_file)):
            data = metadata.get_metadata(ds_short_name=ds_short_name, time_position=time_position,
                                         time_units=time_units, lon_postion=lon_postion,
                                         lat_postion=lat_postion,
                                         format=format, version='0.9')
            return MDX.generate_xml_data(data=data, access_url=access_url, output_folder=output_folder)
        return {}

    def extract_binary_metadata(self, ds_short_name, version, access_url, binary_file, binary_vars={},
                               output_folder='/tmp', format='Binary'):
        """

        """
        switcher = {
            "globalir": mdx.ExtractGlobalirMetadata
        }
        regex = binary_vars.get('regex', '.*')
        if match(regex, os.path.basename(binary_file)):
            metadata = switcher.get(ds_short_name, self.default_switch)(binary_file)
            data = metadata.get_metadata(ds_short_name=ds_short_name, version=version, format=format)
            granule_new_name = data.get('UpdatedGranuleUR', None)
            return MDX.generate_xml_data(data=data, access_url=access_url, output_folder=output_folder, granule_new_name=granule_new_name)
        return {}

    def extract_ascii_metadata(self, ds_short_name, version, access_url, ascii_file, ascii_vars={},
                               output_folder='/tmp', format='ASCII'):
        """
        Function to extract metadata from ASCII files
        :param ds_short_name: collection shortname
        :param version: version
        :param access_url: The access URL to the granule
        :param ascii_file: Path to ascii file
        :param ascii_vars: ASCII variables
        :param output_folder: Location to output created echo10xml file
        :param format: data type of input file
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
            "2dimpacts": mdx.Extract2dimpactsMetadata,
            "apuimpacts": mdx.ExtractApuimpactsMetadata,
            "sbumetimpacts": mdx.ExtractSbumetimpactsASCIIMetadata,
            "sbuplimpacts": mdx.ExtractSbuplimpactsMetadata,
            "er2navimpacts": mdx.ExtractEr2navimpactsMetadata,
            "nalma": mdx.ExtractNalmaMetadata,
            "nalmanrt": mdx.ExtractNalmanrtMetadata,
            "nalmaraw": mdx.ExtractNalmarawMetadata,
            "p3metnavimpacts": mdx.ExtractP3metnavimpactsMetadata,
            "tammsimpacts": mdx.ExtractTammsimpactsMetadata,
            "lipimpacts": mdx.ExtractLipimpactsMetadata,
            "gpmsurmetc3vp": mdx.ExtractGpmsurmetc3vpMetadata,
            "gpmarsifld": mdx.ExtractGpmarsifldMetadata,
            "gpmvisecc3vp": mdx.ExtractGpmvisecc3vpMetadata
        }

        regex = ascii_vars.get('regex', '.*')

        if match(regex, os.path.basename(ascii_file)):
            metadata = switcher.get(ds_short_name, self.default_switch)(ascii_file)
            data = metadata.get_metadata(ds_short_name=ds_short_name, format=format,
                                         version=version)
            return MDX.generate_xml_data(data=data, access_url=access_url, output_folder=output_folder)
        return {}

    def extract_kml_metadata(self, ds_short_name, version, access_url, kml_file, ascii_vars={},
                             output_folder='/tmp', format='KML'):
        """
        Function to extract metadata from KML files
        :param ds_short_name: collection shortname
        :param version: version
        :param access_url: The access URL to the granule
        :param kml_file: Path to kml file
        :param ascii_vars: ascii variables
        :param output_folder: Location to output created echo10xml file
        :param format: data type of input file
        :return:
        """

        switcher = {
            "gpmsatpaifld": mdx.ExtractGpmsatpaifldMetadata,
            "gripstorm": mdx.ExtractGripstormKMLMetadata
        }

        regex = ascii_vars.get('regex', '.*')

        if match(regex, os.path.basename(kml_file)):
            metadata = switcher.get(ds_short_name, self.default_switch)(kml_file)
            data = metadata.get_metadata(ds_short_name=ds_short_name, format=format,
                                         version=version)
            return MDX.generate_xml_data(data=data, access_url=access_url, output_folder=output_folder)
        return {}

    def extract_browse_metadata(self, ds_short_name, version, access_url, browse_file,
                                browse_vars={}, output_folder='/tmp', format='BROWSE'):
        """
        Function to extract metadata from Browse files
        :param format:
        :param ds_short_name: collection shortname
        :param version: version
        :param access_url: The access URL to the granule
        :param browse_file: Path to Browse file
        :param browse_vars: Browse variables
        :param output_folder: Location to output created echo10xml file
        :param format: data type of input file
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
            "gpmlipiphx": mdx.ExtractGpmlipiphxPNGMetadata,
            "phipsimpacts": mdx.ExtractPhipsimpactsMetadata,
            "nymesoimpacts": mdx.ExtractNymesoimpactsMetadata
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
            "gpmlipiphx": "PNG",
            "phipsimpacts": "PNG",
            "nymesoimpacts": "PNG"
        }

        regex = browse_vars.get('regex', '.*')
        if match(regex, os.path.basename(browse_file)):
            metadata = switcher.get(ds_short_name, self.default_switch)(browse_file)
            data = metadata.get_metadata(ds_short_name=ds_short_name,
                                         format=format_template.get(ds_short_name, format),
                                         version=version)
            return MDX.generate_xml_data(data=data, access_url=access_url, output_folder=output_folder)
        return {}

    def extract_avi_metadata(self, ds_short_name, version, access_url, browse_file, browse_vars={},
                             output_folder='/tmp', format='AVI'):
        """
        Function to extract metadata from Browse files
        :param format:
        :param ds_short_name: collection shortname
        :param version: version
        :param access_url: The access URL to the granule
        :param browse_file: Path to Browse file
        :param browse_vars: Browse variables
        :param output_folder: Location to output created echo10xml file
        :param format: data type of input file
        :return:
        """

        switcher = {
            "gpmpipicepop": mdx.ExtractGpmpipicepopAVIMetadata
        }

        regex = browse_vars.get('regex', '.*')

        if match(regex, os.path.basename(browse_file)):
            metadata = switcher.get(ds_short_name, self.default_switch)(browse_file)
            data = metadata.get_metadata(ds_short_name=ds_short_name, format=format,
                                         version=version)
            return MDX.generate_xml_data(data=data, access_url=access_url, output_folder=output_folder)
        return {}

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
            self.logger.error("Error uploading file %s: %s" % (
                os.path.basename(os.path.basename(filename)), str(e)))

    def extract_metadata(self, file_path, config, output_folder):
        """
        High-level extract metadata from file
        :param file_path: Path to file
        :param config: Config passed in through aws step function
        :param output_folder: Location to output created echo10xml file
        :return:
        """
        collection = config.get('collection')
        buckets = config.get('buckets')
        protected_bucket = buckets.get('protected').get('name')
        ds_short_name = collection.get('name')
        version = collection.get('version')
        metadata_extractor_vars = collection.get('meta', {}).get('metadata_extractor', [])
        access_url = os.path.join(config.get('distribution_endpoint'), protected_bucket,
                                  config['fileStagingDir'],
                                  os.path.basename(file_path))
        processing_switcher = {
            "netcdf": self.extract_netcdf_metadata,
            "csv": self.extract_csv_metadata,
            "binary": self.extract_binary_metadata,
            "ascii": self.extract_ascii_metadata,
            "browse": self.extract_browse_metadata,
            "kml": self.extract_kml_metadata,
            "avi": self.extract_avi_metadata
        }

        for metadata_extractor_var in metadata_extractor_vars:
            return processing_switcher.get(metadata_extractor_var.get('module'),
                                           self.default_switch)(ds_short_name, version, access_url,
                                                                file_path, metadata_extractor_var,
                                                                output_folder)

    def get_bucket(self, filename, files, buckets):
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

    def exclude_fetch(self):
        """
        This function is to exclude fetching the granules from specific shortnames
        :return:
        """
        return ["tcspecmwf", "gpmwrflpvex", "relampagolma", "goesrpltavirisng", "gpmvanlpvex",
                "gpmikalpvex", "gpmkorlpvex", "gpmkerlpvex", "gpmkumlpvex", "gpmseafluxicepop",
                "kakqimpacts", "kccximpacts", "kbgmimpacts", "kboximpacts", "kbufimpacts"]

    def mutate_input(self, output_folder, input_file):
        """
        Point the input to local folder instead of S3
        :param input_file: The input file from S3
        :param output_folder: The local folder to ouput file to
        :return: path to the file in local machine
        """
        filename = os.path.basename(input_file)
        return [f"{output_folder.rstrip('/')}/{filename}"]

    @property
    def input_keys(self):
        return {
            'input_key': r'^(.*)\.(nc|tsv|txt|gif|tar|zip|png|kml|dat|gz|pdf|docx|kmz|xlsx|eos|csv'
                         r'|hdf5|hdf|nc4|ict|xls|.*rest|h5|xlsx)$'
        }

    def get_output_files(self, output_file_path, excluded):
        """
        """
        output_files = [] if excluded else [output_file_path]
        if os.path.isfile(output_file_path + ".cmr.xml"):
            output_files += [output_file_path + ".cmr.xml"]
        return output_files

    def process(self):
        """
        Override the processing wrapper
        :return:
        """
        key = 'input_key'
        collection = self.config.get('collection')
        collection_name = collection.get('name')
        collection_version = collection.get('version')
        buckets = self.config.get('buckets')
        self.config['fileStagingDir'] = None if 'fileStagingDir' not in self.config.keys() else \
            self.config['fileStagingDir']
        self.config['fileStagingDir'] = f"{collection_name}__{collection_version}" if \
            self.config['fileStagingDir'] is None else self.config['fileStagingDir']
        url_path = collection.get('url_path', self.config['fileStagingDir'])

        excluded = collection_name in self.exclude_fetch()
        if excluded:
            output = {key: self.mutate_input(self.path, self.input[0])}
            s3_client = boto3.resource('s3')
            source_bucket = buckets.get('internal').get('name')
            copy_source = {
                'Bucket': source_bucket,
                'Key': re.search(f'^s3://{source_bucket}/(.*)', self.input[0])[1]
            }
            bucket = s3_client.Bucket(buckets.get('protected').get('name'))
            bucket.copy(copy_source,
                        f"{url_path}/{re.search('(.*)/(.*)$', input[0])[2]}")
        else:
            output = self.fetch_all()
        # Assert we have inputs to process
        assert output[key], "fetched files list should not be empty"
        files_sizes = {}
        for output_file_path in output.get(key):
            data = self.extract_metadata(file_path=output_file_path, config=self.config,
                                         output_folder=self.path)
            generated_files = self.get_output_files(output_file_path, excluded)
            if data.get('UpdatedGranuleUR', False):
                updated_output_path = self.get_output_files(os.path.join(self.path,
                                                                         data['UpdatedGranuleUR']),
                                                            excluded)
                generated_files.extend(updated_output_path)
            for generated_file in generated_files:
                files_sizes[generated_file.split('/')[-1]] = os.path.getsize(generated_file)
            self.output += generated_files
        uploaded_files = self.upload_output_files()
        granule_data = {}
        for uploaded_file in uploaded_files:
            if uploaded_file is None or not uploaded_file.startswith('s3'):
                continue
            filename = uploaded_file.split('/')[-1]
            granule_id = filename.split('.cmr.xml')[0]
            if granule_id not in granule_data.keys():
                granule_data[granule_id] = {'granuleId': granule_id, 'files': []}
            granule_data[granule_id]['files'].append(
                {
                    "path": self.config['fileStagingDir'],
                    "url_path": url_path,
                    "bucket": self.get_bucket(filename, collection.get('files', []),
                                              buckets)['name'],
                    "fileName": filename,  # Cumulus changed the key name to be camelCase
                    "filename": uploaded_file,  # We still need to provide some custom steps with
                    # this key holding the object URI
                    "size": files_sizes.get(filename, 0)
                }
            )
        final_output = list(granule_data.values())
        # Clean up
        for generated_file in self.output:
            os.remove(generated_file)

        return {"granules": final_output, "input": uploaded_files}

    def default_switch(self, *args):
        """
        This is a default switch to pass
        *args is an arbitrary argument
        :return:
        """
        pass


if __name__ == '__main__':
    MDX.cli()
