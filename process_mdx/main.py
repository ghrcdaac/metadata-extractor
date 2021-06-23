import granule_metadata_extractor.processing as mdx
import granule_metadata_extractor.src as src
from cumulus_process import Process, s3
from re import match
import os
import boto3
import json


class MDX(Process):
    """
    Class to extract spatial and temporal metadata
    """

    def generate_xml_data(self, data, access_url, output_folder):
        """
        Generates echo10xml file from python dict
        :param data: Python dict containing metadata information of granule
        :param access_url: S3 location where granule can be downloaded from
        :param output_folder: Local location where echo10xml file will be stored
        :return: input dict
        """

        granule_new_name = data.get('UpdatedGranuleUR', None)
        if granule_new_name:
            access_url = access_url.replace(os.path.basename(access_url),
                                            os.path.basename(granule_new_name))
        data['OnlineAccessURL'] = access_url
        echo10xml = src.GenerateEcho10XML(data,
                                          age_off=self.config.get('collection',
                                                                  {}).get('meta',
                                                                          {}).get('age-off', None))
        echo10xml.generate_echo10_xml_file(output_folder=output_folder)
        return data

    def extract_netcdf_metadata(self, ds_short_name, version, access_url, netcdf_file, netcdf_vars,
                                output_folder='/tmp', file_format='netCDF-4'):
        """
        Function to extract metadata from netCDF files
        :param ds_short_name: collection shortname
        :param version: version
        :param access_url: The access URL to the granule
        :param netcdf_file: Path to netCDF file
        :param netcdf_vars: netCDF variables
        :param output_folder: Location to output created echo10xml file
        :param file_format: data type of input file
        :return:
        """
        regex = netcdf_vars.get('regex')
        switcher = self.read_switcher_json("netcdf")

        time_variable_key = netcdf_vars.get('time_var_key')
        lon_variable_key = netcdf_vars.get('lon_var_key')
        lat_variable_key = netcdf_vars.get('lat_var_key')
        time_units = netcdf_vars.get('time_units', 'units')

        file_format = 'netCDF-3' if '.nc4' not in netcdf_file else file_format

        if match(regex, os.path.basename(netcdf_file)):
            # TODO- Future optimization to have better error handling here with default switcher
            metadata = getattr(mdx, switcher.get(ds_short_name, "None"))(netcdf_file)
            data = metadata.get_metadata(ds_short_name=ds_short_name,
                                         time_variable_key=time_variable_key,
                                         lon_variable_key=lon_variable_key,
                                         lat_variable_key=lat_variable_key,
                                         time_units=time_units, format=file_format,
                                         version=version)
            return MDX.generate_xml_data(self, data=data, access_url=access_url,
                                         output_folder=output_folder)
        return {}

    def extract_csv_metadata(self, ds_short_name, version, access_url, csv_file, csv_vars=None,
                             output_folder='/tmp', file_format='CSV'):
        """
        Function to extract metadata from netCDF files
        :param ds_short_name: collection shortname
        :param version: version
        :param access_url: The access URL to the granule
        :param csv_file: Path to csv file
        :param csv_vars: Variables within csv file
        :param output_folder: Location to output created echo10xml file
        :param file_format: data type of input file
        :return:
        """
        csv_vars = csv_vars or {}
        metadata = getattr(src, "ExtractCSVMetadata")(csv_file)

        time_position = csv_vars.get('time_row_position', 0)
        lon_postion = csv_vars.get('lon_row_position', 15)
        lat_postion = csv_vars.get('lat_row_position', 16)
        time_units = csv_vars.get('time_units', 'hours')
        regex = csv_vars.get('regex', '.*')

        if match(regex, os.path.basename(csv_file)):
            data = metadata.get_metadata(ds_short_name=ds_short_name, time_position=time_position,
                                         time_units=time_units, lon_postion=lon_postion,
                                         lat_postion=lat_postion,
                                         format=file_format, version=version)
            return MDX.generate_xml_data(self, data=data, access_url=access_url,
                                         output_folder=output_folder)
        return {}

    def extract_binary_metadata(self, ds_short_name, version, access_url, binary_file,
                                binary_vars=None, output_folder='/tmp', file_format='Binary'):
        """
        Function to extract metadata from binary files
        :param ds_short_name: collection shortname
        :param version: collection version
        :param access_url: The access URL to the granule
        :param binary_file: Path to binary file
        :param binary_vars: binary variables
        :param output_folder: Location to output created echo10xml file
        :param file_format: data type of input file
        :return:
        """
        binary_vars = binary_vars or {}
        switcher = self.read_switcher_json("binary")
        regex = binary_vars.get('regex', '.*')
        if match(regex, os.path.basename(binary_file)):
            metadata = getattr(mdx, switcher.get(ds_short_name, "None"))(binary_file)
            data = metadata.get_metadata(ds_short_name=ds_short_name, version=version,
                                         format=file_format)
            return MDX.generate_xml_data(self, data=data, access_url=access_url,
                                         output_folder=output_folder)
        return {}

    def extract_ascii_metadata(self, ds_short_name, version, access_url, ascii_file,
                               ascii_vars=None, output_folder='/tmp', file_format='ASCII'):
        """
        Function to extract metadata from ASCII files
        :param ds_short_name: collection shortname
        :param version: version
        :param access_url: The access URL to the granule
        :param ascii_file: Path to ascii file
        :param ascii_vars: ASCII variables
        :param output_folder: Location to output created echo10xml file
        :param file_format: data type of input file
        :return:
        """
        ascii_vars = ascii_vars or {}
        switcher = self.read_switcher_json("ascii")

        regex = ascii_vars.get('regex', '.*')

        if match(regex, os.path.basename(ascii_file)):
            metadata = getattr(mdx, switcher.get(ds_short_name, "None"))(ascii_file)
            data = metadata.get_metadata(ds_short_name=ds_short_name, format=file_format,
                                         version=version)
            return MDX.generate_xml_data(self, data=data, access_url=access_url,
                                         output_folder=output_folder)
        return {}

    def extract_kml_metadata(self, ds_short_name, version, access_url, kml_file, kml_vars=None,
                             output_folder='/tmp', file_format='KML'):
        """
        Function to extract metadata from KML files
        :param ds_short_name: collection shortname
        :param version: version
        :param access_url: The access URL to the granule
        :param kml_file: Path to kml file
        :param kml_vars: kml variables
        :param output_folder: Location to output created echo10xml file
        :param file_format: data type of input file
        :return:
        """
        kml_vars = kml_vars or {}
        switcher = self.read_switcher_json("kml")

        regex = kml_vars.get('regex', '.*')

        if match(regex, os.path.basename(kml_file)):
            metadata = getattr(mdx, switcher.get(ds_short_name, "None"))(kml_file)
            data = metadata.get_metadata(ds_short_name=ds_short_name, format=file_format,
                                         version=version)
            return MDX.generate_xml_data(self, data=data, access_url=access_url,
                                         output_folder=output_folder)
        return {}

    def extract_browse_metadata(self, ds_short_name, version, access_url, browse_file,
                                browse_vars=None, output_folder='/tmp', file_format='BROWSE'):
        """
        Function to extract metadata from Browse files
        :param ds_short_name: collection shortname
        :param version: version
        :param access_url: The access URL to the granule
        :param browse_file: Path to Browse file
        :param browse_vars: Browse variables
        :param output_folder: Location to output created echo10xml file
        :param file_format: data type of input file
        :return:
        """
        browse_vars = browse_vars or {}
        switcher = self.read_switcher_json("browse_process")
        format_template = self.read_switcher_json("browse_format")

        regex = browse_vars.get('regex', '.*')
        if match(regex, os.path.basename(browse_file)):
            metadata = getattr(mdx, switcher.get(ds_short_name, "None"))(browse_file)
            data = metadata.get_metadata(ds_short_name=ds_short_name,
                                         format=format_template.get(ds_short_name, file_format),
                                         version=version)
            return MDX.generate_xml_data(self, data=data, access_url=access_url,
                                         output_folder=output_folder)
        return {}

    def extract_avi_metadata(self, ds_short_name, version, access_url, avi_file, avi_vars=None,
                             output_folder='/tmp', file_format='AVI'):
        """
        Function to extract metadata from Browse files
        :param ds_short_name: collection shortname
        :param version: version
        :param access_url: The access URL to the granule
        :param avi_file: Path to AVI file
        :param avi_vars: AVI variables
        :param output_folder: Location to output created echo10xml file
        :param file_format: data type of input file
        :return:
        """
        avi_vars = avi_vars or {}
        switcher = self.read_switcher_json("avi")

        regex = avi_vars.get('regex', '.*')

        if match(regex, os.path.basename(avi_file)):
            metadata = getattr(mdx, switcher.get(ds_short_name, "None"))(avi_file)
            data = metadata.get_metadata(ds_short_name=ds_short_name, format=file_format,
                                         version=version)
            return MDX.generate_xml_data(self, data=data, access_url=access_url,
                                         output_folder=output_folder)
        return {}

    def extract_legacy_metadata(self, ds_short_name, version, access_url, legacy_file,
                                legacy_vars=None, output_folder='/tmp', file_format='ASCII'):
        """
        Function to extract metadata from legacy dataset files
        :param ds_short_name: collection shortname
        :param version: version
        :param access_url: The access URL to the granule
        :param legacy_file: Path to legacy file
        :param legacy_vars: legacy variables
        :param output_folder: Location to output created echo10xml file
        :param file_format: data type of input file
        :return:
        """
        legacy_vars = legacy_vars or {}
        regex = legacy_vars.get('regex', '.*')

        if match(regex, os.path.basename(legacy_file)):
            metadata = getattr(mdx, "ExtractLegacyMetadata")(legacy_file)
            data = metadata.get_metadata(ds_short_name=ds_short_name, format=file_format,
                                         version=version)
            return MDX.generate_xml_data(self, data=data, access_url=access_url,
                                         output_folder=output_folder)
        return {}

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
            "avi": self.extract_avi_metadata,
            "legacy": self.extract_legacy_metadata
        }

        return_data_dict = {}
        for metadata_extractor_var in metadata_extractor_vars:
            data = processing_switcher.get(metadata_extractor_var.get('module'),
                                           self.default_switch)(ds_short_name, version, access_url,
                                                                file_path, metadata_extractor_var,
                                                                output_folder)
            return_data_dict = data if data else return_data_dict
        return return_data_dict

    @staticmethod
    def exclude_fetch():
        """
        This function is to exclude fetching the granules from specific shortnames
        :return:
        """
        return ["tcspecmwf", "gpmwrflpvex", "relampagolma", "goesrpltavirisng", "gpmvanlpvex",
                "gpmikalpvex", "gpmkorlpvex", "gpmkerlpvex", "gpmkumlpvex", "gpmseafluxicepop",
                "kakqimpacts", "kccximpacts", "kbgmimpacts", "kboximpacts", "kbufimpacts"]

    @staticmethod
    def mutate_input(output_folder, input_file):
        """
        Point the input to local folder instead of S3
        :param input_file: The input file from S3
        :param output_folder: The local folder to ouput file to
        :return: path to the file in local machine
        """
        filename = os.path.basename(input_file)
        return [f"{output_folder.rstrip('/')}/{filename}"]

    def upload_output_files(self):
        """
        Uploads all self.output files to same location as input file
        :return: list of files uploaded to S3 (as well as input which should already be in S3)
        """
        upload_output_list = list()
        source_path = os.path.dirname(self.input[0])
        for output_file in self.output:
            output_filename = os.path.basename(output_file)
            uri_out = os.path.join(source_path, output_filename)
            upload_output_list.append(uri_out)
            if output_filename is not os.path.basename(self.input[0]):
                try:
                    uri_out_info = s3.uri_parser(uri_out)
                    s3_client = boto3.resource('s3').Bucket(uri_out_info["bucket"]).\
                        Object(uri_out_info['key'])
                    with open(output_file, 'rb') as data:
                        s3_client.upload_fileobj(data)
                except Exception as e:
                    self.logger.error(f'Error uploading file {output_filename}: {str(e)}')
            if not output_filename.startswith('s3'):
                os.remove(output_file)
        return upload_output_list

    @property
    def input_keys(self):
        return {
            'input_key': r'^(.*)\.(nc|tsv|txt|gif|tar|zip|png|kml|dat|gz|pdf|docx|kmz|xlsx|eos|csv'
                         r'|hdf5|hdf|nc4|ict|xls|.*rest|h5|xlsx|1Hz|impacts_archive|\d{5})$',
            'legacy_key': r'^(.*).*$'
        }

    @staticmethod
    def get_output_files(output_file_path, excluded):
        """
        Returns list of granules processed through MDX (source and generated)
        :param output_file_path: path to file
        :param excluded: variable that defines whether a collection is excluded from "normal" mdx
                         processing
        :return: list of granules processed through MDX
        """
        output_files = [] if excluded else [output_file_path]
        if os.path.isfile(output_file_path + ".cmr.xml"):
            output_files += [output_file_path + ".cmr.xml"]
        return output_files

    @staticmethod
    def read_switcher_json(file_type):
        """
        Reads switchers.json and returns the appropriate json switcher for the file type
        :param file_type: string which describes which extract module is calling the method
        :return: dict of processes for specific file type
        """
        path_to_switcher_json = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                             "switchers.json")
        with open(path_to_switcher_json, 'r') as f:
            output_dict = json.load(f)
        return output_dict[file_type]

    def process(self):
        """
        Override the processing wrapper
        :return: dict with list of granules (source and generated) associated with granule & input
        """
        collection = self.config.get('collection')
        collection_name = collection.get('name')
        collection_version = collection.get('version')
        is_legacy = collection.get('meta', {}).get('metadata_extractor', [])[0].get('module') == \
                    'legacy'
        key = 'legacy_key' if is_legacy else 'input_key'
        self.config['fileStagingDir'] = None if 'fileStagingDir' not in self.config.keys() else \
            self.config['fileStagingDir']
        self.config['fileStagingDir'] = f"{collection_name}__{collection_version}" if \
            self.config['fileStagingDir'] is None else self.config['fileStagingDir']
        url_path = collection.get('url_path', self.config['fileStagingDir'])
        excluded = collection_name in self.exclude_fetch() or is_legacy
        if excluded:
            self.output.append(self.input[0])
            output = {key: self.mutate_input(self.path, self.input[0])}
        else:
            output = self.fetch_all()
        # Assert we have inputs to process
        assert output[key], "fetched files list should not be empty"
        files_sizes = {}
        input_size = None
        for output_file_path in output.get(key):
            data = self.extract_metadata(file_path=output_file_path, config=self.config,
                                         output_folder=self.path)
            input_size = float(data.get('SizeMBDataGranule', 0)) * 1E6
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
            filename = os.path.basename(uploaded_file)
            granule_id = filename.split('.cmr.xml')[0]
            if granule_id not in granule_data.keys():
                granule_data[granule_id] = {'granuleId': granule_id, 'files': []}
            granule_data[granule_id]['files'].append(
                {
                    "path": self.config['fileStagingDir'],
                    "url_path": url_path,
                    "name": filename,  # Cumulus changed the key name to be camelCase
                    "filename": uploaded_file,  # We still need to provide some custom steps with
                    # this key holding the object URI
                    "size": files_sizes.get(filename, input_size),
                    "filepath": f"{url_path.rstrip('/')}/{filename}",
                    "fileStagingDir": os.path.dirname(s3.uri_parser(uploaded_file)['key'])
                }
            )
        final_output = list(granule_data.values())

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
