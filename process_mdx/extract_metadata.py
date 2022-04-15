import granule_metadata_extractor.processing as mdx
import granule_metadata_extractor.src as src
from re import match
import json
import os


class ExtractMetadata:
    """
    Class for metadata extraction code
    """

    def __init__(self, config):
        self.config = config

    def generate_json_data(self, data, access_url, output_folder):
        """
        Generates umm-g json for CMR metadata
        :param data: python dict containing granule extracted metadata
        :param access_url: url to get data
        :param output_folder: local folder to create json file at
        :return: input dict
        """

        granule_new_name = data.get('UpdatedGranuleUR', None)
        if granule_new_name:
            access_url = access_url.replace(os.path.basename(access_url),
                                            os.path.basename(granule_new_name))
        data['OnlineAccessURL'] = access_url
        umm_json = src.GenerateUmmGJson(data,
                                        age_off=self.config.get('collection', {}).get('meta', {}).
                                        get('age-off', None))
        umm_json.generate_umm_json_file(output_folder=output_folder)
        return data

    def extract_netcdf_metadata(self, ds_short_name, version, access_url, file, file_vars,
                                output_folder='/tmp', format='netCDF-4'):
        """
        Function to extract metadata from netCDF files
        :param ds_short_name: collection shortname
        :param version: version
        :param access_url: The access URL to the granule
        :param file: Path to netCDF file
        :param file_vars: netCDF variables
        :param output_folder: Location to output created echo10xml file
        :param format: data type of input file
        :return:
        """

        filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'switcher.json')
        with open(filepath) as f:
            switcher_file = json.load(f)
        switcher = switcher_file['netcdf']

        regex = file_vars.get('regex')
        time_variable_key = file_vars.get('time_var_key')
        lon_variable_key = file_vars.get('lon_var_key')
        lat_variable_key = file_vars.get('lat_var_key')
        time_units = file_vars.get('time_units', 'units')

        if match(regex, os.path.basename(file)):
            metadata = switcher.get(ds_short_name, src.ExtractNetCDFMetadata)(file)
            format = 'netCDF-3' if '.nc4' not in file else format
            data = metadata.get_metadata(ds_short_name=ds_short_name,
                                         time_variable_key=time_variable_key,
                                         lon_variable_key=lon_variable_key,
                                         lat_variable_key=lat_variable_key,
                                         time_units=time_units, format=format, version=version)
            return ExtractMetadata.generate_json_data(self, data=data, access_url=access_url,
                                                      output_folder=output_folder)
        return {}

    def extract_csv_metadata(self, ds_short_name, version, access_url, file, file_vars={},
                             output_folder='/tmp', format='CSV'):
        """
        Function to extract metadata from netCDF files
        :param ds_short_name: collection shortname
        :param version: version
        :param access_url: The access URL to the granule
        :param file: Path to csv file
        :param file_vars: Variables within csv file
        :param output_folder: Location to output created echo10xml file
        :param format: data type of input file
        :return:
        """

        metadata = src.ExtractCSVMetadata(file)

        time_position = file_vars.get('time_row_position', 0)
        lon_postion = file_vars.get('lon_row_position', 15)
        lat_postion = file_vars.get('lat_row_position', 16)
        time_units = file_vars.get('time_units', 'hours')
        regex = file_vars.get('regex', '.*')

        if match(regex, os.path.basename(file)):
            data = metadata.get_metadata(ds_short_name=ds_short_name, time_position=time_position,
                                         time_units=time_units, lon_postion=lon_postion,
                                         lat_postion=lat_postion,
                                         format=format, version='0.9')
            return ExtractMetadata.generate_json_data(self, data=data, access_url=access_url,
                                                      output_folder=output_folder)
        return {}

    def extract_module_metadata(self, ds_short_name, version, access_url, file, module_type,
                                file_vars={}, output_folder='/tmp', file_format=''):
        """
        Function to extract metadata from files
        :param ds_short_name: collection shortname
        :param version: version
        :param access_url: The access URL to the granule
        :param file: Path to file
        :param module_type: Collection definition mdx module type
        :param file_vars: Variables
        :param output_folder: Location to output created umm.json file
        :param file_format: data type of input file
        :return:
        """
        # Read json switcher file
        filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'switcher.json')
        with open(filepath) as f:
            switcher_file = json.load(f)
        # Get correct switcher dependent on module
        switcher = switcher_file.get(module_type)
        file_format = switcher['browse_formats'][ds_short_name] if module_type == 'browse' else \
            file_format
        regex = file_vars.get('regex', '.*')
        if match(regex, os.path.basename(file)):
            processing_file = switcher.get(ds_short_name)
            metadata = getattr(mdx, processing_file)(file) if processing_file else \
                self.default_switch(file)
            data = metadata.get_metadata(ds_short_name=ds_short_name, format=file_format,
                                         version=version)
            return ExtractMetadata.generate_json_data(self, data=data, access_url=access_url,
                                                      output_folder=output_folder)
        return {}

    def default_switch(self, *args):
        """
        This is a default switch to pass
        *args is an arbitrary argument
        :return:
        """
        pass

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
        generalized_mdx_modules = ['ascii', 'avi', 'browse', 'binary', 'kml']
        processing_switcher = {
            "netcdf": self.extract_netcdf_metadata,
            "csv": self.extract_csv_metadata
        }
        format_switcher = {
            "ascii": "ASCII",
            "avi": "AVI",
            "browse": "BROWSE",
            "binary": "Binary",
            "kml": "KML"
        }

        return_data_dict = {}
        for metadata_extractor_var in metadata_extractor_vars:
            module_type = metadata_extractor_var.get('module')
            if module_type in generalized_mdx_modules:
                data = self.extract_module_metadata(ds_short_name, version, access_url, file_path,
                                                    module_type, metadata_extractor_var,
                                                    output_folder,
                                                    file_format=format_switcher.get(module_type))
            else:
                data = processing_switcher.get(module_type,
                                               self.default_switch)(ds_short_name, version,
                                                                    access_url, file_path,
                                                                    metadata_extractor_var,
                                                                    output_folder)
            return_data_dict.update(data) if data else None
        return return_data_dict
