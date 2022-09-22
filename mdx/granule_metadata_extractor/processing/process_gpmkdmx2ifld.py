from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
import json
import os
import pathlib


class ExtractGpmkdmx2ifldMetadata(ExtractNetCDFMetadata):
    """
    A class to extract dataset metadata 
    """

    def __init__(self, file_path):
        self.file_path = file_path

        with open(os.path.join(pathlib.Path(__file__).parent.absolute(),
                               '../src/helpers/gpmkdmx2ifldRefData.json'), 'r') as fp:
            #self.gpmseafluxicepop_loc = json.load(fp)
            self.dataset_loc = json.load(fp)

    def get_variables_min_max(self, data):
        """

        :param data:
        :return:
        """
        pass

    def get_wnes_geometry(self, scale_factor=1.0, offset=0):
        """
        Extract the geometry from a GIF file
        :param scale_factor: In case it is not CF compliant we will need scale factor
        :param offset: data offset if the netCDF not CF compliant
        :return: list of bounding box coordinates [west, north, east, south]
        """
        pass

    def get_temporal(self, time_variable_key='time', units_variable='units', scale_factor=1.0,
                     offset=0,
                     date_format='%Y-%m-%dT%H:%M:%SZ'):
        """
        :param time_variable_key: The NetCDF variable we need to target
        :param units_variable: The NetCDF variable we need to target
        :param scale_factor: In case it is not CF compliant we will need scale factor
        :param offset: data offset if the netCDF not CF compliant
        :param date_format IF specified the return type will be a string type
        :return:
        """
        pass

    def get_temporal_lookup(self, granule_name):
        """
        Because the files are huge we need to create a lookup file to get temporal
        instead of downloading the file an extracting it programaticlly
        Note that we already have get_metadata function that does that
        :param granule_name: filename also is used as a lookup key
        :return: start_date, stop_date
        """
        start_date, stop_date = self.dataset_loc.get(granule_name).get('temporal')
        return start_date, stop_date

    def get_wnes_geometry_lookup(self, granule_name):
        """
        Because the files are huge we need to create a lookup file to get geometry
        instead of downloading the file an extracting it programaticlly
        Note that we already have get_wnes_geometry function that does that
        :param granule_name: filename also is used as a lookup key
        :return: [west, north,east, south]
        """
        return self.dataset_loc.get(granule_name).get('wnes_geometry')

    def get_metadata(self, ds_short_name, format='Binary', version='1', **kwargs):
        """
        :param ds_short_name:
        :param time_variable_key:
        :param lon_variable_key:
        :param lat_variable_key:
        :param time_units:
        :param format:
        :return:
        """
        data = dict()
        data['GranuleUR'] = granule_name = os.path.basename(self.file_path)
        start_date, stop_date = self.get_temporal_lookup(granule_name)
        data['ShortName'] = ds_short_name
        data['BeginningDateTime'], data['EndingDateTime'] = start_date, stop_date

        geometry_list = self.get_wnes_geometry_lookup(granule_name)
        data['WestBoundingCoordinate'], data['NorthBoundingCoordinate'], \
        data['EastBoundingCoordinate'], data['SouthBoundingCoordinate'] = list(
            str(x) for x in geometry_list)
        data['checksum'] = self.dataset_loc.get(granule_name).get('checksum',
                                                                   "09f7e02f1290be211da707a266f153b3")
        data['SizeMBDataGranule'] = self.dataset_loc.get(granule_name).get('SizeMBDataGranule',
                                                                            "1400")
        data['DataFormat'] = self.dataset_loc.get(granule_name).get('format')
        data['VersionId'] = version
        return data
