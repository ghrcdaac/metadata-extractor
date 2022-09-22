from ..src.extract_ascii_metadata import ExtractASCIIMetadata
import json
import os
import pathlib


class ExtractGpmkumlpvexMetadata(ExtractASCIIMetadata):
    """
    A class to extract metadata from this dataset
    """

    def __init__(self, file_path):
        # global dataset_loc, north, south, east, west, start_time, end_time
        super().__init__(file_path)

        with open(os.path.join(pathlib.Path(__file__).parent.absolute(),
                               '../src/helpers/gpmkumlpvexRefData.json'), 'r') as fp:
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
        # north, south, east, west = [round((x * scale_factor) + offset, 3) for x in
        #                             [self.north, self.south, self.east, self.west]]
        # return [self.convert_360_to_180(west), north, self.convert_360_to_180(east), south]
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
        # start_date = datetime.strptime(str(self.start_time), '%Y-%m-%d %H:%M:%S')
        # start_date = start_date.strftime(date_format)
        #
        # stop_date = datetime.strptime(str(self.end_time), '%Y-%m-%d %H:%M:%S')
        # stop_date = stop_date.strftime(date_format)
        #
        # return start_date, stop_date
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

    def get_metadata(self, ds_short_name, format='GRIB', version='1', **kwargs):
        """

        :param ds_short_name:
        :param time_variable_key:
        :param lon_variable_key:
        :param lat_variable_key:
        :param time_units:
        :param format:
        :return:
        """
        # start_date, stop_date = self.get_temporal(time_variable_key='lon', units_variable='time',
        #                                           date_format='%Y-%m-%dT%H:%M:%SZ')

        # data['GranuleUR'] = self.file_path.split('/')[-1]
        data = dict()
        data['GranuleUR'] = granule_name = os.path.basename(self.file_path)
        start_date, stop_date = self.get_temporal_lookup(granule_name)
        data['ShortName'] = ds_short_name
        data['BeginningDateTime'], data['EndingDateTime'] = start_date, stop_date

        # geometry_list = self.get_wnes_geometry()
        geometry_list = self.get_wnes_geometry_lookup(granule_name)
        data['WestBoundingCoordinate'], data['NorthBoundingCoordinate'], \
        data['EastBoundingCoordinate'], data['SouthBoundingCoordinate'] = list(
            str(x) for x in geometry_list)
        # data['SizeMBDataGranule'] = str(round(self.get_file_size_megabytes(), 2))
        # data['checksum'] = self.get_checksum()
        data['checksum'] = self.dataset_loc.get(granule_name).get('checksum',
                                                                   "09f7e02f1290be211da707a266f153b3")
        data['SizeMBDataGranule'] = self.dataset_loc.get(granule_name).get('SizeMBDataGranule',
                                                                            "1400")
        data['DataFormat'] = self.dataset_loc.get(granule_name).get('format')
        data['VersionId'] = version
        return data


# if __name__ == '__main__':
    # print('Extracting Gpmkumlpvex Metadata')
    # path_to_file = "../test/fixtures/lpvex_RADAR_KUMPULA_UF_20100901.tar.gz"
    # exnet = ExtractGoesrpltavirisngMetadata(path_to_file)
    # metada = exnet.get_metadata("test")
    # print(metada)
