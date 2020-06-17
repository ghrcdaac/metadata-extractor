from ..src.extract_kml_metadata import ExtractKMLMetadata
import json
from os import path
from datetime import datetime


class ExtractGpmsatpaifldMetadata(ExtractKMLMetadata):
    """
    A class to extract Gpmsatpaifld
    """
    metadata = {}
    max_lat = -90.0
    min_lat = 90.0
    max_lon = -180.0
    min_lon = 180.0
    minTime = datetime(2100, 1, 1)
    maxTime = datetime(1900, 1, 1)

    def __init__(self, file_path):
        super().__init__(file_path)
        with open(path.join(path.dirname(__file__), f"../src/helpers/gpmsatpaifldRefData.json"), 'r') as fp:
            self.metadata = json.load(fp)
        self.get_variables_min_max(file_path)

    def get_variables_min_max(self, variable_key):
        """

        :param c_data: KML data
        :param variable_key: The KML (filename) key we need to target
        :return: list longitude coordinates
        """
        fname = variable_key.split('/')[-1].split('.')[0]
        key = fname.split('_')[-1]
        try:
            self.minTime = datetime.strptime(self.metadata[key]['start'], '%Y-%m-%dT%H:%M:%SZ')
            self.maxTime = datetime.strptime(self.metadata[key]['end'], '%Y-%m-%dT%H:%M:%SZ')
            self.max_lat = self.metadata[key]['NLat']
            self.min_lat = self.metadata[key]['SLat']
            self.max_lon = self.metadata[key]['ELon']
            self.min_lon = self.metadata[key]['WLon']
        except KeyError:
            print('Value Not Found!')

        return

    def get_wnes_geometry(self, scale_factor=1.0, offset=0):
        """
        Extract the geometry from a KML file
        :param scale_factor: In case it is not CF compliant we will need scale factor
        :param offset: data offset if the KML not CF compliant
        :return: list of bounding box coordinates [west, north, east, south]
        """
        north, south, east, west = [round((x * scale_factor) + offset, 3) for x in
                                    [self.max_lat, self.min_lat, self.max_lon, self.min_lon]]
        return [self.convert_360_to_180(west), north, self.convert_360_to_180(east), south]

    def get_temporal(self, time_variable_key='time', units_variable='units', scale_factor=1.0, offset=0,
                     date_format='%Y-%m-%dT%H:%M:%SZ'):
        """
        :param time_variable_key: The KML variable we need to target
        :param units_variable: The KML variable we need to target
        :param scale_factor: In case it is not CF compliant we will need scale factor
        :param offset: data offset if the netCDF not CF compliant
        :param date_format IF specified the return type will be a string type
        :return:
        """

        start_date = self.minTime.strftime(date_format)
        stop_date = self.maxTime.strftime(date_format)
        return start_date, stop_date

    def get_metadata(self, ds_short_name, format='KML', version='01'):
        """
        extracts time and geometry data from the granule
        :param ds_short_name:
        :param time_variable_key:
        :param lon_variable_key:
        :param lat_variable_key:
        :param time_units:
        :param format:
        :return:
        """
        start_date, stop_date = self.get_temporal(time_variable_key='lon',
                                                  units_variable='time',
                                                  date_format='%Y-%m-%dT%H:%M:%SZ')
        data = dict()
        data['ShortName'] = ds_short_name
        data['GranuleUR'] = self.file_path.split('/')[-1]
        data['BeginningDateTime'], data['EndingDateTime'] = start_date, stop_date

        geometry_list = self.get_wnes_geometry()

        data['WestBoundingCoordinate'], data['NorthBoundingCoordinate'], \
        data['EastBoundingCoordinate'], data['SouthBoundingCoordinate'] = list(str(x) for x in geometry_list)
        data['SizeMBDataGranule'] = str(round(self.get_file_size_megabytes(), 2))
        data['checksum'] = self.get_checksum()
        data['DataFormat'] = format
        data['VersionId'] = version
        return data


if __name__ == '__main__':
    print('Extracting Gpmsatpaifld Metadata')
