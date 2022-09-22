from ..src.extract_browse_metadata import ExtractBrowseMetadata
import json
from os import path
from datetime import datetime



dc8lase_loc = {}
north = 0
south = 0
west = 0
east = 0
start_time = 0
end_time = 0

class ExtractDc8laseMetadata(ExtractBrowseMetadata):
    """
    A class to extract Er2edop
    """

    def __init__(self, file_path):
        global dc8lase_loc, north, south, east, west, start_time, end_time
        super().__init__(file_path)

        fname = file_path.split('/')[-1]
        key = fname.split('_')[1]

        with open(path.join(path.dirname(__file__), f"../src/helpers/dc8laseRefData.json"), 'r') as fp:
            dc8lase_loc = json.load(fp)
        default_val ={'start': '2100-01-01T00:00:00Z', 'end': '1900-01-01T00:00:00Z',
                      'NLat': -90.0, 'SLat': 90.0, 'ELon': -180.0, 'WLon': 180.0}

        north = dc8lase_loc.get(key, default_val).get("NLat")
        south = dc8lase_loc.get(key, default_val).get("SLat")
        east = dc8lase_loc.get(key, default_val).get("ELon")
        west = dc8lase_loc.get(key, default_val).get("WLon")
        start_time = dc8lase_loc.get(key, default_val).get("start")
        end_time = dc8lase_loc.get(key, default_val).get("end")

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
        global north, south, east, west
        north, south, east, west = [round((x * scale_factor) + offset, 3) for x in [north, south, east, west]]
        return [self.convert_360_to_180(west), north, self.convert_360_to_180(east), south]

    def get_temporal(self, time_variable_key='time', units_variable='units',  scale_factor=1.0, offset=0,
                     date_format = '%Y-%m-%dT%H:%M:%SZ'):
        """
        :param time_variable_key: The NetCDF variable we need to target
        :param units_variable: The NetCDF variable we need to target
        :param scale_factor: In case it is not CF compliant we will need scale factor
        :param offset: data offset if the netCDF not CF compliant
        :param date_format IF specified the return type will be a string type
        :return:
        """
        global start_time, end_time
        start_date = datetime.strptime(str(start_time), '%Y-%m-%dT%H:%M:%SZ')
        start_date = start_date.strftime(date_format)

        stop_date = datetime.strptime(str(end_time), '%Y-%m-%dT%H:%M:%SZ')
        stop_date = stop_date.strftime(date_format)

        return start_date, stop_date


    def get_metadata(self, ds_short_name, format='GIF', version='01'):
        """

        :param ds_short_name:
        :param time_variable_key:
        :param lon_variable_key:
        :param lat_variable_key:
        :param time_units:
        :param format:
        :return:
        """
        start_date, stop_date = self.get_temporal(time_variable_key='lon', units_variable='time',
                                                  date_format='%Y-%m-%dT%H:%M:%SZ')
        data = dict()
        data['ShortName'] = ds_short_name
        data['GranuleUR'] = self.file_path.split('/')[-1]
        data['BeginningDateTime'], data['EndingDateTime'] = start_date, stop_date

        geometry_list = self.get_wnes_geometry()
        #print(geometry_list)
        data['WestBoundingCoordinate'], data['NorthBoundingCoordinate'], \
        data['EastBoundingCoordinate'], data['SouthBoundingCoordinate'] = list(str(x) for x in geometry_list)
        data['SizeMBDataGranule'] = str(round(self.get_file_size_megabytes(), 2))
        data['checksum'] = self.get_checksum()
        data['DataFormat'] = format
        data['VersionId'] = version
        return data

if __name__ == '__main__':
    print('Extracting ER2edop Metadata')
    file_path = "/Users/amarouane/workstation/gitlab/granule-metadata-extractor/test/fixtures/camex3_98233_lase408_n071.gif"
    exnet = ExtractDc8laseMetadata(file_path)
    metada = exnet.get_metadata("test")
