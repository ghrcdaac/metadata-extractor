from ..src.extract_ascii_metadata import ExtractASCIIMetadata
import json
from os import path
from datetime import datetime

aranda_loc = {}
file_lines = []
max_lat = -90.0
min_lat = 90.0
max_lon = -180.0
min_lon = 180.0
minTime = '2100-01-01T00:00:00Z'
maxTime = '1900-01-01T00:00:00Z'


class ExtractGpmodmlpvexMetadata(ExtractASCIIMetadata):
    """
    A class to extract Gpmodmlpvex
    """

    def __init__(self, file_path):
        global aranda_loc, file_lines
        super().__init__(file_path)
        with open(path.join(path.dirname(__file__), f"../src/helpers/odmRefData.json"), 'r') as fp:
            aranda_loc = json.load(fp)
        with open(file_path, 'r') as f:
            file_lines = f.readlines()
        self.timestamp = None

    def __get_min_max(self, date_format):
        """

        :param reader: csv cursor reader
        : culumn_position: Position of the column you want extract min, max
        :return: min_value, max_value
        """
        global file_lines, minTime, maxTime
        dtstr = None
        for ii in range(1, len(file_lines)):
            if '-' in file_lines[ii] and ':' in file_lines[ii]:
                # print(ii,' ',lines[ii])

                line = file_lines[ii]
                tmp = line.split()
                # 1   9-13-2010  12:54:12  5.05    0.00     241 Pulses    8  bins
                tmp0 = tmp[1].split('-')
                mon_str = tmp0[0]
                day_str = tmp0[1]
                year_str = tmp0[2]
                if len(mon_str) == 1:
                    mon_str = '0' + mon_str
                if len(day_str) == 1:
                    day_str = '0' + day_str

                pos = line.find(':')
                tmp1 = line[pos - 2:pos + 6].split(':')
                hr_str = tmp1[0].strip()
                min_str = tmp1[1].strip()
                sec_str = tmp1[2].strip()

                if len(hr_str) == 1:
                    hr_str = '0' + hr_str
                if len(min_str) == 1:
                    min_str = '0' + min_str
                if len(sec_str) == 1:
                    sec_str = '0' + sec_str

                dt = datetime.strptime(year_str + mon_str + day_str + hr_str + min_str + sec_str,
                                       "%Y%m%d%H%M%S")
                dtstr = dt.strftime('%Y%m%d%H%M')
                self.get_variables_min_max(dtstr)
                dt = dt.strftime(date_format)

                minTime = min(dt, minTime)
                maxTime = max(dt, maxTime)

        return minTime, maxTime, dtstr

    def get_variables_min_max(self, variable_key):
        """

        :param c_data: netCDF data
        :param variable_key: The NetCDF key we need to target
        :return: list longitude coordinates
        """
        global aranda_loc, max_lat, min_lat, max_lon, min_lon
        if variable_key in aranda_loc:
            current_lat = aranda_loc[variable_key]['lat']
            current_lon = aranda_loc[variable_key]['lon']
            max_lat = max(current_lat, max_lat)
            min_lat = min(current_lat, min_lat)
            max_lon = max(current_lon, max_lon)
            min_lon = min(current_lon, min_lon)
        return max_lat, min_lat, max_lon, min_lon

    def get_wnes_geometry(self, scale_factor=1.0, offset=0):
        """
        Extract the geometry from a netCDF file
        :param nc_data: netCDF data
        :param timestamp:  The NetCDF variable we need to target
        :param scale_factor: In case it is not CF compliant we will need scale factor
        :param offset: data offset if the netCDF not CF compliant
        :return: list of bounding box coordinates [west, north, east, south]
        """
        self.__get_min_max('%Y-%m-%dT%H:%M:%SZ')
        north, south, east, west = [round((x * scale_factor) + offset, 3) for x in
                                    self.get_variables_min_max(self.timestamp)]
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

        start_date, stop_date, timestamp = self.__get_min_max(date_format)
        self.timestamp = timestamp
        return start_date, stop_date


    def get_metadata(self, ds_short_name, format='ASCII', version='01'):
        """

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

        gemetry_list = self.get_wnes_geometry()

        data['WestBoundingCoordinate'], data['NorthBoundingCoordinate'], \
        data['EastBoundingCoordinate'], data['SouthBoundingCoordinate'] = list(str(x) for x in gemetry_list)
        data['SizeMBDataGranule'] = str(round(self.get_file_size_megabytes(), 2))
        data['checksum'] = self.get_checksum()
        data['DataFormat'] = format
        data['VersionId'] = version
        return data

if __name__ == '__main__':
    print('Extracting Gpmodmlpvex Metadata')
    file_path = "/Users/navaneeth/granule-metadata-extractor/test/fixtures/lpvex_SHP_Aranda_ODM_u100915_08.txt'"
    exnet = ExtractGpmodmlpvexMetadata(file_path)
    metada = exnet.get_metadata("test")
    # with open('the_filename.txt', 'rb') as f:
    #     my_list = pickle.load(f)
    #
    # for itm in my_list:
    #     print(exnet.get_wnes_geometry(itm))
