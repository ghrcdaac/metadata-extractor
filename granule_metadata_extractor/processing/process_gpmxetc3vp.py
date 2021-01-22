from ..src.extract_ascii_metadata import ExtractASCIIMetadata
from datetime import datetime


class ExtractGpmxetc3vpMetadata(ExtractASCIIMetadata):
    """
    A class to extract Gpmxetc3vp
    """
    start_time = datetime(2100, 1, 1)
    end_time = datetime(1900, 1, 1)
    north = -90.0
    south = 90.0
    east = -180.0
    west = 180.0

    def __init__(self, file_path):
        super().__init__(file_path)

        file_name = file_path.split('/')[-1]
        self.file_path = file_path
        self.origin_lat = 44.23
        self.origin_lon = -79.78
        self.bbox_offset = 0.01
        self.get_variables_min_max(file_name)

    def get_variables_min_max(self, file_name):
        """
        Extracts temporal and spatial metadata from input file
        :param file_name: file name
        :return: list longitude coordinates
        """
        with open(self.file_path, 'rb') as f:
            lines = f.readlines()[1:]
            for line in lines:
                try:
                    dtstr = line.decode("utf8").split(',')[0]
                    dt = datetime.strptime(dtstr, '%Y-%m-%d %H:%M:%S')
                    self.start_time = min(dt, self.start_time)
                    self.end_time = max(dt, self.end_time)
                except UnicodeError:
                    # This exception fixes the below line which has an error within it's utf8
                    # encoding:
                    # b'2007-03-20 00:08:00,1.1743493e+009,5.729,1.178,981,209.8,5.874,
                    # 3.019,4.953,2.546,96.3,0,0,-3.027,-1.53,-1.282,-.624,-.346,-2.991,5.346,
                    # 3.715,0,0,13.05\xff,\r\n'
                    dtstr = eval(str(line).replace('\\xff',
                                                   '')).decode("utf8").split(',')[0]
                    dt = datetime.strptime(dtstr, '%Y-%m-%d %H:%M:%S')
                    self.start_time = min(dt, self.start_time)
                    self.end_time = max(dt, self.end_time)
        self.north, self.south, self.east, self.west = [self.origin_lat + self.bbox_offset,
                                                        self.origin_lat - self.bbox_offset,
                                                        self.origin_lon + self.bbox_offset,
                                                        self.origin_lon - self.bbox_offset]

    def get_wnes_geometry(self, scale_factor=1.0, offset=0, **kwargs):
        """
        Extract the geometry from a netCDF file
        :param nc_data: netCDF data
        :param timestamp:  The NetCDF variable we need to target
        :param scale_factor: In case it is not CF compliant we will need scale factor
        :param offset: data offset if the netCDF not CF compliant
        :return: list of bounding box coordinates [west, north, east, south]
        """
        north, south, east, west = [round((x * scale_factor) + offset, 3) for x in
                                    [self.north, self.south, self.east, self.west]]
        return [self.convert_360_to_180(west), north, self.convert_360_to_180(east), south]

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
        start_date = self.start_time.strftime(date_format)
        stop_date = self.end_time.strftime(date_format)
        return start_date, stop_date

    def get_metadata(self, ds_short_name, format='ASCII-csv', version='01', **kwargs):
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
        data['EastBoundingCoordinate'], data['SouthBoundingCoordinate'] = list(
            str(x) for x in gemetry_list)
        data['SizeMBDataGranule'] = str(round(self.get_file_size_megabytes(), 2))
        data['checksum'] = self.get_checksum()
        data['DataFormat'] = format
        data['VersionId'] = version
        return data


if __name__ == '__main__':
    print('Extracting Gpmxetc3vp Metadata')
    path_to_file = r'C:\Users\ecampos\Documents\granule-metadata-extractor\test\fixtures' \
                   r'\NAMMA_DROP_20060807_193132_P.dat'
    exnet = ExtractGpmxetc3vpMetadata(path_to_file)
    metada = exnet.get_metadata("test")
