from ..src.extract_ascii_metadata import ExtractASCIIMetadata
from datetime import datetime


class ExtractGpm2dc3vpMetadata(ExtractASCIIMetadata):
    """
    A class to extract metadata from Gpm2dc3vp ASCII files
    """

    sn16_lon = -79.7805861111111 #-1 * (79.+46./60+50.11/60/60)
    sn16_lat = 44.233180555555556 #44.+13./60 +59.45/60/60

    start_time = datetime(2100, 1, 1)
    end_time = datetime(1900, 1, 1)
    north = -90.
    south = 90.
    east = -180.
    west = 180.

    def __init__(self, file_path):
        super().__init__(file_path)
        with open(file_path, 'r') as f:
            self.file_lines = f.readlines()
        self.get_variables_min_max()

    def get_variables_min_max(self):
        """
        :return: list longitude coordinates
        """
        self.north = self.sn16_lat+0.01
        self.south = self.sn16_lat-0.01
        self.east = self.sn16_lon+0.01
        self.west = self.sn16_lon-0.01

        date_str = '20'+self.file_path.split('/')[-1].split('_')[3][1:] #i.e.,2006337 (%Y%j)

        for line in self.file_lines[2:]:
            tkn = line.split() #tkn[0] i.e., '08:23:53.074'
            dt = datetime.strptime(date_str+tkn[0],'%Y%j%H:%M:%S.%f')
            self.start_time = min(self.start_time, dt)
            self.end_time = max(self.end_time, dt)

    def get_wnes_geometry(self, scale_factor=1.0, offset=0):
        """
        Extract the geometry from a file
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
        :param time_variable_key: The variable we need to target
        :param units_variable: The variable we need to target
        :param scale_factor: In case it is not CF compliant we will need scale factor
        :param offset: data offset if the netCDF not CF compliant
        :param date_format IF specified the return type will be a string type
        :return:
        """
        start_date = self.start_time.strftime(date_format)
        stop_date = self.end_time.strftime(date_format)
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

        geometry_list = self.get_wnes_geometry()

        data['WestBoundingCoordinate'], data['NorthBoundingCoordinate'], \
        data['EastBoundingCoordinate'], data['SouthBoundingCoordinate'] = list(
            str(x) for x in geometry_list)
        data['SizeMBDataGranule'] = str(round(self.get_file_size_megabytes(), 2))
        data['checksum'] = self.get_checksum()
        data['DataFormat'] = format
        data['VersionId'] = version
        return data


if __name__ == '__main__':
    print('Extracting Gpm2dc3vp Metadata from ASCII files')
    file_path = "/home/lwang13/GHRCCLOUD/granule-metadata-extractor/test/fixtures" \
                "/c3vp_2dvd_sn16_V06336_flakes_noRA.txt"
    exnet = ExtractGpm2dc3vpMetadata(file_path)
    metada = exnet.get_metadata("test")
