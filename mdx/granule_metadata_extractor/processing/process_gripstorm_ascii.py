from ..src.extract_ascii_metadata import ExtractASCIIMetadata
from datetime import datetime


class ExtractGripstormASCIIMetadata(ExtractASCIIMetadata):
    """
    A class to extract metadata from Gripstorm ASCII files
    """

    replace_date_dict = {
        '100832': '100901',
        '100833': '100902'
    }

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
        :param c_data: netCDF data
        :param variable_key: The NetCDF key we need to target
        :return: list longitude coordinates
        """
        for line in self.file_lines:
            tkn = line.split()

            if 'NaN' in tkn[4:5]:
                continue

            if tkn[2] in self.replace_date_dict:
                tkn[2] = self.replace_date_dict[tkn[2]]

            dt = datetime.strptime(f"20{tkn[2]}{tkn[3]}", '%Y%m%d%H%M')
            self.start_time = min(self.start_time, dt)
            self.end_time = max(self.end_time, dt)

            lat, lon = [float(tkn[4][0:-1]), float(tkn[5][0:-1])]

            if 'S' in tkn[4]:
                lat = -1. * lat
            if 'W' in tkn[5]:
                lon = -1. * lon

            self.north = max(self.north, lat)
            self.south = min(self.south, lat)
            self.east = max(self.east, lon)
            self.west = min(self.west, lon)


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
    print('Extracting Gripstorm Metadata from ASCII files')
    file_path = "/home/lwang13/GHRCCLOUD/granule-metadata-extractor/test/fixtures" \
                "/grip_storm_201008120830_consensus-forecast.txt"
    exnet = ExtractGripstormASCIIMetadata(file_path)
    metada = exnet.get_metadata("test")
