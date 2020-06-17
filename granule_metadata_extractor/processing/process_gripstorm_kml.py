from ..src.extract_kml_metadata import ExtractKMLMetadata
from datetime import datetime, timedelta


class ExtractGripstormKMLMetadata(ExtractKMLMetadata):
    """
    A class to extract metadata from Gripstorm KML files
    """
    start_time = datetime(2100, 1, 1)
    end_time = datetime(1900, 1, 1)
    north = -90.
    south = 90.
    east = -180.
    west = 180.


    def __init__(self, file_path):
        super().__init__(file_path)
        self.file_name = file_path.split('/')[-1]
        with open(file_path, 'r') as f:
            self.file_lines = f.readlines()
        self.get_variables_min_max()

    def get_variables_min_max(self):
        """
        :param c_data: netCDF data
        :param variable_key: The NetCDF key we need to target
        :return: list longitude coordinates
        """
        valid_line = ['<description>', 'Time:', 'Location:']
        flag = 0

        for line in self.file_lines:
            if all(x in line for x in valid_line):
                tkn = line.split('<br>')

                dt = datetime.strptime(tkn[1].split()[1], '%Y%m%d%H')
                self.start_time = min(self.start_time, dt)
                self.end_time = max(self.end_time, dt)

                loc_str = tkn[3].split('</br>')[0].split()[1].split(',')
                lat, lon = [float(loc_str[0][0:-1]), float(loc_str[1][0:-1])]

                if lon == -0.0:
                    lon = 0.0

                self.north = max(self.north, lat)
                self.south = min(self.south, lat)
                self.east = max(self.east, lon)
                self.west = min(self.west, lon)
                flag = 1

        if not flag:
            self.start_time = min(self.start_time,
                                  datetime.strptime(self.file_name.split('_')[-1][0:8],
                                                    '%Y%m%d'))
            self.end_time = max(self.end_time, self.start_time + timedelta(seconds=86399))
            # Assign Browse collection metadata's lat/lon coverage
            self.north = 87.6
            self.south = 0.8
            self.east = 0.0
            self.west = -178.5

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

    def get_metadata(self, ds_short_name, format='KML', version='01'):
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
    print('Extracting Gripstorm Metadata from KML files')
    file_path = "/home/lwang13/GHRCCLOUD/granule-metadata-extractor/test/fixtures" \
                "/grip_storm_track_fcst_11L_late_latest_201009191620.kml"
    exnet = ExtractGripstormKMLMetadata(file_path)
    metada = exnet.get_metadata("test")
