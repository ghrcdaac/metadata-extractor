from ..src.extract_ascii_metadata import ExtractASCIIMetadata
from datetime import datetime, timedelta
import os
import re
import gzip
import shutil


class ExtractNalmarawMetadata(ExtractASCIIMetadata):
    """
    A class to extract Nalmaraw granule metadata
    """
    start_time = datetime(2100, 1, 1)
    end_time = datetime(1900, 1, 1)
    north = -90.0
    south = 90.0
    east = -180.0
    west = 180.0

    def __init__(self, file_path):
        super().__init__(file_path)
        self.file_path = file_path
        self.file_name = os.path.basename(file_path)
        self.get_variables_min_max()

    def get_variables_min_max(self, **kwargs):
        """
        Extracts temporal metadata and assign spatial metadata for nalmaraw granules
        """
        self.start_time = min(datetime.strptime(re.search(r'^.*(\d{6}_\d{6}).*$',
                                                          self.file_name)[1], '%y%m%d_%H%M%S'),
                              self.start_time)
        self.end_time = max(self.end_time, self.start_time + timedelta(seconds=599))

        station_dict = {
            "A": {
                "lat": 34.80925861,
                "lon": -87.03572250
            },
            "B": {
                "lat": 34.64338080,
                "lon": -86.77140250
            },
            "C": {
                "lat": 34.72535360,
                "lon": -86.64497810
            },
            "D": {
                "lat": 34.66563310,
                "lon": -86.35861290
            },
            "E": {
                "lat": 34.74556220,
                "lon": -86.51265060
            },
            "F": {
                "lat": 34.98364960,
                "lon": -86.83935450
            },
            "G": {
                "lat": 34.89969360,
                "lon": -86.55784870
            },
            "H": {
                "lat": 34.61219060,
                "lon": -86.51968730
            },
            "J": {
                "lat": 34.52313820,
                "lon": -86.96816440
            },
            "K": {
                "lat": 34.65786694,
                "lon": -87.34364694
            },
            "L": {
                "lat": 35.15320361,
                "lon": -87.06117444
            },
            "M": {
                "lat": 35.06845668,
                "lon": -86.56240889
            }
        }

        station_identifier = re.search(r'^L([A-Z])_NALMA_.*_\d{6}_\d{6}.dat$',
                                       self.file_name)[1]
        self.north = station_dict[station_identifier]['lat'] + 0.001
        self.south = station_dict[station_identifier]['lat'] - 0.001
        self.east = station_dict[station_identifier]['lon'] + 0.001
        self.west = station_dict[station_identifier]['lon'] - 0.001

        self.compress_raw_file()

    def compress_raw_file(self):
        """
        Compresses nalmaraw granules for archiving
        """
        self.file_path = f'{self.file_path}.gz'
        self.file_name = os.path.basename(self.file_path)
        with open(self.file_path.rstrip('.gz'), 'rb') as f_in, gzip.open(self.file_path,
                                                                         'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

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

    def get_metadata(self, ds_short_name, format='Binary', version='01', **kwargs):
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
        data['GranuleUR'] = self.file_name
        data['UpdatedGranuleUR'] = self.file_name
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
    print('Extracting Nalmaraw Metadata')
