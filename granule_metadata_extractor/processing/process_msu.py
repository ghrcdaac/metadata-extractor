from ..src.extract_ascii_metadata import ExtractASCIIMetadata
from datetime import datetime, timedelta
import os
import re
from calendar import monthrange
from netCDF4 import Dataset
from dateutil.relativedelta import relativedelta


class ExtractMsuMetadata(ExtractASCIIMetadata):
    """
    A class to extract Msu spatial and temporal metadata
    """

    def __init__(self, file_path):
        super().__init__(file_path)

        self.file_path = file_path
        self.file_name = os.path.basename(self.file_path)
        self.format = "ASCII" if ".txt" in self.file_name else "netCDF-4"
        self.end_of_day_offset = timedelta(days=1) - timedelta(seconds=1)
        self.start_of_30_year_period = datetime(1991, 1, 1)
        self.end_of_30_year_period = datetime(2020, 12, 31) + self.end_of_day_offset
        self.default_metadata = {
            'start': datetime(2100, 1, 1),
            'end': datetime(1900, 1, 1),
            'north': 88.75,
            'south': -88.75,
            'east': 178.75,
            'west': -178.75
        }
        self.start_time, self.end_time, self.north, self.south, self.east, self.west = \
            [self.default_metadata[x] for x in ['start', 'end', 'north', 'south', 'east', 'west']]

        if self.format == "ASCII":
            self.get_variables_min_max()
        else:
            self.get_netcdf_metadata()

    @staticmethod
    def calculate_offset(input_day):
        """
        This method is used to accomodate day offset of netcdf files for txt files
        :param input_day: day to offset from
        :return: day to match netcdf file
        """
        return timedelta(days=(int(input_day) * 5) - 3)

    def get_variables_min_max(self, **kwargs):
        """
        Extracts temporal and spatial metadata from Msu files
        :return: list longitude coordinates
        """
        special_type_match = re.search(r'^.*([a-z]{3})(\.\d{4})?_.*$', self.file_name)
        if special_type_match[1] in 'acg':
            self.start_time = self.start_of_30_year_period
            self.end_time = self.end_of_30_year_period
        elif special_type_match[1] in 'amg':
            if 'pen' in self.file_name:
                pen_min_five_day_period = 1
                pen_max_five_day_period = 73
                self.start_time = datetime(int(special_type_match[2].lstrip('.')), 1, 1) + \
                                  self.calculate_offset(pen_min_five_day_period)
                self.end_time = datetime(self.start_time.year, 1, 1) + self.end_of_day_offset + \
                                self.calculate_offset(pen_max_five_day_period)
            else:
                self.start_time = datetime(int(special_type_match[2].lstrip('.')), 1, 1)
                self.end_time = datetime(self.start_time.year + 1, 1, 1) - timedelta(seconds=1)
        else:
            self.get_ascii_nongridded_metadata()

    def get_ascii_nongridded_metadata(self):
        """
        Extracts metadata from ascii non-gridded msu files
        """
        with open(self.file_path, 'r') as f:
            for line in f.readlines():
                matches = re.search(r'^[ \t]+(\d{4})[ \t]+(\d{1,2}).*$', line)
                if matches:
                    if 'pen' in self.file_name:
                        # Pen are 5 day periods, so to find actual day, we must take 5 day period
                        # multiply by 5 to get days from beginning of year then subtract 3 to align
                        # with netcdf start/end days
                        self.start_time = min(self.start_time, datetime(int(matches[1]), 1, 1) +
                                              self.calculate_offset(matches[2]))
                        self.end_time = max(self.end_time,
                                            datetime(int(matches[1]), 1, 1) +
                                            self.calculate_offset(matches[2]) +
                                            self.end_of_day_offset)
                    else:
                        self.start_time = min(self.start_time, datetime(int(matches[1]),
                                                                        int(matches[2]), 1))
                        self.end_time = max(self.end_time, datetime(int(matches[1]),
                                                                    int(matches[2]),
                                                                    monthrange(int(matches[1]),
                                                                               int(matches[2]))[
                                                                        1]) +
                                            self.end_of_day_offset)

    def get_netcdf_metadata(self):
        """
        Extracts metadata from netcdf msu file
        """
        dataset = Dataset(self.file_path)
        time_var = dataset.variables["time"]
        if 'acg' in self.file_name:
            self.start_time = self.start_of_30_year_period
            self.end_time = self.end_of_30_year_period
        elif 'pen' in self.file_name:
            time_start = datetime.strptime(re.search(r'^days since (.*)$', time_var.units)[1],
                                           '%Y-%m-%d %H:%M:%S')
            self.start_time = time_start + timedelta(days=int(time_var.valid_min))
            self.end_time = time_start + timedelta(days=int(time_var.valid_max)) + \
                            self.end_of_day_offset
        else:
            self.start_time = datetime.strptime(
                re.search(r'^months since (.*)$', time_var.units)[1],
                '%Y-%m-%d %H:%M:%S')
            self.end_time = self.start_time + relativedelta(months=int(time_var.valid_max)) \
                            - timedelta(seconds=1)

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
        print(self.end_time)
        stop_date = self.end_time.strftime(date_format)
        return start_date, stop_date

    def get_metadata(self, ds_short_name, format='ASCII', version='01', **kwargs):
        """

        :param ds_short_name: dataset shortname
        :param format: file format
        :param version: collection version number
        :return:
        """
        start_date, stop_date = self.get_temporal(time_variable_key='lon',
                                                  units_variable='time',
                                                  date_format='%Y-%m-%dT%H:%M:%SZ')
        data = dict()
        data['ShortName'] = ds_short_name
        data['GranuleUR'] = self.file_name
        data['BeginningDateTime'], data['EndingDateTime'] = start_date, stop_date

        gemetry_list = self.get_wnes_geometry()

        data['WestBoundingCoordinate'], data['NorthBoundingCoordinate'], \
        data['EastBoundingCoordinate'], data['SouthBoundingCoordinate'] = list(
            str(x) for x in gemetry_list)
        data['SizeMBDataGranule'] = str(round(self.get_file_size_megabytes(), 2))
        data['checksum'] = self.get_checksum()
        data['DataFormat'] = self.format
        data['VersionId'] = version
        return data


if __name__ == '__main__':
    print('Extracting MSU Metadata')
    path_to_file = r'C:\Users\ecampos\Documents\granule-metadata-extractor\test\fixtures' \
                   r'\NAMMA_DROP_20060807_193132_P.dat'
    exnet = ExtractMsuMetadata(path_to_file)
    metada = exnet.get_metadata("test")
