from ..src.extract_ascii_metadata import ExtractASCIIMetadata
from datetime import datetime
import os
import re


class Extract2dimpactsMetadata(ExtractASCIIMetadata):
    """
    A class to extract 2dimpacts spatial and temporal metadata
    """
    start_time = datetime(2100, 1, 1)
    end_time = datetime(1900, 1, 1)
    north = -90.0
    south = 90.0
    east = -180.0
    west = 180.0
    loc = {'sn70': [37.93450, -75.47081], 'sn25': [37.93715, -75.46622],
           'sn38': [37.92938, -75.47314], 'sn35': [37.94432, -75.48116],
           'sn37': [37.93764, -75.45618]}

    def __init__(self, file_path):
        super().__init__(file_path)

        self.file_path = file_path
        self.file_name = os.path.basename(self.file_path)

        with open(file_path, 'r') as f:
            self.file_lines = f.readlines()

        self.get_variables_min_max(self.file_name)

    def get_variables_min_max(self, filename):
        """
        Extracts temporal and spatial metadata from 2dimpacts files
        :param filename: file name
        :return: list longitude coordinates
        """

        if 'diameter020' in filename:
            self.read_metadata_diameter020()
        elif 'raintotalhour' in filename:
            self.read_metadata_raintotalhour()
        else:
            self.read_metadata_regular()

    def read_metadata_diameter020(self):
        """
        Extracts temporal and spatial metadata from diameter020 files
        """
        self.start_time = datetime.strptime('2020-01-15T01:37:06Z', '%Y-%m-%dT%H:%M:%SZ')
        self.end_time = datetime.strptime('2020-02-28T20:41:44Z', '%Y-%m-%dT%H:%M:%SZ')
        self.north, self.south, self.east, self.west = [37.954319999999996, 37.919380000000004,
                                                        -75.44618, -75.49116000000001]

    def read_metadata_raintotalhour(self):
        """
        Extracts temporal and spatial metadata from raintotalhour files
        """
        stn_id = re.search('^impacts_2dvd_(sn.*)_raintotalhour.txt$', self.file_name)[1]

        for line in self.file_lines:
            tkn = line.split()
            granule_start = datetime.strptime(f"{tkn[0]},{tkn[1]},{tkn[2]}", '%Y,%j,%H:%M')
            granule_end = datetime.strptime(f"{tkn[0]},{tkn[3]},{tkn[4]}", '%Y,%j,%H:%M')

            self.start_time = min(self.start_time, granule_start)
            self.end_time = max(self.end_time, granule_end)

            self.north, self.south, self.east, self.west = [self.loc[stn_id][0] + 0.01,
                                                            self.loc[stn_id][0] - 0.01,
                                                            self.loc[stn_id][1] + 0.01,
                                                            self.loc[stn_id][1] - 0.01]

        if self.start_time == datetime(2100, 1, 1) or self.end_time == datetime(1900, 1, 1):
            self.start_time = datetime.strptime('2020-01-15T01:37:06Z', '%Y-%m-%dT%H:%M:%SZ')
            self.end_time = datetime.strptime('2020-02-28T20:41:44Z', '%Y-%m-%dT%H:%M:%SZ')

    def read_metadata_regular(self):
        """
        Extracts temporal and spatial metadata from the following files:
        eachdrop, largedrop, dropcounts, raindsd_ter, raindsd, rainparameter_ter, rainparameter
        """
        stn_id = re.search('^impacts_2dvd_.*(sn\\d{2})_.*$', self.file_name)[1]

        for line in self.file_lines:
            tkn = line.split()
            if 'eachdrop' in self.file_name or 'largedrop' in self.file_name:
                dt = datetime.strptime(f"{tkn[0]},{tkn[1]},{tkn[2]},{tkn[3]},{tkn[4]}",
                                       "%Y,%j,%H,%M,%S.%f")
            else:
                dt = datetime.strptime(f"{tkn[0]},{tkn[1]},{tkn[2]},{tkn[3]}", "%Y,%j,%H,%M")

            self.start_time = min(self.start_time, dt)
            self.end_time = max(self.end_time, dt)

            self.north, self.south, self.east, self.west = [self.loc[stn_id][0] + 0.01,
                                                            self.loc[stn_id][0] - 0.01,
                                                            self.loc[stn_id][1] + 0.01,
                                                            self.loc[stn_id][1] - 0.01]

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
    print('Extracting Namdrop_raw Metadata')
    path_to_file = r'C:\Users\ecampos\Documents\granule-metadata-extractor\test\fixtures' \
                   r'\NAMMA_DROP_20060807_193132_P.dat'
    exnet = Extract2dimpactsMetadata(path_to_file)
    metada = exnet.get_metadata("test")
