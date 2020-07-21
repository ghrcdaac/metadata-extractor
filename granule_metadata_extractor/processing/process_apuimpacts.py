from ..src.extract_ascii_metadata import ExtractASCIIMetadata
from datetime import datetime, timedelta
import os
import re


class ExtractApuimpactsMetadata(ExtractASCIIMetadata):
    """
    A class to extract apuimpacts spatial and temporal metadata
    """
    start_time = datetime(2100, 1, 1)
    end_time = datetime(1900, 1, 1)
    north = -90.0
    south = 90.0
    east = -180.0
    west = 180.0
    loc = {'apu04':[37.9346, -75.4710], \
       'apu07':[37.9345, -75.4708],\
       'apu01':[37.9290, -75.4737],\
       'apu11':[37.9442, -75.4638],\
       'apu15': [37.9372, -75.4661],\
       'apu17': [37.9444, -75.4812],\
       'apu18': [37.9376, -75.4561],\
       'apu23': [38.0140, -75.4549],\
       'apu25': [38.0564, -75.4097],\
       'apu05': [38.0977, -75.4316],\
       'apu20': [38.1964, -75.3688],\
       'apu21': [38.1003, -75.5524],\
       'apu08': [38.0675, -75.5794]}
    g_sttime = datetime(2020, 1, 15, 17, 27)
    g_endtime = datetime(2020, 2, 29, 20, 56)
    g_nlat = 38.2064
    g_slat = 37.9190
    g_elon = -75.3588
    g_wlon = -75.5894

    def __init__(self, file_path):
        super().__init__(file_path)

        self.file_path = file_path
        self.file_name = os.path.basename(self.file_path)

        with open(file_path, 'r') as f:
            self.file_lines = f.readlines()

        self.get_variables_min_max(self.file_name)

    def get_variables_min_max(self, filename):
        """
        Extracts temporal and spatial metadata from apuimpacts files
        :param filename: file name
        :return: list longitude coordinates
        """

        if '_min' in filename:
            self.read_min_files()
        elif '_data' in filename:
            self.read_data_files()
        elif '_rainevent' in filename:
            self.read_rainevent_files()
        else:
            self.start_time, self.end_time = [datetime(2020, 1, 15, 17, 27), 
                                              datetime(2020, 2, 29, 20, 56)]
            self.north, self.south, self.east, self.west = [38.2064,37.9190,-75.3588,-75.5894]


    def read_min_files(self):
        """
        Extracts temporal and spatial metadata from diameter020 files
        """
        for line in self.file_lines:
            tkn = line.split()
            dt = datetime(int(tkn[0]), 1, 1, int(tkn[2]), int(tkn[3])) + \
                 timedelta(days=int(tkn[1])-1)
            self.start_time = min(self.start_time, dt)
            self.end_time = max(self.end_time, dt)
        site = self.file_name.split('/')[-1].split('_')[1]
        self.north, self.south, self.east, self.west = [self.loc[site][0] + 0.01,
                                                        self.loc[site][0] - 0.01,
                                                        self.loc[site][1] + 0.01,
                                                        self.loc[site][1] - 0.01]
    def read_data_files(self):
        """
        Extracts temporal and spatial metadata from diameter020 files
        """
        for line in self.file_lines:
            tkn = line.split()
            dt = datetime(int(tkn[0]), 1, 1, int(tkn[2]), int(tkn[3]), int(tkn[4])) + \
                 timedelta(days=int(tkn[1])-1)
            self.start_time = min(self.start_time, dt)
            self.end_time = max(self.end_time, dt)
        site = self.file_name.split('/')[-1].split('_')[1]
        self.north, self.south, self.east, self.west = [self.loc[site][0] + 0.01,
                                                        self.loc[site][0] - 0.01,
                                                        self.loc[site][1] + 0.01,
                                                        self.loc[site][1] - 0.01]

    def read_rainevent_files(self):
        """
        Extracts temporal and spatial metadata from diameter020 files
        """
        for line in self.file_lines:
            tkn = line.split()
            st_dtstr = f"{tkn[0].decode('utf-8')}0101T{tkn[2].decode('utf-8')}"
            end_dtstr = f"{tkn[0].decode('utf-8')}0101T{tkn[4].decode('utf-8')}"
            st_dt = datetime.strptime(st_dtstr, '%Y%m%dT%H:%M') + \
                 timedelta(days=int(tkn[1])-1)
            end_dt = datetime.strptime(end_dtstr, '%Y%m%dT%H:%M') + \
                 timedelta(days=int(tkn[3])-1)
            self.start_time = min(self.start_time, st_dt)
            self.end_time = max(self.end_time, end_dt)
        site = self.file_name.split('/')[-1].split('_')[1]
        self.north, self.south, self.east, self.west = [self.loc[site][0] + 0.01,
                                                        self.loc[site][0] - 0.01,
                                                        self.loc[site][1] + 0.01,
                                                        self.loc[site][1] - 0.01]

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
    print('Extracting apuimpacts Metadata')
    path_to_file = r'../../test/fixtures' \
                   r'\impacts_apu01_rainparameter_min.txt'
    exnet = ExtractapuimpactsMetadata(path_to_file)
    metada = exnet.get_metadata("test")
